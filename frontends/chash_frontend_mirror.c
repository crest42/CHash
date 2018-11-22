#include "../include/chash.h"
#include "../include/chash_frontend.h"
#include "../include/chash_backend.h"
#include <stdio.h>

extern int
remove_key(struct key* key);
extern int
get_key(nodeid_t id,struct key *k);

extern int
add_key(struct key* k, unsigned char* d);

extern struct node* self;
extern struct node* successorlist;
extern struct aggregate stats;
uint32_t stable = 0, old = 0;

int
chash_frontend_put(uint32_t key_size,
                 unsigned char* key,
                 uint32_t offset,
                 uint32_t data_size,
                 unsigned char* data)
{
  struct item item;
  struct node target;
  item.size = data_size;
  item.offset = offset;
  item.flags = 0;
  item.block = *((uint32_t *)key);
  hash(item.hash, key, key_size, HASH_DIGEST_SIZE);
  find_successor(
    self, &target, get_mod_of_hash(item.hash, CHORD_RING_SIZE));
  nodeid_t first = target.id;
  int i = 0;
  do {
    //printf("put item with id %d on node %d\n",get_mod_of_hash(item.hash, CHORD_RING_SIZE),target.id);
    put_raw(data, &item, &target);
    find_successor(self, &target, target.id);
    //printf("next: %d\n",target.id);
    i++;
  } while (first != target.id && i != REPLICAS);

  return CHASH_OK;
}

int chash_frontend_get(uint32_t key_size, unsigned char *key, uint32_t buf_size, unsigned char *buf) {
  //printf("read block %d with buf size %d\n",*((uint32_t *)key),buf_size);
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, key, key_size, HASH_DIGEST_SIZE);
    unsigned char msg[CHORD_HEADER_SIZE + HASH_DIGEST_SIZE];
  nodeid_t id = get_mod_of_hash(out,CHORD_RING_SIZE);
  struct node target;
  struct node *mynode = self;
  chord_msg_t type = -1;
  int i = 0;
  do {
    find_successor(mynode, &target, id);
    marshal_msg(
      MSG_TYPE_GET, target.id, HASH_DIGEST_SIZE, out, msg);
    type = chord_send_block_and_wait(
      &target, msg, sizeof(msg), MSG_TYPE_GET_RESP, buf, buf_size,NULL);
    id = target.id;
    i++;
  } while (type == MSG_TYPE_GET_EFAIL && i < REPLICAS);
  return CHORD_OK;
}


static int maint_global(void) {
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  struct node successor;
  nodeid_t successorlist[SUCCESSORLIST_SIZE];
  for (key = *first_key; key != NULL; key = key->next) {
    find_successor(self, &successor, key->id);
    bool owns = false;
    if (successor.id == self->id) {
      owns = true;
    } else {
      get_successorlist_id(&successor, successorlist);
      for (int i = 0; i < REPLICAS - 1; i++) {
        if (successorlist[i] == self->id) {
          owns = true;
          break;
        }
      }
    }
    if (!owns) {
      DEBUG(INFO, "Do not own key %d reinsert!\n", key->id);
      chash_frontend_put(
        sizeof(uint32_t), (unsigned char*)&key->block, 0, key->size, key->data);
      remove_key(key);
    }
  }
  return CHASH_OK;
}

static void add_interval(struct key_range *r,unsigned char *buf, uint32_t *offset) {
  memcpy(buf+*offset,r,sizeof(struct key_range));
  *offset += sizeof(struct key_range);
}

static int maint_sync(unsigned char *buf, uint32_t size) {
  for(int i = 0;i<REPLICAS-1;i++) {
    if(successorlist[i].id == self->id) {
      break;
    }
    sync_node(buf,size,&successorlist[i]);
  }
  return CHORD_OK;
}

static int maint_local(void) {
  //TODO: Do not assume that all keys fit into a single message. Atm buf is floor(1004/8) = 125 keys
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  struct node *self = self;
  unsigned char buf[MAX_MSG_SIZE-CHORD_HEADER_SIZE];
  memset(buf,0,sizeof(buf));
  struct key_range r = {.start = 0, .end = 0};
  uint32_t offset = 0;
  for (key = *first_key; key != NULL;
       key = key->next) {
    if(in_interval(self->additional->predecessor,self,key->id)) {
      if(r.start == 0) {
        r.start = key->id;
        r.end   = key->id;
      } else {
        if(key->id == (r.start+1)%CHORD_RING_SIZE) {
          r.end = key->id;
        } else {
          add_interval(&r,buf,&offset);
          r.start = key->id;
          r.end = key->id;
        }
      }
    }
  }
  if(r.start != 0 && r.end != 0) {
    add_interval(&r,buf,&offset);
  }
  /*for(uint32_t i = 0;i<offset;i+=(sizeof(nodeid_t)*2)) {
    printf("range %d is from %d to %d\n",i,*((nodeid_t *)(&buf[i])),*((nodeid_t *)(&buf[i+sizeof(nodeid_t)])));
  }*/
  if(offset > 0) {
    maint_sync(buf,offset);
  }
  return CHASH_OK;
}

int handle_sync_fetch(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size){
  assert(type == MSG_TYPE_SYNC_REQ_FETCH);
  assert(msg_size > 0);
  (void)type;
  (void)src;
  (void)s;
  (void)msg_size;
  struct key *k = (struct key*)data, get;
  if(get_key(k->id,&get) != CHORD_ERR) {
    add_key(k, data + sizeof(struct key));
  }
  return CHASH_OK;
}

int handle_sync(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size) {
  assert(type == MSG_TYPE_SYNC);
  (void)type;
  unsigned char buf[MAX_MSG_SIZE];
  uint32_t offset = 0;
  struct key_range req = {.start = 0, .end = 0};
  for (uint32_t i = 0; i < msg_size / sizeof(struct key_range); i++) {
    struct key_range *r  = (struct key_range *)(data + (i * sizeof(struct key_range)));
    assert(r->end - r->start <= CHORD_RING_SIZE);
    for(;r->start <= r->end;r->start = ((r->start+1)%CHORD_RING_SIZE)) {
      struct key k;
      if(get_key(r->start,&k) == CHORD_ERR) {
        if(req.start == 0) {
          req.start = r->start;
          req.end   = r->end;
        } else {
          if(r->start == (req.start+1)) {
            req.end = r->start;
          } else {
          add_interval(&req,buf,&offset);
          req.start = r->start;
          req.end   = r->end;
          }
        }
      }
    }
  }
  if(req.start != 0 && req.end != 0) {
    add_interval(&req, buf, &offset);
  }
  DEBUG(INFO, "Req %d keys to sync\n", offset / (2 * sizeof(uint32_t)));
  marshal_msg(MSG_TYPE_SYNC_REQ_FETCH, src, offset, buf, buf);
  return chord_send_nonblock_sock(buf, CHORD_HEADER_SIZE + offset, s);
}

int chash_frontend_periodic(void *data) {
  if(data) {
    if(stable >= 3) {
      //printf("s: %d sec: %d\n",stats->available,stats->available/128);
      *((uint32_t *)data) = (int)(stats.available / 128);
    } else {
      uint32_t tmp = *((uint32_t*)data);
      if(tmp == old) {
        stable++;
      } else {
        old = tmp;
      }
    }
    if(stable >= UINT_MAX) {
      stable = 3;
    }
  } else {
    printf("no data\n");
  }
  //TODO: Error handling
  DEBUG(INFO, "Start maint_global\n");
  maint_global();
  DEBUG(INFO, "Start maint_local\n");
  maint_local();
  return CHASH_OK;
}


int push_key(uint32_t id, struct node *target) {
  struct key k;
  get_key(id,&k);
  unsigned char msg[MAX_MSG_SIZE];
  marshal_msg(
    MSG_TYPE_SYNC_REQ_FETCH, target->id, sizeof(struct key), (unsigned char*)&k,msg);
  add_msg_cont(k.data, msg, k.size, CHORD_HEADER_SIZE + sizeof(struct key));
  chord_send_block_and_wait(
    target, msg, CHORD_HEADER_SIZE + sizeof(struct key) + k.size, MSG_TYPE_NO_WAIT, NULL, 0, NULL);
  return CHASH_OK;
}
