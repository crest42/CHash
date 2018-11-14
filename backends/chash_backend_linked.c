#include "../chash.h"
#include "../../chord/include/network.h"
#include "chash_backend_linked.h"
extern struct chash_frontend frontend;

struct key* first_key;
struct key* last_key;
struct key**
get_first_key(void)
{
  return &first_key;
}

struct key**
get_last_key(void)
{
  return &last_key;
}


static int remove_key(struct key *key) {
  struct key *search_key, *prev_key = NULL;
  struct key **first_key = get_first_key(), **last_key = get_last_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (memcmp(key->hash,search_key->hash,HASH_DIGEST_SIZE) == 0) {
      if(!prev_key) {
        *first_key = search_key->next;
        free(search_key);
        break;
      } else {
        if(*last_key == search_key) {
          *last_key = prev_key;
        }
        prev_key->next = search_key->next;
        free(search_key);
        break;
      }
    }
    prev_key = search_key;
  }
  return CHASH_OK;
}

void dump(unsigned char *data,uint32_t size) {
    for(uint32_t i = 0;i<size;i++) {
        printf("%02x",data[i]);
    }
    printf("\n");
}

static struct key *get_key(nodeid_t id) {
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  for (key = *first_key; key != NULL;
       key = key->next) {
         if(key->id == id) {
           return key;
         }
  }
  return NULL;
}

static struct key *add_key(struct key *k, unsigned char *d) {
  struct key **first_key = get_first_key(), **last_key = get_last_key();
  if (*first_key == NULL) {
    *first_key = malloc(sizeof(struct key));
    *last_key = *first_key;
  } else {
    assert(!(*last_key)->next);
    ((*last_key)->next) = malloc(sizeof(struct key));

    struct key *tmp = (*last_key)->next;
    *last_key = tmp;
  }
  struct key* new_key = *last_key;
  memcpy(new_key, k, sizeof(struct key));
  new_key->next = NULL;
  new_key->data = malloc(new_key->size);
  memcpy(new_key->data, d, new_key->size);
  return new_key;
}

int chash_linked_list_put(struct item *item, unsigned char *data) {
  nodeid_t id = get_mod_of_hash(item->hash, CHORD_RING_SIZE);
  struct key *k = get_key(id);
  if (k) {
    assert((item->offset + item->size) <= k->size);
    memcpy(k->data + item->offset, data, item->size);
  } else {
    struct key k;
    memcpy(k.hash, item->hash, HASH_DIGEST_SIZE);
    k.id = id;
    k.next = NULL;
    k.size = item->size;
    k.block = item->block;
    k.data = NULL;
    add_key(&k, data);
  }
  return CHASH_OK;
}

void print_hash(unsigned char *hash, size_t size) {
    for(size_t i = 0;i<size;i++) {
        printf("%x",hash[i]);
    }
}

int chash_linked_list_get(unsigned char *hash, nodeid_t *id, uint32_t *size, unsigned char **data) {
  struct key* search_key = NULL;
  struct key** first_key = get_first_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (memcmp(hash,search_key->hash,HASH_DIGEST_SIZE) == 0) {
      break;
    }
  }
  if (search_key) {
    *data = search_key->data;
    *size = search_key->size;
    *id   = search_key->id;
  } else {
    *data = NULL;
    *size = 0;
    *id = 0;
  }
  return CHASH_OK;
}

/*
static nodeid_t get_last_node(nodeid_t *id, int size) {
  assert(id);
  nodeid_t first = id[0];
  nodeid_t ret = id[0];
  for(int i = 1;i<size;i++) {
    if(first == id[i]) {
      return ret;
    } else {
      ret = id[i];
    }
  }
  return id[size];
}*/

static int maint_global(void) {
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  struct node successor;
  nodeid_t successorlist[SUCCESSORLIST_SIZE];
  for (key = *first_key; key != NULL; key = key->next) {
    find_successor(get_own_node(), &successor, key->id);
    bool owns = false;
    if (successor.id == get_own_node()->id) {
      owns = true;
    } else {
      get_successorlist_id(&successor, successorlist);
      for (int i = 0; i < REPLICAS - 1; i++) {
        if (successorlist[i] == get_own_node()->id) {
          owns = true;
          break;
        }
      }
    }
    if (!owns) {
      DEBUG(INFO, "Do not own key %d reinsert!\n", key->id);
      frontend.put(
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
  struct node *successorlist = get_successorlist();
  for(int i = 0;i<REPLICAS-1;i++) {
    if(successorlist[i].id == get_own_node()->id) {
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
  struct node *self = get_own_node();
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

int chash_linked_list_maint(void *data) {
  if(data) {
    struct aggregate *stats = get_stats();
    printf("s: %d sec: %d\n",stats->available,stats->available/128);
    *((uint32_t *)data) = (int)(stats->available / 128);
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
  struct key* k = (struct key*)data;
  if(!get_key(k->id)) {
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
  unsigned char buf[MAX_MSG_SIZE];
  uint32_t offset = 0;
  struct key_range req = {.start = 0, .end = 0};
  for (uint32_t i = 0; i < msg_size / sizeof(struct key_range); i++) {
    struct key_range *r  = (struct key_range *)(data + (i * sizeof(struct key_range)));
    assert(r->end - r->start <= CHORD_RING_SIZE);
    for(;r->start <= r->end;r->start = ((r->start+1)%CHORD_RING_SIZE)) {
      if(!get_key(r->start)) {
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

int push_key(uint32_t id, struct node *target) {
  struct key* k = get_key(id);
  unsigned char msg[MAX_MSG_SIZE];
  marshal_msg(
    MSG_TYPE_SYNC_REQ_FETCH, target->id, sizeof(struct key), (unsigned char*)k,msg);
  add_msg_cont(k->data, msg, k->size, CHORD_HEADER_SIZE + sizeof(struct key));
  chord_send_block_and_wait(
    target, msg, CHORD_HEADER_SIZE + sizeof(struct key) + k->size, MSG_TYPE_NO_WAIT, NULL, 0, NULL);
  return CHASH_OK;
}

//(uint32_t key_size, unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data)