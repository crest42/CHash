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
  struct key *search_key, *last_key = NULL, *tmp;
  struct key **first_key = get_first_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (memcmp(key->hash,search_key->hash,HASH_DIGEST_SIZE) == 0) {
      if(!last_key) {
        tmp = *first_key;
        *first_key = search_key->next;
        free(tmp);
        break;
      } else {
        tmp = search_key;
        last_key->next = search_key->next;
        free(tmp);
        break;
      }
    }
    last_key = search_key;
  }
  return CHASH_OK;
}
void dump(unsigned char *data,uint32_t size) {
    for(uint32_t i = 0;i<size;i++) {
        printf("%02x",data[i]);
    }
    printf("\n");
}

int chash_linked_list_put(struct item *item, unsigned char *data) {
  bool found = false;
  struct key** first_key = get_first_key();
  struct key *new = NULL, *last = *first_key;
  if (*first_key == NULL) {
    //printf( "first key in null create new\n");
    *first_key = malloc(sizeof(struct key));
    new = *first_key;
  } else {
    DEBUG(INFO, "first key is not null search for last\n");
    for (new = *first_key; new != NULL; new = new->next) {
      if(memcmp(new->hash,item->hash,HASH_DIGEST_SIZE) == 0) {
        assert((item->offset+item->size) <= new->size);
        //printf("overwrite block with size %d and offset %d\n",size,offset);
        //dump(data,size);
        memcpy(new->data+item->offset,data,item->size);
        found = true;
        break;
      }
      last = new;
    }
    if(!found) {
      new = last;
      new->next = malloc(sizeof(struct key));
      new = new->next;
    }
  }

  if(!found) {
    //printf("insert new block with size %d and offset %d\n",size,offset);
    new->size = 128;
    memcpy(new->hash, item->hash, HASH_DIGEST_SIZE);
    new->data = malloc(new->size);
    memcpy(new->data+item->offset, data, item->size);
    new->id = get_mod_of_hash(new->hash, CHORD_RING_SIZE);
    new->block = item->block;
    new->next = NULL;
    DEBUG(INFO,
          "Got New key with size %d id: %d owner: %d next: %p\n",
          new->size,
          new->id,
          new->owner,
          new->next);
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
  for (key = *first_key; key != NULL;
       key = key->next) {
      find_successor(get_own_node(),&successor,key->id);
      bool owns = false;
      if(successor.id == get_own_node()->id) {
        owns = true;
      } else {
        get_successorlist_id(&successor,successorlist);
        for(int i = 0;i<REPLICAS-1;i++) {
          if(successorlist[i] == get_own_node()->id) {
            owns = true;
            break;
          }
        }
      }
      if(!owns) {
        frontend.put(sizeof(uint32_t),(unsigned char *)&key->block,0,key->size,key->data);
        remove_key(key);
      }
  }
  return CHASH_OK;
}

static void add_interval(nodeid_t start,nodeid_t end,unsigned char *buf, uint32_t *offset) {
  memcpy(buf+*offset,&start,sizeof(start));
  *offset += sizeof(nodeid_t);
  memcpy(buf+*offset,&end,sizeof(end));
  *offset += sizeof(nodeid_t);
}

static int maint_sync(unsigned char *buf, uint32_t size) {
  struct node *successorlist = get_successorlist();
  for(int i = 0;i<SUCCESSORLIST_SIZE;i++) {
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
  nodeid_t start = 0, end = 0;
  uint32_t offset = 0;
  for (key = *first_key; key != NULL;
       key = key->next) {
    if(in_interval(self->predecessor,self,key->id)) {
      if(start == 0) {
        start = key->id;
        end   = key->id;
      } else {
        if(key->id == start+1) {
          end = key->id;
        } else {
          add_interval(start,end,buf,&offset);
          start = key->id;
          end = key->id;
        }
      }
    }
  }
  if(start != 0 && end != 0) {
    add_interval(start,end,buf,&offset);
  }
  for(uint32_t i = 0;i<offset;i+=(sizeof(nodeid_t)*2)) {
    //printf("range %d is from %d to %d\n",i,*((nodeid_t *)(&buf[i])),*((nodeid_t *)(&buf[i+sizeof(nodeid_t)])));
  }
  if(offset > 0) {
    maint_sync(buf,offset);
  }
  return CHASH_OK;
}

int chash_linked_list_maint(void *data) {
  (void)data;
  int ret = maint_global();
  maint_local();
  return ret;
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

int handle_sync(chord_msg_t type,
                unsigned char *data,
                nodeid_t src,
                int sock,
                struct sockaddr* src_addr,
                size_t src_addr_size) {
  assert(type == MSG_TYPE_SYNC);
  uint32_t size = *((uint32_t *)data);
  uint32_t offset = sizeof(uint32_t);
  for(uint32_t i = 0;i<size;i = i + (2*sizeof(uint32_t))) {
    //printf("got %d -> %d sync request offset %d\n",*((uint32_t*)(data+offset)),*((uint32_t*)(data+offset+sizeof(uint32_t))),offset);
    uint32_t start = *((uint32_t*)(data+offset));
    offset += sizeof(uint32_t);
    uint32_t end = *((uint32_t*)(data+offset));
    offset += sizeof(uint32_t);
    assert(end-start <= CHORD_RING_SIZE);
    for(;start <= end;start++) {
      if(!get_key(start)) {
        //printf("need to fetch %d\n",start);
      } else {
        //printf("already got %d\n",start);
      }
    }
  }
  unsigned char msg[MAX_MSG_SIZE];
  marshall_msg(MSG_TYPE_SYNC_REQ_RESP,
               src,
               0,
               NULL,
               msg);
  return chord_send_nonblock_sock(
    sock, msg, CHORD_HEADER_SIZE + size, src_addr, src_addr_size);
  return CHASH_OK;
}

//(uint32_t key_size, unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data)