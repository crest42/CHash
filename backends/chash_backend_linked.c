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


int remove_key(struct key *key) {
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

struct key *get_key(nodeid_t id) {
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

struct key *add_key(struct key *k, unsigned char *d) {
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