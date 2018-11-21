#include "../chash.h"
#include "../../chord/include/network.h"
#include "chash_backend.h"
#ifdef CHASH_BACKEND_LINKED
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

int chash_backend_init(void *data) {
  (void)data;
  return CHASH_OK;
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

int get_key(nodeid_t id, struct key *k) {
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  for (key = *first_key; key != NULL;
       key = key->next) {
         if(key->id == id) {
           *k = *key;
           return CHORD_OK;
         }
  }
  return CHORD_ERR;
}

int add_key(struct key *k, unsigned char *d) {
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
  return CHORD_OK;
}

int chash_backend_put(struct item *item, unsigned char *data) {
  nodeid_t id = get_mod_of_hash(item->hash, CHORD_RING_SIZE);
  struct key k;
  if (get_key(id, &k) == CHORD_OK) {
    assert((item->offset + item->size) <= k.size);
    memcpy(k.data + item->offset, data, item->size);
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

int chash_backend_get_data(unsigned char *hash, size_t size, unsigned char *buf) {
  struct key* search_key = NULL;
  struct key** first_key = get_first_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (memcmp(hash,search_key->hash,HASH_DIGEST_SIZE) == 0) {
      break;
    }
  }
  if(search_key) {
    memcpy(buf,search_key->data,size);
  }
  return CHASH_OK;
}

int chash_backend_get(unsigned char *hash, nodeid_t *id, uint32_t *size) {
  struct key* search_key = NULL;
  struct key** first_key = get_first_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (memcmp(hash,search_key->hash,HASH_DIGEST_SIZE) == 0) {
      break;
    }
  }
  if (search_key) {
    *size = search_key->size;
    *id   = search_key->id;
  } else {
    *size = 0;
    *id = 0;
  }
  return CHASH_OK;
}

#endif