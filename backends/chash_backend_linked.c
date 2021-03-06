#include "../include/chash.h"
#include "../include/chash_backend.h"
#include "../../chord/include/network.h"
#ifdef CHASH_BACKEND_LINKED
extern struct chash_frontend frontend;

uint32_t key_count;
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
  key_count--;
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

int get_key(unsigned char *hash, uint32_t id, struct key *k) {
  struct key* key = NULL;
  struct key** first_key = get_first_key();
  for (key = *first_key; key != NULL;
       key = key->next) {
         if((hash == NULL && key->id == id) || (hash && memcmp(hash,key->hash,HASH_DIGEST_SIZE) == 0)) {
           *k = *key;
           return CHORD_OK;
         }
  }
  return CHORD_ERR;
}

int add_key(struct key *k, unsigned char *d) {
  key_count++;
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
  struct key k;
  if (get_key(item->hash,0, &k) == CHORD_OK) {
    assert((item->offset + item->size) <= k.size);
    memcpy(k.data + item->offset, data, item->size);
  } else {
    struct key k;
    memcpy(k.hash, item->hash, HASH_DIGEST_SIZE);
    k.id = get_mod_of_hash(item->hash, CHORD_RING_SIZE);
    k.next = NULL;
    k.size = item->size;
    k.block = item->block;
    k.data = NULL;
    add_key(&k, data);
  }
  return CHASH_OK;
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
