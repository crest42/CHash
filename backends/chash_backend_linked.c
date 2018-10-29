#include "chash_backend_linked.h"

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