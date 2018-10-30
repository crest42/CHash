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

int chash_linked_list_maint(void *data) {
  (void)data;
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
  return CHORD_OK;
}

//(uint32_t key_size, unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data)