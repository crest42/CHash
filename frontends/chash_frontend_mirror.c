#include "../chash.h"
#include "chash_frontend_mirror.h"
#include <stdio.h>

int chash_mirror_put(uint32_t key_size, unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data) {
  struct item item;
  struct node target;
  item.size = data_size;
  item.offset = offset;
  item.flags = 0;
  item.block = *((uint32_t *)key);
  hash(item.hash, key, key_size, HASH_DIGEST_SIZE);
  find_successor(
    get_own_node(), &target, get_mod_of_hash(item.hash, CHORD_RING_SIZE));
  nodeid_t first = target.id;
  int i = 0;
  do {
    //printf("put item with id %d on node %d\n",get_mod_of_hash(item.hash, CHORD_RING_SIZE),target.id);
    put_raw(data, &item, &target);
    find_successor(get_own_node(), &target, target.id);
    //printf("next: %d\n",target.id);
    i++;
  } while (first != target.id && i != REPLICAS);

  return CHASH_OK;
}

int chash_mirror_get(uint32_t key_size, unsigned char *key, uint32_t buf_size, unsigned char *buf) {
  //printf("read block %d with buf size %d\n",*((uint32_t *)key),buf_size);
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, key, key_size, HASH_DIGEST_SIZE);
  return get_raw(out, buf, buf_size);
}