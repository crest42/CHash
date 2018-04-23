#include "chash.h"
#include "../chord/chord.h"
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>

static int
send_chunk(unsigned char* buf,
           size_t size,
           struct node* target,
           unsigned char* hash,
           size_t hash_size)
{
  assert((size + hash_size + sizeof(size_t)) < MAX_MSG_SIZE);
  unsigned char msg[MAX_MSG_SIZE];
  memset(msg, 0, sizeof(msg)),
    DEBUG(INFO, "Send %d bytes (%p) to node %d\n", size, buf, target->id);
  marshall_msg(
    MSG_TYPE_PUT, target->id, sizeof(size_t), (unsigned char*)&size, msg);
  int offset = CHORD_HEADER_SIZE + sizeof(size_t);
  memcpy(msg + offset, hash, HASH_DIGEST_SIZE);
  offset += HASH_DIGEST_SIZE;
  memcpy(msg + offset, buf, size);
  chord_msg_t type = chord_send_block_and_wait(
    target, msg, offset + size, MSG_TYPE_PUT_ACK, NULL, 0);
  DEBUG(INFO, "Got message with type %d as response\n", (type));
  return CHORD_OK;
}

int
handle_get(unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size)
{
  DEBUG(INFO, "HANDLE GET CALLED\n");
  struct key* search_key = NULL;
  size_t resp_size = 0;
  unsigned char* content = NULL;
  nodeid_t* id = (nodeid_t*)data;
  struct key** first_key = get_first_key();
  for (search_key = *first_key; search_key != NULL;
       search_key = search_key->next) {
    if (search_key->id == *id) {
      break;
    }
  }
  if (search_key) {
    content = search_key->data;
    resp_size = search_key->size;
  }

  unsigned char msg[resp_size + CHORD_HEADER_SIZE];
  marshall_msg(MSG_TYPE_GET_RESP, src, resp_size, content, msg);
  int ret = chord_send_nonblock_sock(
    sock, msg, CHORD_HEADER_SIZE + resp_size, src_addr, src_addr_size);
  return ret;
}

int
handle_put(unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size)
{
  DEBUG(INFO, "HANDLE PUT CALLED. Send answer\n");
  struct key** first_key = get_first_key();
  struct key* new = NULL;
  if (*first_key == NULL) {
    DEBUG(INFO, "first key in null create new\n");
    *first_key = malloc(sizeof(struct key));
    new = *first_key;
  } else {
    DEBUG(INFO, "first key is not null search for last\n");

    for (new = *first_key; new->next != NULL; new = new->next)
      ;
    new->next = malloc(sizeof(struct key));
    new = new->next;
  }

  memcpy(&(new->size), data, sizeof(size_t));
  memcpy(new->hash, data + sizeof(size_t), HASH_DIGEST_SIZE);
  new->data = malloc(new->size);
  memcpy(new->data, data + sizeof(size_t) + HASH_DIGEST_SIZE, new->size);
  new->id = get_mod_of_hash(new->hash, CHORD_RING_SIZE);
  new->owner = src;
  new->next = NULL;
  DEBUG(INFO,
        "Got New key with size %d id: %d owner: %d next: %p\n",
        new->size,
        new->id,
        new->owner,
        new->next);

  unsigned char msg[CHORD_HEADER_SIZE + sizeof(nodeid_t)];
  struct node* mynode = get_own_node();
  marshall_msg(MSG_TYPE_PUT_ACK,
               src,
               sizeof(nodeid_t),
               (unsigned char*)&(mynode->id),
               msg);
  int ret = chord_send_nonblock_sock(
    sock, msg, CHORD_HEADER_SIZE + sizeof(nodeid_t), src_addr, src_addr_size);
  return ret;
}

int
init_chash(void)
{
  struct chord_callbacks* cc = get_callbacks();
  cc->put_handler = handle_put;
  cc->get_handler = handle_get;
  return CHORD_OK;
}

int
put(unsigned char* data, size_t size, nodeid_t* id)
{
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, data, size, HASH_DIGEST_SIZE);
  *id = get_mod_of_hash(out, CHORD_RING_SIZE);
  struct node target;
  target.id = 0;
  struct node* mynode = get_own_node();
  if (!mynode) {
    DEBUG(ERROR, "mynode undefined\n");
    return CHASH_ERR;
  }
  find_successor(mynode, &target, *id);
  DEBUG(INFO, "Put %d bytes data into %d->%d\n", size, *id, target.id);
  for (size_t i = 0; i < size; i = i + CHASH_CHUNK_SIZE) {
    size_t send_size = 0;
    if (size - i < CHASH_CHUNK_SIZE) {
      send_size = size - i;
    } else {
      send_size = CHASH_CHUNK_SIZE;
    }
    if (send_chunk(data + i, send_size, &target, out, HASH_DIGEST_SIZE) ==
        CHORD_ERR) {
      DEBUG(ERROR,
            "Error in send chunk of size %d to target node %d\n",
            send_size,
            target.id);
    }
  }
  return CHASH_OK;
}

int
get(nodeid_t id, unsigned char* buf)
{
  if (!buf) {
    return CHASH_ERR;
  }
  DEBUG(INFO, "Get data for id %d\n", id);

  unsigned char msg[CHORD_HEADER_SIZE + sizeof(nodeid_t)];
  struct node target;
  struct node* mynode = get_own_node();
  find_successor(mynode, &target, id);

  marshall_msg(
    MSG_TYPE_GET, target.id, sizeof(nodeid_t), (unsigned char*)&id, msg);
  chord_msg_t type = chord_send_block_and_wait(
    &target, msg, sizeof(msg), MSG_TYPE_GET_RESP, buf, MAX_MSG_SIZE);
  if (type != MSG_TYPE_GET_RESP) {
    return CHORD_ERR;
  }
  return CHASH_OK;
}