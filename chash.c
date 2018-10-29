#include "chash.h"
#include "../chord/include/chord.h"
#include "../chord/include/network.h"
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include<stdio.h>

struct chash_storage_backend backend;

static int chash_default_put(uint32_t offset,uint32_t size, nodeid_t id, unsigned char *hash, unsigned char *data) {
    (void)offset;
    (void)size;
    (void)id;
    (void)hash;
    (void)data;
    return CHASH_OK;
}

static int chash_default_get(unsigned char *hash, nodeid_t *id,uint32_t *size, unsigned char **data) {
    (void)id;
    (void)hash;
    (void)data;
    (void)size;
    return CHASH_OK;
}

int
init_chash(struct chash_storage_backend *b)
{
  if(!b) {
    backend.put = chash_default_put;
    backend.get = chash_default_get;
    backend.data = NULL;
  } else {
    backend = *b;
  }
  struct chord_callbacks* cc = get_callbacks();
  cc->put_handler = handle_put;
  cc->get_handler = handle_get;
  return CHORD_OK;
}

static int
send_chunk(unsigned char* buf,
           uint32_t offset,
           uint32_t size,
           struct node* target,
           unsigned char* hash,
           size_t hash_size)
{
  assert((size + hash_size + 2*sizeof(uint32_t)) < MAX_MSG_SIZE);
  unsigned char msg[MAX_MSG_SIZE];
  memset(msg,0,sizeof(msg));
  DEBUG(INFO, "Send %d bytes (%p) to node %d\n", size, buf, target->id);
  //printf( "Send %d bytes (%p) with offset %d to node %d\n", size, buf,offset, target->id);

  size_t s = sizeof(uint32_t);
  marshall_msg(
    MSG_TYPE_PUT, target->id, s, (unsigned char*)&offset, msg);
  s+=CHORD_HEADER_SIZE;
  add_msg_cont((unsigned char *)&size,msg,sizeof(uint32_t),s);
  s += sizeof(uint32_t);
  add_msg_cont(hash,msg,HASH_DIGEST_SIZE,s);
  s += HASH_DIGEST_SIZE;
  add_msg_cont(buf,msg,size,s);
  s += size;

  chord_msg_t type = chord_send_block_and_wait(
    target, msg, s, MSG_TYPE_PUT_ACK, NULL, 0);
  DEBUG(INFO, "Got message with type %d as response\n", (type));
  if (type == MSG_TYPE_CHORD_ERR) {
    return CHORD_ERR;
  }
  return CHORD_OK;
}

int
handle_get(chord_msg_t type,unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size)
{
  assert(type == MSG_TYPE_GET);
  DEBUG(INFO, "HANDLE GET CALLED\n");
  unsigned char msg[MAX_MSG_SIZE];
  size_t        size;
  unsigned char *content, *hash = data;
  nodeid_t      id;

  backend.get(hash,&id,&size,&content);
  chord_msg_t msg_type = MSG_TYPE_GET_RESP;
  if(size == 0 || content == NULL) {
    assert(size == 0 && content == NULL);
    msg_type = MSG_TYPE_GET_EFAIL;
  }
//printf("ret size %d\n",size);
  marshall_msg(msg_type, src, size, content, msg);
  return chord_send_nonblock_sock(
    sock, msg, CHORD_HEADER_SIZE + size, src_addr, src_addr_size);
}

int
handle_put(chord_msg_t type,
           unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size)
{
  assert(type == MSG_TYPE_PUT);
  DEBUG(INFO, "HANDLE PUT CALLED. Send answer\n");
  //printf( "HANDLE PUT CALLED. Send answer\n");

  uint32_t p = 0;
  uint32_t offset   = (uint32_t)(*(data+p));
  p += sizeof(uint32_t);
  uint32_t size   = (uint32_t)(*(data+p));
  p += sizeof(uint32_t);
  unsigned char *hash = data + p;
  p += HASH_DIGEST_SIZE;
  unsigned char *cont = data + p;
  nodeid_t id = get_mod_of_hash(hash, CHORD_RING_SIZE);
  chord_msg_t msg_type = MSG_TYPE_PUT_ACK;
  if(backend.put(offset,size,id,hash,cont) != CHASH_OK) {
    msg_type = MSG_TYPE_PUT_EFAIL;
  }
  unsigned char msg[CHORD_HEADER_SIZE + sizeof(nodeid_t)];
  marshall_msg(msg_type,
               src,
               sizeof(nodeid_t),
               (unsigned char*)&(get_own_node()->id),
               msg);
  return chord_send_nonblock_sock(
    sock, msg, CHORD_HEADER_SIZE + sizeof(nodeid_t), src_addr, src_addr_size);
}

static int
put_raw(unsigned char* data, unsigned char *hash,uint32_t offset, uint32_t size, nodeid_t id)
{
  struct node target;
  target.id = 0;
  struct node* mynode = get_own_node();
  if (!mynode) {
    DEBUG(ERROR, "mynode undefined\n");
    return CHASH_ERR;
  }
  find_successor(mynode, &target, id);
  DEBUG(INFO, "Put %d bytes data into %d->%d\n", size, id, target.id);
  for (size_t i = 0; i < size; i = i + CHASH_CHUNK_SIZE) {
    size_t send_size = 0;
    if (size - i < CHASH_CHUNK_SIZE) {
      send_size = size - i;
    } else {
      send_size = CHASH_CHUNK_SIZE;
    }
    if(i > 0) {
      offset = 0;
    }
    if (send_chunk(data + i,offset, send_size, &target, hash, HASH_DIGEST_SIZE) ==
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
put(unsigned char* data, size_t size, nodeid_t* id)
{
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, data, size, HASH_DIGEST_SIZE);
  *id = get_mod_of_hash(out, CHORD_RING_SIZE);
  return put_raw(data,out,0,size,*id);
}
#include <stdio.h>
int chash_put_block(unsigned char *data,int block,uint32_t offset,uint32_t size) {
  unsigned char out[HASH_DIGEST_SIZE];
  //printf("hash block\n");
  hash(out, (unsigned char *)&block, sizeof(int), HASH_DIGEST_SIZE);
    //printf("Get mod of hash\n");

  nodeid_t id = get_mod_of_hash(out, CHORD_RING_SIZE);
  //printf("chash block %d to %d\n",block,id);
  return put_raw(data,out,offset,size,id);
}

static int
get_raw(unsigned char *hash, unsigned char* buf,uint32_t size){
  assert(buf);
  assert(hash);
  unsigned char msg[CHORD_HEADER_SIZE + HASH_DIGEST_SIZE];
  nodeid_t    id = get_mod_of_hash(hash,CHORD_RING_BITS);
  struct node target;
  struct node *mynode = get_own_node();
  find_successor(mynode, &target, id);

  marshall_msg(
    MSG_TYPE_GET, target.id, HASH_DIGEST_SIZE, hash, msg);
  chord_msg_t type = chord_send_block_and_wait(
    &target, msg, sizeof(msg), MSG_TYPE_GET_RESP, buf, size);
  if (type != MSG_TYPE_GET_RESP) {
    return CHORD_ERR;
  }
  return CHASH_OK;
}

int
get(unsigned char* buf,uint32_t size)
{
  if (!buf) {
    return CHASH_ERR;
  }
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, (unsigned char *)&buf, sizeof(int), HASH_DIGEST_SIZE);
  DEBUG(INFO, "Get data for id %d\n", id);
  return get_raw(out,buf,size);
}

int chash_get_block(int block,unsigned char *ret) {
  unsigned char out[HASH_DIGEST_SIZE];
  hash(out, (unsigned char *)&block, sizeof(int), HASH_DIGEST_SIZE);
  return get_raw(out,ret,128);
}