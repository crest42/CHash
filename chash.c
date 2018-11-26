#include "include/chash.h"
#include "include/chash_backend.h"
#include "../chord/include/chord.h"
#include <assert.h>
#include <errno.h>
#include <sys/time.h>
#include <unistd.h>
#include<stdio.h>


struct chash_backend backend;
struct chash_frontend frontend;
extern struct node* self;

static int
chash_backend_default_put(struct item* item, unsigned char* data)
{
  (void)item;
  (void)data;
  return CHASH_OK;
}

static int chash_backend_default_get(unsigned char *hash, nodeid_t *id,uint32_t *size) {
    (void)id;
    (void)hash;
    (void)size;
    return CHASH_OK;
}

static int chash_frontend_default_put(uint32_t key_size,unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data) {
    (void)key_size;
    (void)key;
    (void)data_size;
    (void)data;
    (void)offset;
    return CHASH_OK;
}

static int chash_frontend_default_get(uint32_t key_size,unsigned char *key, uint32_t buf_size, unsigned char *buf) {
    (void)key_size;
    (void)key;
    (void)buf_size;
    (void)buf;
    return CHASH_OK;
}

static int chash_periodic(void *data) {
  (void)data;
  if(backend.backend_periodic_hook) {
    backend.backend_periodic_hook(data);
  }
  if(frontend.frontend_periodic_hook) {
    frontend.frontend_periodic_hook(data);
  }
  return CHASH_OK;
}

int
init_chash(struct chash_backend *b,struct chash_frontend *f)
{
  if(!b) {
    backend.put = chash_backend_default_put;
    backend.get = chash_backend_default_get;
    backend.periodic_data = NULL;
  } else {
    backend = *b;
  }
  if(!f) {
    frontend.put = chash_frontend_default_put;
    frontend.get = chash_frontend_default_get;
    set_callback(MSG_TYPE_PUT, handle_put);
    set_callback(MSG_TYPE_GET, handle_get);
    frontend.periodic_data = NULL;
  } else {
    frontend = *f;
    set_callback(MSG_TYPE_PUT, frontend.put_handler);
    set_callback(MSG_TYPE_GET, frontend.get_handler);
  }

  struct hooks* h = get_hooks();
  h->periodic_hook       = chash_periodic;
  set_callback(MSG_TYPE_SYNC, frontend.sync_handler);
  set_callback(MSG_TYPE_SYNC_REQ_FETCH, frontend.sync_fetch_handler);
  return CHORD_OK;
}

static int
send_chunk(unsigned char* buf,
           struct node* target,
           struct item *item)
{
  assert((item->size + HASH_DIGEST_SIZE + sizeof(struct item)) < MAX_MSG_SIZE);
  unsigned char msg[MAX_MSG_SIZE];
  memset(msg,0,sizeof(msg));
  DEBUG(INFO, "Send %d bytes (%p) to node %d\n", item->size, buf, target->id);
  //printf( "Send %d bytes (%p) with offset %d to node %d\n", size, buf,offset, target->id);

  size_t s = sizeof(struct item);
  marshal_msg(
    MSG_TYPE_PUT, target->id, s, (unsigned char*)item, msg);
  s += CHORD_HEADER_SIZE;
  add_msg_cont(buf,msg,item->size,s);
  s += item->size;
  chord_msg_t type =
    chord_send_block_and_wait(target, msg, s, MSG_TYPE_PUT_ACK, NULL, 0, NULL);
  if (type == MSG_TYPE_CHORD_ERR) {
    return CHORD_ERR;
  }
  return CHORD_OK;
}

int
handle_get(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size)
{
  (void)type;
  (void)msg_size;
  assert(type == MSG_TYPE_GET);
  assert(msg_size > 0);
  DEBUG(INFO, "HANDLE GET CALLED\n");
  unsigned char msg[MAX_MSG_SIZE];
  unsigned char buf[CHASH_CHUNK_SIZE];
  size_t        size = 0;
  unsigned char *hash = data;
  nodeid_t      id;

  chash_backend_get(hash,&id,(uint32_t*)&size);
  chord_msg_t msg_type = MSG_TYPE_GET_RESP;
  if(size == 0) {
    msg_type = MSG_TYPE_GET_EFAIL;
  }
  if(size > 0) {
    chash_backend_get_data(hash,size,buf);
  }
  //printf("ret size %d\n",size);
  marshal_msg(msg_type, src, size, buf, msg);
  return chord_send_nonblock_sock(msg, CHORD_HEADER_SIZE + size, s);
}

int
handle_put(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size)
{
    (void)type;
  (void)msg_size;
  assert(type == MSG_TYPE_PUT);
  assert(msg_size > 0);
  DEBUG(INFO, "HANDLE PUT CALLED. Send answer\n");
  //printf( "HANDLE PUT CALLED. Send answer\n");
  struct item* item = (struct item*)data;
  unsigned char *cont = data + sizeof(struct item);
  chord_msg_t msg_type = MSG_TYPE_PUT_ACK;
  if(backend.put(item,cont) != CHASH_OK) {
    msg_type = MSG_TYPE_PUT_EFAIL;
  }
  unsigned char msg[CHORD_HEADER_SIZE + sizeof(nodeid_t)];
  marshal_msg(msg_type,
               src,
               sizeof(nodeid_t),
               (unsigned char*)&(self->id),
               msg);
  return chord_send_nonblock_sock(msg, CHORD_HEADER_SIZE + sizeof(nodeid_t), s);
}

int
put_raw(unsigned char* data, struct item *item, struct node *target)
{
  assert(item);
  assert(target);
  assert(target->id > 0);
  DEBUG(INFO, "Put %d bytes data into %d->%d\n", item->size, get_mod_of_hash(item->hash,CHORD_RING_SIZE), target->id);
  //TODO: Remove chunk size. Should be handled in calling function
  uint32_t size = item->size;
  for (size_t i = 0; i < size; i = i + CHASH_CHUNK_SIZE) {
    if ((size - i) < CHASH_CHUNK_SIZE) {
      item->size = size - i;
    } else {
      item->size = CHASH_CHUNK_SIZE;
    }
    if(i > 0) {
      item->offset = 0;
    }
    if (send_chunk(data + i,target,item) ==
        CHORD_ERR) {
      DEBUG(ERROR,
            "Error in send chunk of size %d to target node %d\n",
            item->size,
            target->id);
    }
  }
  return CHASH_OK;
}

int
put(unsigned char* data, size_t size)
{
  struct item item;
  struct node target;
  item.size = size;
  item.offset = 0;
  item.flags = 0;
  item.block = 0;
  hash(item.hash, data, item.size, HASH_DIGEST_SIZE);
  find_successor(
    self, &target, get_mod_of_hash(item.hash, CHORD_RING_SIZE));
  return put_raw(data, &item, &target);
}

int
get_raw(unsigned char *hash, unsigned char* buf,uint32_t size){
  assert(buf);
  assert(hash);
  unsigned char msg[CHORD_HEADER_SIZE + HASH_DIGEST_SIZE];
  nodeid_t id = get_mod_of_hash(hash,CHORD_RING_SIZE);
  struct node target;
  struct node *mynode = self;
  find_successor(mynode, &target, id);
  marshal_msg(
    MSG_TYPE_GET, target.id, HASH_DIGEST_SIZE, hash, msg);
  chord_msg_t type = chord_send_block_and_wait(
    &target, msg, sizeof(msg), MSG_TYPE_GET_RESP, buf, size,NULL);
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
  DEBUG(INFO, "Get data for id %d\n", get_mod_of_hash(out,CHORD_RING_SIZE));
  return get_raw(out,buf,size);
}

int sync_node(unsigned char *buf,uint32_t size,struct node *target) {
  unsigned char msg[MAX_MSG_SIZE];
  marshal_msg(
    MSG_TYPE_SYNC, target->id, size, buf, msg);
  size_t resp_size = 0;
  chord_send_block_and_wait(target,
                            msg,
                            sizeof(msg),
                            MSG_TYPE_SYNC_REQ_FETCH,
                            msg,
                            sizeof(msg),
                            &resp_size);
  for (uint32_t i = 0; i < resp_size / sizeof(struct key_range);i++) {
    struct key_range* r = ((struct key_range*)(msg)) + i;
    for (uint32_t i = r->start; i != (r->end+1);i = (i+1)%CHORD_RING_SIZE) {
      push_key(i,target);
    }
  }
  return CHORD_OK;
}
