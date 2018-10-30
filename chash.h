#ifndef _LIBCHASH_H
#define _LIBCHASH_H
#include "../chord/include/chord.h"
#include <stdint.h>
#define CHASH_OK (0)
#define CHASH_ERR (-1)
#define CHASH_CHUNK_SIZE (128)

struct item {
  unsigned char hash[HASH_DIGEST_SIZE];
  uint32_t block;
  uint32_t flags;
  uint32_t offset;
  uint32_t size;
};


typedef int (*chash_backend_put)(struct item *,
                                 unsigned char*);

typedef int (*chash_backend_get)(unsigned char*,
                                 nodeid_t *,
                                 uint32_t *,
                                 unsigned char**);

typedef int (*chash_frontend_put)(uint32_t,
                                 unsigned char *,
                                 uint32_t,
                                 uint32_t,
                                 unsigned char*);

typedef int (*chash_frontend_get)(uint32_t,
                                 unsigned char*,
                                 uint32_t ,
                                 unsigned char *);

struct chash_backend {
    chash_backend_put put;
    chash_backend_get get;
    chord_periodic_hook backend_periodic_hook;
    void *data;
};

struct chash_frontend {
    chash_frontend_put put;
    chash_frontend_get get;
    chord_callback put_handler;
    chord_callback get_handler;
    chord_periodic_hook frontend_periodic_hook;
    void* data;
};


int
put(unsigned char* data, size_t size);

int
get(unsigned char* buf,uint32_t size);

int
handle_get(chord_msg_t type,unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size);
int
handle_put(chord_msg_t type,
           unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size);
int
init_chash(struct chash_backend *b,struct chash_frontend *f);

int
put_raw(unsigned char* data,
        struct item *item,
        struct node *target);
int
get_raw(unsigned char* hash, unsigned char* buf, uint32_t size);

#endif
