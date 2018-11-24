#ifndef _LIBCHASH_H
#define _LIBCHASH_H
#include "../../chord/include/chord.h"
#include "../../chord/include/chord_util.h"
#include "../../chord/include/network.h"

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

struct key
{
  nodeid_t id; /*!< Id of the key. The node id is the hashed ipv6 address of
                  the node modulo the ring size */
  uint32_t block;
  uint32_t size;
  unsigned char hash[20];
  unsigned char *data;
  struct key* next;
};


typedef int (*chash_backend_put_f)(struct item *,
                                 unsigned char*);

typedef int (*chash_backend_get_f)(unsigned char*,
                                 nodeid_t *,
                                 uint32_t *);

typedef int (*chash_frontend_put_f)(uint32_t,
                                 unsigned char *,
                                 uint32_t,
                                 uint32_t,
                                 unsigned char*);

typedef int (*chash_frontend_get_f)(uint32_t,
                                 unsigned char*,
                                 uint32_t ,
                                 unsigned char *);

struct chash_backend {
    chash_backend_put_f put;
    chash_backend_get_f get;
    chord_periodic_hook backend_periodic_hook;
    void* periodic_data;
};

struct chash_frontend {
    chash_frontend_put_f put;
    chash_frontend_get_f get;
    chord_callback put_handler;
    chord_callback get_handler;
    chord_callback sync_handler;
    chord_callback sync_fetch_handler;
    chord_periodic_hook frontend_periodic_hook;
    void* periodic_data;
};

struct key_range {
  nodeid_t start;
  nodeid_t end;
};

int
push_key(nodeid_t id, struct node *target);

int
put(unsigned char* data, size_t size);

int
get(unsigned char* buf,uint32_t size);

int
handle_get(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size);
int
handle_put(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size);
int
init_chash(struct chash_backend *b,struct chash_frontend *f);

int
put_raw(unsigned char* data,
        struct item *item,
        struct node *target);
int
get_raw(unsigned char* hash, unsigned char* buf, uint32_t size);

int sync_node(unsigned char *buf,uint32_t size,struct node *target);

#endif
