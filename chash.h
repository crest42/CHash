#ifndef _LIBCHASH_H
#define _LIBCHASH_H
#include "../chord/include/chord.h"
#include <stdint.h>
#define CHASH_OK (0)
#define CHASH_ERR (-1)
#define CHASH_CHUNK_SIZE (256)

typedef int (*chash_storage_put)(uint32_t,
                                 uint32_t,
                                 nodeid_t,
                                 unsigned char*,
                                 unsigned char*);

typedef int (*chash_storage_get)(unsigned char*,
                                 nodeid_t *,
                                 uint32_t *,
                                 unsigned char**);

struct chash_storage_backend {
    chash_storage_put put;
    chash_storage_get get;
    void *data;
};

int
put(unsigned char* data, size_t size, nodeid_t* id);

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
init_chash(struct chash_storage_backend *b);

int
chash_get_block(int block, unsigned char* ret);
int
chash_put_block(unsigned char* data, int block,uint32_t offset, uint32_t size);

#endif
