#ifndef _LIBCHASH_H
#define _LIBCHASH_H
#include "../chord/chord.h"
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#define CHASH_OK (0)
#define CHASH_ERR (-1)
#define CHASH_CHUNK_SIZE (256)

int
put(unsigned char* data, size_t size, nodeid_t* id);

int
get(nodeid_t id, unsigned char* buf);

int
handle_get(unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size);
int
handle_put(unsigned char* data,
           nodeid_t src,
           int sock,
           struct sockaddr* src_addr,
           size_t src_addr_size);
int
init_chash(void);
#endif