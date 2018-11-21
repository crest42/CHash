#ifndef _LIBCHASH_BACKEND_H
#define _LIBCHASH_BACKEND_H
#include "chash.h"

struct key**
get_first_key(void);

int
chash_backend_init(void *data);

int
chash_backend_put(struct item* item, unsigned char* data);

int
chash_backend_get_data(unsigned char *hash, size_t len, unsigned char *data);

int
chash_backend_get(unsigned char *hash, nodeid_t *id, uint32_t *size);

int
remove_key(struct key* key);

int
get_key(nodeid_t id, struct key *k);

int
add_key(struct key* k, unsigned char* d);

size_t
get_size(void);
#endif //_LIBCHASH_BACKEND_H
