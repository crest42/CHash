#ifndef _LIBCHASH_BACKEND_LINKED_H
#define _LIBCHASH_BACKEND_LINKED_H
#include "../chash.h"
#include "chash_backend_linked_internal.h"
#include <stdio.h> //TODO: Remove
#define REPLICAS (2)
struct key**
get_first_key(void);

struct key**
get_last_key(void);

int chash_linked_list_put(struct item *item, unsigned char *data);

int chash_linked_list_get(unsigned char *hash, nodeid_t *id, uint32_t *size, unsigned char **data);

int chash_linked_list_maint(void *data);
#endif //_LIBCHASH_STORE_LINKED_H