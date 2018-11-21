#ifndef CHORD_FRONTEND_H
#define CHASH_FRONTEND_H
#define REPLICAS (2)

int
chash_frontend_put(uint32_t key_size,
                 unsigned char* key,
                 uint32_t offset,
                 uint32_t data_size,
                 unsigned char* data);

int
chash_frontend_get(uint32_t key_size,
                 unsigned char* key,
                 uint32_t buf_size,
                 unsigned char* buf);

int chash_frontend_maint(void *data);

int
handle_sync(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper* s,
            size_t msg_size);

int
handle_sync_fetch(chord_msg_t type,
            unsigned char* data,
            nodeid_t src,
            struct socket_wrapper *s,
            size_t msg_size);

int
chash_frontend_periodic(void* data);
#endif // CHASH_FRONTEND_MIRROR_H
