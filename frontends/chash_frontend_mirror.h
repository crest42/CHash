#ifndef CHORD_FRONTEND_MIRROR_H
#define CHASH_FRONTEND_MIRROR_H
#define REPLICAS (2)

int
chash_mirror_put(uint32_t key_size,
                 unsigned char* key,
                 uint32_t offset,
                 uint32_t data_size,
                 unsigned char* data);

int
chash_mirror_get(uint32_t key_size,
                 unsigned char* key,
                 uint32_t buf_size,
                 unsigned char* buf);

#endif //CHASH_FRONTEND_MIRROR_H