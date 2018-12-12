#include "../include/chash.h"
#include "../include/chash_backend.h"
#include "../../chord/include/network.h"
#include "mtd.h"
//#define CHASH_BACKEND_RIOT_MTD
#ifdef CHASH_BACKEND_RIOT_MTD
extern struct chash_frontend frontend;

struct key* first_key;
struct key* last_key;
mtd_dev_t *mtd_backend_dev;

struct key empty = { .id = 0, .block = 0, .size = 0, .hash = {'\0'}, .data = NULL, .next = NULL};

static int get_sector_count(void) {
  return mtd_backend_dev->sector_count;
}

static int get_page_size(void) {
  return mtd_backend_dev->page_size;
}

static int get_pages_per_sector(void) {
  return mtd_backend_dev->pages_per_sector;
}

static int b_write_wrapper(mtd_dev_t *dev, const void *buff, uint32_t addr, uint32_t size) {
  int ret = -1;
  size_t sector_size = get_pages_per_sector() * get_page_size();
  uint32_t sector_start = addr / sector_size;
  uint32_t sector_end = (addr + size) / sector_size;
  uint32_t offset = addr % sector_size;
  uint32_t size_remaining = size;
  char buf[sector_size];
  for (uint32_t i = sector_start; i <= sector_end;i++) {
    if(i > sector_start) {
      offset = 0;
    }
    if(offset+size_remaining > sector_size) {
      size = sector_size - offset;
    } else {
      size = size_remaining;
    }
    addr = i * sector_size;
    mtd_backend_dev->driver->read(
      dev, buf, addr, sizeof(buf));
    memcpy(buf + offset, buff, size);

    if ((ret = mtd_backend_dev->driver->erase(
           dev, addr, sector_size)) < 0) {
      return ret;
    }
    for (int i = 0; i < (int)(sector_size / dev->page_size);i++) {
      if ((ret = mtd_backend_dev->driver->write(dev, buf+(i*dev->page_size), ((addr)+(i*dev->page_size)), dev->page_size)) <
          0) {
        return ret;
      }
    }
    size_remaining -= size;
  }
  return ret;
}

size_t get_size(void) {
  return get_sector_count() * get_page_size() * get_pages_per_sector();
}

int chash_backend_init(void *data) {
  mtd_backend_dev = (mtd_dev_t*)data;
  mtd_backend_dev->driver->init(mtd_backend_dev);
  int ret = b_write_wrapper(
    mtd_backend_dev, &empty, 0, sizeof(struct key));
  return ret;
}

struct key**
get_first_key(void)
{
  return &first_key;
}

struct key**
get_last_key(void)
{
  return &last_key;
}

int remove_key(struct key *key) {
  struct key tmp;
  uint32_t addr = 0;
  mtd_backend_dev->driver->read(mtd_backend_dev, &tmp, addr, sizeof(struct key));
  while (tmp.id != key->id) {
    addr = (uint32_t)tmp.next;
    mtd_backend_dev->driver->read(
      mtd_backend_dev, &tmp, addr, sizeof(struct key));
  }
  if(tmp.id == key->id) {
    b_write_wrapper(
      mtd_backend_dev, &empty, addr, sizeof(struct key));
  }
  return CHASH_OK;
}

int get_key(unsigned char *hash, uint32_t id, struct key *k) {
  (void)hash;
  struct key tmp;
  uint32_t addr = 0;
  mtd_backend_dev->driver->read(mtd_backend_dev, &tmp, addr, sizeof(struct key));
  while (tmp.id != id) {
    if(tmp.next == NULL || tmp.id == 0) {
      break;
    }
    addr = (uint32_t)tmp.next;
    mtd_backend_dev->driver->read(
      mtd_backend_dev, &tmp, addr, sizeof(struct key));
  }
  if(tmp.id == id) {
    memcpy(k, &tmp, sizeof(struct key));
    return CHORD_OK;
  }
  return CHASH_ERR;
}

int add_key(struct key *k, unsigned char *d) {
  struct key tmp;
  uint32_t addr = 0;
  mtd_backend_dev->driver->read(
    mtd_backend_dev, &tmp, addr, sizeof(struct key));
  int i = 0;
  while (tmp.id != 0) {
    i++;
    addr = (uint32_t)tmp.next;
    mtd_backend_dev->driver->read(
      mtd_backend_dev, &tmp, addr, sizeof(struct key));
  }
  k->data = ((unsigned char *)addr) + sizeof(struct key);
  k->next = (struct key *)(k->data + k->size);
  int ret = 0;
  if ((ret = b_write_wrapper(mtd_backend_dev, k, addr, sizeof(struct key))) <
      0) {
    return CHASH_ERR;
  }


  if (b_write_wrapper(mtd_backend_dev, d, (uint32_t)k->data, k->size) < 0) {
    return CHASH_ERR;
  }

  if (b_write_wrapper(
        mtd_backend_dev, &empty, (uint32_t)k->next, sizeof(empty)) < 0) {
    return CHASH_ERR;
  }
  return CHASH_OK;
}
int chash_backend_put(struct item *item, unsigned char *data) {
  nodeid_t id = get_mod_of_hash(item->hash, CHORD_RING_SIZE);
  struct key k;
  if (get_key(NULL,id, &k) == CHORD_OK) {
    assert((item->offset + item->size) <= k.size);
    b_write_wrapper(mtd_backend_dev,data,(uint32_t)(k.data+item->offset),item->size);
  } else {
    struct key k;
    memcpy(k.hash, item->hash, HASH_DIGEST_SIZE);
    k.id = id;
    k.next = NULL;
    k.size = item->size;
    k.block = item->block;
    k.data = NULL;
    add_key(&k, data);
  }
  return CHASH_OK;
}

int chash_backend_get_data(unsigned char *hash, size_t len, unsigned char *data) {
  struct key tmp;
  uint32_t addr = 0;
  mtd_backend_dev->driver->read(mtd_backend_dev, &tmp, addr, sizeof(struct key));
  while (memcmp(hash,tmp.hash,HASH_DIGEST_SIZE) != 0) {
    addr = (uint32_t)tmp.next;
    mtd_backend_dev->driver->read(
      mtd_backend_dev, &tmp, addr, sizeof(struct key));
  }
  if (memcmp(hash,tmp.hash,HASH_DIGEST_SIZE) == 0) {
    mtd_backend_dev->driver->read(
        mtd_backend_dev, data, (uint32_t)tmp.data, len);
    return CHASH_OK;
  }
  return CHASH_ERR;
}

int chash_backend_get(unsigned char *hash, nodeid_t *id, uint32_t *size) {

  struct key tmp;
  uint32_t addr = 0;
  mtd_backend_dev->driver->read(mtd_backend_dev, &tmp, addr, sizeof(struct key));

  while (memcmp(hash,tmp.hash,HASH_DIGEST_SIZE) != 0) {
    if(tmp.next == NULL) {
      break;
    }
    addr = (uint32_t)tmp.next;
    mtd_backend_dev->driver->read(
      mtd_backend_dev, &tmp, addr, sizeof(struct key));
  }
  if (memcmp(hash, tmp.hash, HASH_DIGEST_SIZE) == 0) {
    *size = tmp.size;
    *id = tmp.id;
  } else {
    *size = 0;
    *id = 0;
  }
  return CHASH_OK;
}

#endif
//(uint32_t key_size, unsigned char *key, uint32_t offset, uint32_t data_size, unsigned char *data)
