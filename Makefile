PHONY = all lib fresh
.DEFAULT_GOAL := all
WFLAGS := -Wall -Wextra -Werror
all: lib 

fresh: clean all

chash: chash.o chash_backend_linked.o chash_frontend_mirror.o

lib: chash.o chash_backend_linked.o chash_frontend_mirror.o
	ar rcs libchash.a chash.o chash_backend_linked.o chash_frontend_mirror.o

small: clean
	@$(MAKE) CCFLAGS="-Os -m32" all

chash.o: src/chash.c include/chash.h backends/chash_backend_linked.c include/chash_backend.h include/chash_frontend.h
	$(CC) -DCHASH_BACKEND_LINKED -c src/chash.c backends/chash_backend_linked.c frontends/chash_frontend_mirror.c $(CCFLAGS) $(WFLAGS)

clean:
	rm -rf *.a *.o
