PHONY = all lib fresh
.DEFAULT_GOAL := all
WFLAGS := -Wall -Wextra -Werror
all: lib 

fresh: clean all

chash: chash.o

lib: chash.o
	ar rcs libchash.a chash.o

small: clean
	@$(MAKE) CCFLAGS="-Os -m32" all

chash.o: src/chash.c include/chash.h
	$(CC) -c src/chash.c $(CCFLAGS) $(WFLAGS)

clean:
	rm -rf *.a *.o
