PHONY = all example lib
.DEFAULT_GOAL := all
WFLAGS := -Wall -Wextra -Werror
all: example

example: lib example.o
	$(CC) example.o -o example -lpthread -lcrypto libchash.a ../chord/libchord.a $(CCFLAGS) $(WFLAGS)

chash: chash.o

lib: chash.o
	ar rcs libchash.a chash.o

small: clean
	@$(MAKE) CCFLAGS="-Os -m32" all

example.o: example.c
	$(CC) -c example.c -lpthread $(CCFLAGS) $(WFLAGS)

chash.o: chash.c chash.h
	$(CC) -c chash.c $(CCFLAGS) $(WFLAGS)

clean:
	rm -rf *.a *.o example
