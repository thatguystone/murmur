CC = gcc
CFLAGS = -O2 -Wall -std=gnu99
LDFLAGS = 

all: murmur

murmur: libmurmur.c libmurmur.h murmur.c 
	$(CC) $(CFLAGS) $@.c $< -o $@

murmur_test: libmurmur.c libmurmur.h murmur_test.c
	$(CC) $(CFLAGS) $@.c -o $@

debug: CFLAGS += -g -DCOMPILE_DEBUG=1
debug: murmur

test: CFLAGS += -DCOMPILE_TEST=1
test: murmur_test
	./murmur_test

clean:
	rm -f murmur murmur_test murmur_test.mmr