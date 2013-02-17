CC = gcc
CFLAGS = -g -Wall -std=gnu99 -DCOMPILE_DEBUG=1
LDFLAGS = 

.PHONY: murmur

all: murmur

murmur:
	$(CC) $(CFLAGS) $@.c -o $@