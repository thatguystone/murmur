CC = gcc
CFLAGS = -g -Wall -std=gnu99
LDFLAGS = 

.PHONY: murmur

all: murmur

murmur:
	$(CC) $(CFLAGS) $@.c -o $@