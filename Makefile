CC = gcc
CFLAGS = -g -Wall -std=gnu99 -DCOMPILE_DEBUG=1
LDFLAGS = 

all: murmur

murmur: libmurmur.c libmurmur.h murmur.c 
	$(CC) $(CFLAGS) $@.c $< -o $@

test: murmur
	@rm -f test.mmr
	@echo
	@./murmur test.mmr 10s:60s 60s:5m

clean:
	rm -f murmur
	rm -f test.mmr