#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

#include "libmurmur.h"

static int _create(const char *path, const int specc, char **specv) {
	struct stat buf;
	if (stat(path, &buf) == -1) {
		if (errno != ENOENT) {
			M_PERROR("That path sucks");
			return 1;
		}
		
		if (murmur_create(path, specc, specv, agg_average, 50) != 0) {
			return 1;
		}
		
		return 0;
	}
	
	M_ERROR("That path already exists!");
	return 1;
}

static int _dump(char *path) {
	struct murmur *mmr = murmur_open(path);
	if (mmr == NULL) {
		return 1;
	}
	
	if (murmur_dump(mmr) != 0) {
		return 1;
	}
	
	murmur_close(mmr);
	
	return 0;
}

static int _info(char *path) {
	struct murmur *mmr = murmur_open(path);
	if (mmr == NULL) {
		return 1;
	}
	
	if (murmur_dump_info(mmr) != 0) {
		return 1;
	}
	
	murmur_close(mmr);
	
	return 0;
}

static void _show_usage() {
	fprintf(stderr, 
		"Usage: murmur COMMAND ...\n"
		"\n"
		"Commands:\n"
		"  create   creates a new murmur database\n"
		"  dump     dumps the contents of a database\n"
		"  info     dumps information about a database\n"
	);
}

int main(int argc, char **argv) {
	if (argc < 2) {
		M_ERROR("You must specify an action.");
		_show_usage();
		return 1;
	}
	
	if (argc < 3) {
		M_ERROR("You must specify a murmur file.");
		_show_usage();
		return 1;
	}
	
	char *command = *(argv + 1);
	char *path = *(argv + 2);
	
	if (strcmp("create", command) == 0) {
		return _create(path, argc-3, argv+3);
	} else if (strcmp("dump", command) == 0) {
		return _dump(path);
	} else if (strcmp("info", command) == 0) {
		return _info(path);
	}
	
	_show_usage();
	return 1;
}