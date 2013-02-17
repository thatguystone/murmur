#define _GNU_SOURCE

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "murmur.h"

/** 
 * A single data point
 */
struct point {
	/**
	 * The start of the interval this point is for.
	 */
	uint64_t interval;
	
	/**
	 * The value at this time.
	 */
	uint64_t value;
} __attribute__ ((packed));

/**
 * The format of the murmur header in the murmur file.
 */
struct murmur_header {
	/**
	 * How to aggregate points in the file.
	 */
	char aggregation;
	
	/**
	 * The amount of time that can be stored in this file.
	 */
	uint64_t max_retention;
	
	/**
	 * Specifies the fraction of data points in a propagation interval
	 * that must have known values for a propagation to occur.
	 */
	char x_files_factor;
	
	/**
	 * The number of archives contained in this file.
	 */
	uint32_t archive_count;
} __attribute__ ((packed));

/**
 * The format of the archive header in the murmur file.
 */
struct archive_header {
	/**
	 * Where the archive begins.
	 */
	uint32_t offset;
	
	/**
	 * The number of seconds each point takes in the archive.
	 */
	uint32_t seconds_per_point;
	
	/**
	 * The number of points in this archive.
	 */
	uint32_t points;
} __attribute__ ((packed));

static int _murmur_validate_archives_sort(const void *a, const void *b) {
	return ((struct archive_header*)a)->seconds_per_point - ((struct archive_header*)b)->seconds_per_point;
}

static char _murmur_validate_archives(const uint32_t archive_count, struct archive_header *archive_headers) {
	if (archive_headers == NULL || archive_count == 0) {
		M_ERROR("Can't create a database without archives...");
		return 0;
	}
	
	qsort(archive_headers, archive_count, sizeof(*archive_headers), _murmur_validate_archives_sort);
	
	for (uint32_t i = 0; i < archive_count - 1; i++) {
		struct archive_header arch = archive_headers[i];
		struct archive_header next_arch = archive_headers[i+1];
		
		uint32_t spp = arch.seconds_per_point;
		uint32_t next_spp = next_arch.seconds_per_point;
		
		if (spp == next_spp) {
			M_ERROR("A murmur database may not have two archives with the same precision (%d == %d).", spp, next_spp);
			return 0;
		}
		
		if (next_spp % spp != 0) {
			M_ERROR("Lower precision archives must evenly divide higher precision archives (%d %% %d != 0).", next_spp, spp);
			return 0;
		}
		
		uint32_t retention = spp * arch.points;
		uint32_t next_retention = next_spp * next_arch.points;
		if (retention > next_retention) {
			M_ERROR("Lower precision archives must cover larger time intervals than higher precision ones (%d < %d)", retention, next_retention);
			return 0;
		}
		
		uint32_t archive_points = arch.points;
		uint32_t points_per_consolidation = next_spp / spp;
		if (archive_points < points_per_consolidation) {
			M_ERROR("Each archive must have at least enough points to consolidate to the next archive (archive%d consolidates %d of archive%d's points, but it only has %d total points)", i + 1, points_per_consolidation, i, archive_points);
			return 0;
		}
	}
	
	return 1;
}

static int _murmur_archive_spec_unit(long *precision, const char *unit) {
	const char const *SECONDS = "seconds";
	const char const *MINUTES = "minutes";
	const char const *HOURS = "hours";
	const char const *DAYS = "days";
	const char const *WEEKS = "weeks";
	const char const *YEARS = "years";
	
	if (strstr(SECONDS, unit) == SECONDS) {
		// No modification necessary
	} else if (strstr(MINUTES, unit) == MINUTES) {
		*precision *= 60;
	} else if (strstr(HOURS, unit) == HOURS) {
		*precision *= 60*60;
	} else if (strstr(DAYS, unit) == DAYS) {
		*precision *= 60*60*24;
	} else if (strstr(WEEKS, unit) == WEEKS) {
		*precision *= 60*60*24*7;
	} else if (strstr(YEARS, unit) == YEARS) {
		*precision *= 60*60*24*7*365;
	} else {
		return 0;
	}
	
	return 1;
}

/**
 * Parses the archive spec into a `struct archive` array.
 *
 * @param spec The string spec to parse.
 * @param[out] archives The archives found in the spec. This MUST ALWAYS be free'd when finished.
 *
 * @return The number of archives parsed.
 */
static uint32_t _murmur_parse_archive_spec(const char *spec, struct archive_header **archive_headers) {
	*archive_headers = NULL;
	
	if (spec == NULL || *spec == '\0') {
		M_ERROR("There is no spec to parse...");
		return 0;
	}
	
	uint32_t count = 1; // 1 comma = 2 values
	for (uint32_t i = 0; spec[i]; count += spec[i] == ',', i++);
	
	struct archive_header *arch_headers = malloc(count * sizeof(*arch_headers));
	
	char *dup_spec = strdup(spec);
	char *curr = dup_spec;
	for (uint32_t i = 0; i < count; i++) {
		char *end;
		char *comma = strchrnul(curr, ',');
		char *colon = strchr(curr, ':');
		
		if (colon > comma) {
			goto error;
		}
		
		*comma = '\0';
		*colon = '\0';
		
		long seconds_per_point = strtol(curr, &end, 10);
		if (end != colon && !_murmur_archive_spec_unit(&seconds_per_point, end)) {
			goto error;
		}
		
		long points = strtol(colon + 1, &end, 10);
		if (end != comma) {
			if (!_murmur_archive_spec_unit(&points, end)) {
				goto error;
			}
			
			points /= seconds_per_point;
		}
		
		struct archive_header *ah = &arch_headers[i];
		ah->seconds_per_point = seconds_per_point;
		ah->points = points;
		
		curr = comma + 1;
	}
	
	*archive_headers = arch_headers;
	free(dup_spec);
	return count;

error:
	M_ERROR("Invalid archive spec");
	free(dup_spec);
	free(arch_headers);
	return 0;
}

static int _murmur_get_archive(struct murmur *mmr, const uint64_t timestamp, struct archive **archive) {
	uint32_t diff = time(NULL) - timestamp;
	
	// If the time is too far in the past, past the support of our file
	if (diff > mmr->max_retention) {
		return -1;
	}
	
	// If the value is in the future
	if (diff <= 0) {
		return -1;
	}
	
	// Find the highest-precision archive that covers timestamp
	struct archive *arch;
	for (uint32_t i = 0; i < mmr->archive_count; i++) {
		arch = &mmr->archives[i];
		if (arch->retention > diff) {
			break;
		}
	}
	
	*archive = arch;
	return 0;
}

int murmur_create(const char *path, const char *archive_spec, const enum aggregation_method aggregation, const char x_files_factor) {
	int fd = open(path, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP);
	if (fd == -1) {
		perror("Could not open file for writing");
		return -1;
	}
	
	struct archive_header *arch_headers;
	uint32_t archive_count = _murmur_parse_archive_spec(archive_spec, &arch_headers);
	
	if (!archive_count || !_murmur_validate_archives(archive_count, arch_headers)) {
		return -1;
	}
	
	uint64_t max_retention = 0;
	uint32_t offset = sizeof(struct murmur_header) + (archive_count * sizeof(*arch_headers));
	
	for (uint32_t i = 0; i < archive_count; i++) {
		struct archive_header *ah = &arch_headers[i];
		
		max_retention += ah->seconds_per_point * ah->points;
		
		// Prepare the struct for writing to disk
		ah->offset = htobe32(offset);
		
		// Can't work with a number in the wrong endianess
		offset += ah->points * sizeof(struct point);
		
		ah->seconds_per_point = htobe32(ah->seconds_per_point);
		ah->points = htobe32(ah->points);
	}
	
	struct murmur_header header = {
		.aggregation = htobe32(aggregation == 0 ? average : aggregation),
		.max_retention = htobe64(max_retention),
		.x_files_factor = x_files_factor,
		.archive_count = htobe32(archive_count),
	};
	
	int ret = 0;
	
	int len = sizeof(header);
	int curr_pos = len;
	if (write(fd, &header, len) != len) {
		perror("Could not write murmur header");
		ret = -1;
		goto done;
	}
	
	len = archive_count * sizeof(*arch_headers);
	curr_pos += len;
	if (write(fd, arch_headers, len) != len) {
		perror("Could not write archive headers");
		ret = -1;
		goto done;
	}
	
	if (fallocate(fd, 0, curr_pos, offset - curr_pos) != 0) {
		perror("Could not allocate archive area");
		ret = -1;
		goto done;
	}
	
done:
	close(fd);
	free(arch_headers);
	return ret;
}

struct murmur* murmur_open(const char *path) {
	// So that the goto error's work
	struct murmur *mmr = NULL;
	
	int fd = open(path, O_RDWR);
	if (fd == -1) {
		perror("Could not open murmur file");
		goto error;
	}
	
	mmr = malloc(sizeof(*mmr));
	memset(mmr, 0, sizeof(*mmr));
	mmr->fd = fd;
	
	struct murmur_header h;
	if (read(fd, &h, sizeof(h)) != sizeof(h)) {
		M_ERROR("Could not read murmur header: file is corrupted");
		goto error;
	}
	
	mmr->aggregation = be32toh(h.aggregation);
	mmr->max_retention = be64toh(h.max_retention);
	mmr->x_files_factor = h.x_files_factor;
	mmr->archive_count = be32toh(h.archive_count);
	
	if (mmr->archive_count == 0) {
		M_ERROR("Murmur file corrupted: no archives specified");
		goto error;
	}
	
	mmr->archives = malloc(mmr->archive_count * sizeof(*mmr->archives));
	for (uint32_t i = 0; i < mmr->archive_count; i++) {
		struct archive *arch = mmr->archives + i;
		struct archive_header ah;
		
		if (read(fd, &ah, sizeof(ah)) != sizeof(ah)) {
			M_ERROR("Could not read archive header: file is corrupted");
			goto error;
		}
		
		arch->offset = be32toh(ah.offset);
		arch->seconds_per_point = be32toh(ah.seconds_per_point);
		arch->points = be32toh(ah.points);
		arch->retention = arch->seconds_per_point * arch->points;
		
		M_DEBUG("Archive header: %u %u %u", 
			be32toh(ah.offset),
			be32toh(ah.seconds_per_point),
			be32toh(ah.points)
		);
	}
	
	return mmr;
	
error:
	if (mmr == NULL) {
		close(fd);
	}
	murmur_close(mmr);
	return NULL;
}

void murmur_close(struct murmur *mmr) {
	if (mmr != NULL) {
		close(mmr->fd);
		free(mmr->archives);
		free(mmr);
	}
}

static int _murmur_seek_to_point(struct murmur *mmr, struct archive *archive, const uint64_t timestamp, uint64_t *interval) {
	*interval = timestamp - (timestamp % archive->seconds_per_point);
	
	if (lseek(mmr->fd, sizeof(struct point) * ((*interval % archive->retention) / archive->seconds_per_point), SEEK_SET) == -1) {
		perror("Could not seek to record");
		return -1;
	}
	
	return 0;
}

int murmur_set(struct murmur *mmr, const uint64_t timestamp, const double value) {
	struct archive *arch;
	if (_murmur_get_archive(mmr, timestamp, &arch) != 0) {
		M_ERROR("Could not locate suitable archive for item at timestamp: %ld", timestamp);
		return -1;
	}
	
	uint64_t interval = 0;
	if (_murmur_seek_to_point(mmr, arch, timestamp, &interval) != 0) {
		return -1;
	}
	
	// First we update the highest-precision archive
	
	struct point pt = {
		.interval = htobe64(interval),
		.value = htobe64(value),
	};
	
	if (write(mmr->fd, &pt, sizeof(pt)) != sizeof(pt)) {
		perror("Could not write record");
		return -1;
	}
	
// const unsigned char * const px = (unsigned char*)&pt.value;
// for (int i = 0; i < sizeof(pt.value); ++i) printf("%02X ", px[i]);
// printf("\n");

	return 0;
}

int murmur_get(struct murmur *mmr, const uint64_t timestamp, double * const value) {
	struct archive *arch;
	if (_murmur_get_archive(mmr, timestamp, &arch) != 0) {
		M_ERROR("Could not locate suitable archive for item at timestamp: %ld", timestamp);
		return -1;
	}
	
	uint64_t interval = 0;
	if (_murmur_seek_to_point(mmr, arch, timestamp, &interval) != 0) {
		return -1;
	}
	
	struct point pt;
	if (read(mmr->fd, &pt, sizeof(pt)) != sizeof(pt)) {
		M_PERROR("Could not read record");
		return -1;
	}
	
	*value = be64toh(pt.value);
	
	return 0;
}

int main(int argc, char **argv) {
	if (argc < 2) {
		M_ERROR("Which murmur file?");
		return 1;
	}
	
	char *path = *(argv + 1);
	
	struct stat buf;
	if (stat(path, &buf) == -1) {
		if (errno != ENOENT) {
			perror("That path is screwed");
			return 1;
		}
		
		if (murmur_create(path, "10s:5h,60s:5h,1h:1y", average, 50) != 0) {
			return 1;
		}
	}
	
	struct murmur *mmr = murmur_open(path);
	if (mmr == NULL) {
		return 1;
	}
	
	int at = time(NULL)-10;
	if (murmur_set(mmr, at, 100) != 0) {
		return 1;
	}
	
	double v = 0;
	murmur_get(mmr, at, &v);
	M_DEBUG("Get: %f", v);
	
	murmur_close(mmr);
}