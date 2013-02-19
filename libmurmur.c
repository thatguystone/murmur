#define _GNU_SOURCE

#include <endian.h>
#include <fcntl.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include "libmurmur.h"

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

/**
 * Given the archive spec, validates that it is okay to use as a murmur archive.
 */
static char _murmur_validate_archives(const uint32_t archive_count, struct archive_header *archive_headers) {
	if (archive_headers == NULL || archive_count == 0) {
		M_ERROR("Can't create a database without archives...");
		return -1;
	}
	
	qsort(archive_headers, archive_count, sizeof(*archive_headers), _murmur_validate_archives_sort);
	
	for (uint32_t i = 0; i < archive_count - 1; i++) {
		struct archive_header arch = archive_headers[i];
		struct archive_header next_arch = archive_headers[i+1];
		
		uint32_t spp = arch.seconds_per_point;
		uint32_t next_spp = next_arch.seconds_per_point;
		
		if (spp == next_spp) {
			M_ERROR("A murmur database may not have two archives with the same precision (%d == %d).", spp, next_spp);
			return -1;
		}
		
		if (next_spp % spp != 0) {
			M_ERROR("Lower precision archives must evenly divide higher precision archives (%d %% %d != 0).", next_spp, spp);
			return -1;
		}
		
		uint32_t retention = spp * arch.points;
		uint32_t next_retention = next_spp * next_arch.points;
		if (retention > next_retention) {
			M_ERROR("Lower precision archives must cover larger time intervals than higher precision ones (%d < %d)", retention, next_retention);
			return -1;
		}
		
		uint32_t archive_points = arch.points;
		uint32_t points_per_consolidation = next_spp / spp;
		if (archive_points < points_per_consolidation) {
			M_ERROR("Each archive must have at least enough points to consolidate to the next archive (archive%d consolidates %d of archive%d's points, but it only has %d total points)", i + 1, points_per_consolidation, i, archive_points);
			return -1;
		}
	}
	
	return 0;
}

/**
 * Converts a time unit to seconds.
 *
 * Examples:
 * 	  10s = 10
 *    60s = 60
 *    1m = 60
 *    2m = 120
 *
 * @param precision The base precision, without a transform
 * @param unit The unit, as a string (s, sec, secon, m, minutes, etc)
 * @param colon The plae in the string where the colon is located (in the "10s:1m" format)
 *
 * @return Number of seconds, or -1 on error.
 */
static int _murmur_spec_to_seconds(long *precision, const char *unit, uint32_t colon) {
	const char const *SECONDS = "seconds";
	const char const *MINUTES = "minutes";
	const char const *HOURS = "hours";
	const char const *DAYS = "days";
	const char const *WEEKS = "weeks";
	const char const *YEARS = "years";
	
	// Longest unit name = seconds + 1 for null terminator
	char _unit[8];
	memset(_unit, 0, sizeof(_unit));
	strncpy(_unit, unit, colon > 0 && colon < sizeof(_unit)-1 ? colon : sizeof(_unit)-1);
	
	if (strstr(SECONDS, _unit) == SECONDS) {
		// No modification necessary
	} else if (strstr(MINUTES, _unit) == MINUTES) {
		*precision *= 60;
	} else if (strstr(HOURS, _unit) == HOURS) {
		*precision *= 60*60;
	} else if (strstr(DAYS, _unit) == DAYS) {
		*precision *= 60*60*24;
	} else if (strstr(WEEKS, _unit) == WEEKS) {
		*precision *= 60*60*24*7;
	} else if (strstr(YEARS, _unit) == YEARS) {
		*precision *= 60*60*24*7*365;
	} else {
		return -1;
	}
	
	return 0;
}

/**
 * Parses the archive spec into a `struct archive` array.
 *
 * @param spec The string spec to parse.
 * @param[out] archives The archives found in the spec. This MUST ALWAYS be free'd when finished.
 *
 * @return The number of archives parsed.
 */
static uint32_t _murmur_parse_archive_spec(const uint32_t specc, char **specv, struct archive_header **archive_headers) {
	*archive_headers = NULL;
	
	if (specc == 0) {
		M_ERROR("There is no spec to parse...");
		return 0;
	}
	
	struct archive_header *arch_headers = malloc(specc * sizeof(*arch_headers));
	
	for (uint32_t i = 0; i < specc; i++) {
		const char *curr = *(specv + i);
		
		char *end;
		char *colon = strchr(curr, ':');
		
		if (colon == NULL) {
			goto error;
		}
		
		long seconds_per_point = strtol(curr, &end, 10);
		if (end != colon && _murmur_spec_to_seconds(&seconds_per_point, end, colon - end) == -1) {
			M_DEBUG("1 %d", end == colon);
			goto error;
		}
		
		long points = strtol(colon + 1, &end, 10);
		if (*end != '\0') {
			if (_murmur_spec_to_seconds(&points, end, 0) == -1) {
				M_DEBUG("2");
				goto error;
			}
			
			points /= seconds_per_point;
		}
		
		struct archive_header *ah = &arch_headers[i];
		ah->seconds_per_point = seconds_per_point;
		ah->points = points;
	}
	
	*archive_headers = arch_headers;
	return specc;

error:
	M_ERROR("Invalid archive spec");
	free(arch_headers);
	return 0;
}

/**
 * Given the list of archives, goes through and finds the most-precise archive to use for the given timestamp.
 *
 * @param mmr Obvious
 * @param timestamp The timestamp to fit
 * @param archive[out] The archive that the point should be written into
 */
static int _murmur_get_archive(struct murmur *mmr, const uint64_t timestamp, struct murmur_archive **archive) {
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
	struct murmur_archive *arch;
	for (uint32_t i = 0; i < mmr->archive_count; i++) {
		arch = &mmr->archives[i];
		if (arch->retention > diff) {
			break;
		}
	}
	
	*archive = arch;
	return 0;
}

/**
 * Moves the offset of the file description in mmr to the start of the point's location in the file.
 *
 * @param mmr Obvious
 * @param arch The archive to move relative to
 * @param timestamp The timestamp to seek to
 * @param[out] interval The time that should be written to disk
 */
static int _murmur_seek_to_point(struct murmur *mmr, struct murmur_archive *arch, const uint64_t timestamp, uint64_t *interval) {
	*interval = timestamp - (timestamp % arch->seconds_per_point);
	
	int seeked = lseek(mmr->fd, arch->offset + (sizeof(struct point) * ((*interval % arch->retention) / arch->seconds_per_point)), SEEK_SET);
	if (seeked == -1) {
		M_PERROR("Could not seek to record");
	}
	
	return seeked;
}

/**
 * Given an aggregation method and a series of points, read directly from disk,
 * this aggregates the points into 1 point using the supplied method.
 */
static double _murmur_aggregate(enum aggregation_method aggregation, uint64_t pointsc, struct point *pointsv) {
	uint64_t last_interval, last_i;
	double val = be64toh(pointsv->value);
	
	switch (aggregation) {
		case average:
		default:
			for (uint64_t i = 1; i < pointsc; i++) {
				val += be64toh((pointsv + i)->value);
			}
			val /= pointsc;
			break;
			
		case sum:
			for (uint64_t i = 1; i < pointsc; i++) {
				val += be64toh((pointsv + i)->value);
			}
			break;
		
		case last:
			last_interval = be64toh(pointsv->interval);
			last_i = 0;
			
			for (uint64_t i = 1; i < pointsc; i++) {
				uint64_t l = be64toh((pointsv + i)->interval);
				if (l > last_interval) {
					last_interval = i;
					last_i = i;
				}
			
			}
			
			val = be64toh((pointsv + last_i)->value);
			
			break;
		
		case max:
			for (uint64_t i = 1; i < pointsc; i++) {
				double v = be64toh((pointsv + i)->value);
				if (v > val) {
					val = v;
				}
			}
			break;
		
		case min:
			for (uint64_t i = 1; i < pointsc; i++) {
				double v = be64toh((pointsv + i)->value);
				if (v < val) {
					val = v;
				}
			}
			break;
	}
	
	return val;
}

// Forward declaration: _murmur_propogate and _murmur_arch_set rely on each other
static int _murmur_propogate(struct murmur *mmr, struct murmur_archive *arch, uint64_t timestamp);

/**
 * Given an archive, sets a value in the archive.
 *
 * @param mmr Obvious
 * @param arch The archive to set the value in.
 * @param timestamp The timestamp of the data to be set
 * @param value The value of the data to be set
 */
static int _murmur_arch_set(struct murmur *mmr, struct murmur_archive *arch, const uint64_t timestamp, const double value) {
	uint64_t interval = 0;
	if (_murmur_seek_to_point(mmr, arch, timestamp, &interval) == -1) {
		return -1;
	}
	
	struct point pt = {
		.interval = htobe64(interval),
		.value = htobe64(value),
	};
	
	if (write(mmr->fd, &pt, sizeof(pt)) != sizeof(pt)) {
		M_PERROR("Could not write record");
		return -1;
	}
	
	return _murmur_propogate(mmr, arch, timestamp);
}

/**
 * Takes the values from the more-precise archive and propogates them, recursively, to the less-precise
 * archives.
 *
 * @param mmr Obvious
 * @param arch The higher-precision archive
 * @param timestamp The timestamp of the data to be propogated down.
 */
static int _murmur_propogate(struct murmur *mmr, struct murmur_archive *arch, const uint64_t timestamp) {
	if (arch->lower == NULL) {
		return 0;
	}
	
	uint64_t interval_start = 0;
	struct murmur_archive *lower = arch->lower;
	
	// So that the error jumps work
	struct point points[lower->seconds_per_point / arch->seconds_per_point];
	
	uint64_t record_start = _murmur_seek_to_point(mmr, arch, timestamp, &interval_start);
	if (record_start == -1) {
		M_PERROR("In propogation: could not seek to offset");
		goto error;
	}
	
	// uint64_t interval_end = interval_start + lower->seconds_per_point;
	uint64_t archive_end = arch->offset + arch->size;
	
	// If the points wrap back to the start of the archive, we can only read a
	// few before having to go back to the beginning. More file seeking!
	if (record_start + sizeof(points) > archive_end) {
		uint64_t to_read = archive_end - record_start;
		
		if (read(mmr->fd, points, to_read) != to_read) {
			M_PERROR("In propogation: could not read points");
			goto error;
		}
		
		if (lseek(mmr->fd, arch->offset, SEEK_SET) == -1) {
			M_PERROR("In propogation: could not seek to start of archive");
			goto error;
		}
		
		uint64_t points_read = to_read / sizeof(*points);
		to_read = sizeof(points) - to_read;
		
		if (read(mmr->fd, points + points_read, to_read) != to_read) {
			M_PERROR("In propogation: could not read wrapped points");
			goto error;
		}
	} else {
		if (read(mmr->fd, points, sizeof(points)) != sizeof(points)) {
			M_PERROR("In propogation: could not read points");
		}
	}
	
	double val = _murmur_aggregate(mmr->aggregation, sizeof(points)/sizeof(*points), points);
	
	if (_murmur_arch_set(mmr, arch->lower, timestamp, val) != 0) {
		goto error;
	}
	
	return 0;

error:
	M_ERROR("Propogation failing. This is really bad. Your archive will probably be inconsistent.");
	return -1;
}


int murmur_create(const char *path, const uint specc, char **specv, const enum aggregation_method aggregation, const char x_files_factor) {
	int fd = open(path, O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR|S_IRGRP);
	if (fd == -1) {
		M_PERROR("Could not open file for writing");
		return -1;
	}
	
	struct archive_header *arch_headers;
	uint32_t archive_count = _murmur_parse_archive_spec(specc, specv, &arch_headers);
	
	if (!archive_count || _murmur_validate_archives(archive_count, arch_headers) == -1) {
		return -1;
	}
	
	uint64_t max_retention = 0;
	uint32_t offset = sizeof(struct murmur_header) + (archive_count * sizeof(*arch_headers));
	
	for (uint32_t i = 0; i < archive_count; i++) {
		struct archive_header *ah = &arch_headers[i];
		
		uint64_t retention = ah->seconds_per_point * ah->points;
		if (retention > max_retention) {
			max_retention = retention;
		}
		
		// Prepare the struct for writing to disk
		ah->offset = htobe32(offset);
		
		// Can't work with a number in the wrong endianess
		offset += ah->points * sizeof(struct point);
		
		ah->seconds_per_point = htobe32(ah->seconds_per_point);
		ah->points = htobe32(ah->points);
	}
	
	struct murmur_header header = {
		.aggregation = (aggregation == 0 ? average : aggregation),
		.max_retention = htobe64(max_retention),
		.x_files_factor = x_files_factor,
		.archive_count = htobe32(archive_count),
	};
	
	int ret = 0;
	
	int len = sizeof(header);
	int curr_pos = len;
	if (write(fd, &header, len) != len) {
		M_PERROR("Could not write murmur header");
		ret = -1;
		goto done;
	}
	
	len = archive_count * sizeof(*arch_headers);
	curr_pos += len;
	if (write(fd, arch_headers, len) != len) {
		M_PERROR("Could not write archive headers");
		ret = -1;
		goto done;
	}
	
	if (fallocate(fd, 0, curr_pos, offset - curr_pos) != 0) {
		M_PERROR("Could not allocate archive area");
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
		M_PERROR("Could not open murmur file");
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
	
	mmr->aggregation = h.aggregation;
	mmr->max_retention = be64toh(h.max_retention);
	mmr->x_files_factor = h.x_files_factor;
	mmr->archive_count = be32toh(h.archive_count);
	
	if (mmr->archive_count == 0) {
		M_ERROR("Murmur file corrupted: no archives specified");
		goto error;
	}
	
	mmr->archives = malloc(mmr->archive_count * sizeof(*mmr->archives));
	struct murmur_archive *prev_archive = NULL;
	for (uint32_t i = 0; i < mmr->archive_count; i++) {
		struct murmur_archive *arch = mmr->archives + i;
		struct archive_header ah;
		
		if (read(fd, &ah, sizeof(ah)) != sizeof(ah)) {
			M_ERROR("Could not read archive header: file is corrupted");
			goto error;
		}
		
		arch->offset = be32toh(ah.offset);
		arch->seconds_per_point = be32toh(ah.seconds_per_point);
		arch->points = be32toh(ah.points);
		arch->retention = arch->seconds_per_point * arch->points;
		arch->size = arch->points * sizeof(struct point);
		arch->lower = NULL;
		
		if (prev_archive != NULL) {
			prev_archive->lower = arch;
		}
		prev_archive = arch;
		
		M_DEBUG("Archive header: offset=%u, spp=%u, points=%u",
			arch->offset,
			arch->seconds_per_point,
			arch->points
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

int murmur_set(struct murmur *mmr, const uint64_t timestamp, const double value) {
	struct murmur_archive *arch;
	if (_murmur_get_archive(mmr, timestamp, &arch) != 0) {
		M_ERROR("Could not locate suitable archive for item at timestamp: %ld", timestamp);
		return -1;
	}
	
	return _murmur_arch_set(mmr, arch, timestamp, value);
}

int murmur_get(struct murmur *mmr, const uint64_t timestamp, double * const value) {
	struct murmur_archive *arch;
	if (_murmur_get_archive(mmr, timestamp, &arch) != 0) {
		M_ERROR("Could not locate suitable archive for item at timestamp: %ld", timestamp);
		return -1;
	}
	
	uint64_t interval = 0;
	if (_murmur_seek_to_point(mmr, arch, timestamp, &interval) == -1) {
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

int murmur_dump_info(struct murmur *mmr) {
	static const char * const AGGREGATION_NAMES[] = {
		"average",
		"sum",
		"last",
		"max",
		"min",
	};
	
	M_INFO("Max data age: %lu seconds", mmr->max_retention);
	M_INFO("Accumulation factor: %d", mmr->x_files_factor);
	M_INFO("Aggregation method: %s", AGGREGATION_NAMES[mmr->aggregation-1]);
	
	M_INFO("Number of archives: %u", mmr->archive_count);
	M_INFO("");
	
	for (uint32_t i = 0; i < mmr->archive_count; i++) {
		struct murmur_archive *arch = mmr->archives + i;
		
		M_INFO("Archive %u:", i);
		M_INFO("  Seconds per point: %u", arch->seconds_per_point);
		M_INFO("  Points: %u", arch->points);
		M_INFO("");
	}
	
	return 0;
}

int murmur_dump(struct murmur *mmr) {
	if (murmur_dump_info(mmr) != 0) {
		return -1;
	}
	
	struct point p;
	while (read(mmr->fd, &p, sizeof(p)) != 0) {
		M_INFO("%lu = %lu", be64toh(p.interval), be64toh(p.value));
	}
	
	return 0;
}