/**
 * Murmur: a murmur-like, time-based data store written in C.
 * @file murmur.h
 */

#include <stdint.h>
#include <stdio.h>

#if defined(COMPILE_DEBUG)
	#define M_DEBUG(format, ...) fprintf(stderr, "DEBUG : %s:%-4d : " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
	/**
	 * Output debug info to the console, only when not built for prod.
	 */
	#define M_DEBUG(format, ...)
#endif

/**
 * General useful information
 */
#define M_INFO(format, ...) fprintf(stderr, "INFO : %s:%-4d : " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)

/**
 * Output warning
 */
#define M_WARN(format, ...) fprintf(stderr, "WARN : %s:%-4d : " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)

/**
 * Output an error we can't recover from
 */
#define M_ERROR(format, ...) fprintf(stderr, "ERROR : %s:%-4d : " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)

/**
 * Output error information from the OS
 */
#define M_PERROR(format, ...) fprintf(stderr, "ERROR : %s:%-4d : " format ": %s\n", __FILE__, __LINE__, ##__VA_ARGS__, strerror(errno))

/**
 * How a murmur file should be aggregated.
 */
enum aggregation_method {
	average = 1,
	sum = 2,
	last = 3,
	max = 4,
	min = 5,
};

/**
 * Information about the archive in the murmur file.
 */
struct archive {
	/** 
	 * Where the archive begins in the file.
	 */
	uint32_t offset;
	
	/**
	 * The amount of time per point.
	 */
	uint32_t seconds_per_point;
	
	/**
	 * The number of points in the archive.
	 */
	uint32_t points;
	
	/**
	 * The total number of seconds contained in the archive.
	 */
	uint32_t retention;
};

/**
 * Represents an entire murmur file.
 */
struct murmur {
	/**
	 * The opened murmur file.
	 */
	int fd;
	
	/**
	 * How to aggregate points in the file.
	 */
	enum aggregation_method aggregation;
	
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
	 * The number of archives in the file.
	 */
	uint32_t archive_count;
	
	/**
	 * An array of archives.
	 */
	struct archive *archives;
};

/**
 * Creates a new murmur archive.
 *
 * @warning This function is destructive: if the given path exists, it will be overriden.
 *
 * @param path The path where the archive should be created
 * @param archives A string specification for how to create the archives.
 * @param aggregation How stats should be aggregated together.
 * @param x_files_factor The fraction of data points (0-100) in a propagation
 * interval that must have known values for a propagation to occur.
 *
 * @return 0 on success
 * @return -1 on failure
 */
int murmur_create(const char *path, const char *archive_spec, const enum aggregation_method aggregation, const char x_files_factor);

/**
 * Opens a murmur file and prepares it for manipulation.
 *
 * @param path Path to the murmur file to open.
 *
 * @return The murmur file, NULL on failure.
 */
struct murmur* murmur_open(const char *path);

/**
 * Closes a murmur file and frees all information.
 *
 * @param mmr The murmur file to close.
 */
void murmur_close(struct murmur *mmr);

/**
 * Gets a point from the file.
 *
 * @param mmr The mumur file.
 * @param timestamp The timestamp to lookup the value at.
 *
 * @return 0 on success, -1 on failure
 */
int murmur_get(struct murmur *mmr, const uint64_t timestamp, double * const value);

/**
 * Updates a point in the file.
 *
 * @param mmr The mumur file.
 * @param value The value to write
 * @param timestamp The timestamp for the value
 *
 * @return 0 on success, -1 on failure.
 */
int murmur_set(struct murmur *mmr, const uint64_t timestamp, const double value);