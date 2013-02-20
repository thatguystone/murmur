/**
 * We're checking the integrity of the file internally, so we need this.
 */
#include "libmurmur.c"

#define PATH "murmur_test.mmr"

/**
 * A test assertion
 */
#define TEST(expr) \
	total++; \
	if (!(expr)) { \
		failed++; \
		printf("Fail (line %d): %s\n", __LINE__, #expr); \
		return 1; \
	}

/**
 * Calculate the number of items in a vector
 */
#define NUM_ELEMS(what) (sizeof(what)/sizeof(*what))

/**
 * Counters for tests.
 */
static unsigned int total = 0;
static unsigned int failed = 0;
static unsigned int total_tests = 0;
static unsigned int failed_tests = 0;

/**
 * The signature for all test functions.
 *
 * @param mmr The instance of murmur to test against
 *
 * @return 0 on success
 * @return anything else on failure
 */
typedef int (*test_fn)();

static int test_sane() {
	char *spec[] = {
		"10s:1m",
		"1m:5m",
	};
	TEST(murmur_create(PATH, NUM_ELEMS(spec), spec, agg_average, 0) == 0);
	
	struct murmur *mmr = murmur_open(PATH);
	TEST(mmr != NULL);
	
	mmr_test_time = 1000;
	
	double val = 100;
	TEST(murmur_set(mmr, mmr_test_time, val) == 0);
	TEST(murmur_get(mmr, mmr_test_time, &val) == 0);
	TEST(val == 100);
	
	TEST(_murmur_arch_get(mmr, mmr->archives + 1, mmr_test_time, &val) == 0);
	TEST(val == (((double)100)/6));
	
	murmur_close(mmr);
	
	return 0;
}

static int test_high_precision_full() {
	char *spec[] = {
		"10s:1m",
		"1m:5m",
	};
	TEST(murmur_create(PATH, NUM_ELEMS(spec), spec, agg_average, 0) == 0);
	
	struct murmur *mmr = murmur_open(PATH);
	TEST(mmr != NULL);
	
	// So that the archive doesn't wrap and everything is
	// aggregated into 1 lower-precision point
	mmr_test_time = (mmr->archives->retention * 5) - 10;
	
	double val = 100;
	time_t at = mmr_test_time;
	for (int i = 0; i < 6; i++) {
		TEST(murmur_set(mmr, at, val) == 0);
		val += 100;
		at -= 10;
	}
	
	at = mmr_test_time;
	for (int i = 0; i < 6; i++) {
		TEST(murmur_get(mmr, at, &val) == 0);
		TEST(val == (100 + (i * 100)));
		at -= 10;
	}
	
	TEST(_murmur_arch_get(mmr, mmr->archives + 1, mmr_test_time, &val) == 0);
	TEST(val == (((double)100+200+300+400+500+600)/6));
	
	murmur_close(mmr);
	
	return 0;
}

static int test_full() {
	char *spec[] = {
		"10s:1m",
		"1m:5m",
	};
	TEST(murmur_create(PATH, NUM_ELEMS(spec), spec, agg_average, 0) == 0);
	
	struct murmur *mmr = murmur_open(PATH);
	TEST(mmr != NULL);
	
	mmr_test_time = 1000;
	
	double val = 100;
	time_t at = mmr_test_time;
	for (int i = 0; i < 6; i++) {
		TEST(murmur_set(mmr, at, val) == 0);
		val += 100;
		at -= 10;
	}
	
	// Start at 1: don't overwrite the propogated value from above
	// Go to 5: we only store 5 minutes of backlog
	for (int i = 1; i < 5; i++) {
		TEST(murmur_set(mmr, at, val) == 0);
		val += 100;
		at -= 60;
	}
	
	// This little bit of maths finds the highest timestamp that
	// will be contained in the archive given that we started at 1000.
	//
	// In reality, murmur won't let us go past the current timestamp, but
	// ignore that crap for testing purposes.
	//
	// Basically, the math reads:
	//   1) To find the start, we need to know where the intervals line up: 1000%60
	//   2) That's the distance we are from the start of this interval,
	//      but we need to get to the start of the NEXT interval, and were already ans
	//      seconds into this interval, so that start is: 60-ans
	//   3) Add our base time back in: 1000+ans
	//   4) That puts us at the start of the next interval, to get to the end of ours: ans-1
	at = (1000+(60-(1000%60)))-1;
	
	for (int i = 0; i < 5; i++) {
		M_INFO("------------");
		for (int j = 0; j < 6; j++) {
			TEST(_murmur_arch_get(mmr, mmr->archives + 1, at, &val) == 0);
			M_INFO("@%ld = %f", at, val);
			at -= 10;
		}
	}
	
	murmur_close(mmr);
	
	return 0;
}

static void test(test_fn fn) {
	total_tests++;
	
	failed_tests += fn() != 0;
}

int main(int argc, char **argv) {
	printf("Running tests...\n\n");
	
	test(test_sane);
	test(test_high_precision_full);
	test(test_full);
	
	printf("\nResults: %u/%u passing (%u/%u conditions passing)\n",
		total_tests - failed_tests,
		total_tests,
		total - failed,
		total
	);
	
	return failed_tests != 0;
}