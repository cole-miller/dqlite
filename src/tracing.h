/* Tracing functionality for dqlite */

#ifndef DQLITE_TRACING_H_
#define DQLITE_TRACING_H_

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>

#include "../include/dqlite.h"

#include "utils.h"

/* This global variable is only written once at startup and is only read
 * from there on. Users should not manipulate the value of this variable. */
DQLITE_VISIBLE_TO_TESTS extern bool _dqliteTracingEnabled;

#define tracef(...)                                                         \
	do {                                                                \
		if (UNLIKELY(_dqliteTracingEnabled)) {                      \
			static char _msg[1024];                             \
			snprintf(_msg, sizeof(_msg), __VA_ARGS__);          \
			time_t thetime = time(NULL); \
			char *ts = ctime(&thetime); \
			fprintf(stderr, "LIBDQLITE %s %s:%d %s\n", \
				ts, __func__, __LINE__, _msg);              \
		}                                                           \
	} while (0)

/* Enable tracing if the appropriate env variable is set, or disable tracing. */
DQLITE_VISIBLE_TO_TESTS void dqliteTracingMaybeEnable(bool enabled);

#endif /* DQLITE_TRACING_H_ */
