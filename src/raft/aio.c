#include <errno.h>
#include <sys/syscall.h> /* _NR_* */
#include <unistd.h> /* syscall */

#include "../raft.h" /* raft_malloc, raft_free */
#include "aio.h"
#include "assert.h"

struct raft_aio_context_s {
	aio_context_t inner;
};

int UvOsIoSetup(unsigned nr, raft_aio_context **out)
{
	struct raft_aio_context_s *ctx;
	int rv;

	ctx = raft_malloc(sizeof(*ctx));
	if (ctx == NULL) {
		return -ENOMEM;
	}
	/* io_setup returns EINVAL if the aio_context_t is not
	 * zero-initialized. */
	ctx->inner = 0;

	rv = (int)syscall(__NR_io_setup, nr, &ctx->inner);
	if (rv == -1) {
		raft_free(ctx);
		return -errno;
	}
	*out = ctx;
	return 0;
}

int UvOsIoDestroy(raft_aio_context *ctx)
{
	int rv;
	rv = (int)syscall(__NR_io_destroy, ctx->inner);
	raft_free(ctx);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}

int UvOsIoSubmit(raft_aio_context *ctx, long nr, struct iocb **iocbpp)
{
	int rv;
	rv = (int)syscall(__NR_io_submit, ctx->inner, nr, iocbpp);
	if (rv == -1) {
		return -errno;
	}
	assert(rv == nr); /* TODO: can something else be returned? */
	return 0;
}

int UvOsIoGetevents(raft_aio_context *ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout)
{
	int rv;
	do {
		rv = (int)syscall(__NR_io_getevents, ctx->inner,
				  min_nr, max_nr, events, timeout);
	} while (rv == -1 && errno == EINTR);

	if (rv == -1) {
		return -errno;
	}
	assert(rv >= min_nr);
	assert(rv <= max_nr);
	return rv;
}
