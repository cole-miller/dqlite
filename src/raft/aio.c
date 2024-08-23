#include "aio.h"
#include "syscall.h"

int UvOsIoSetup(unsigned nr, aio_context_t *ctxp)
{
	int rv;
	rv = io_setup(nr, ctxp);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}

int UvOsIoDestroy(aio_context_t ctx)
{
	int rv;
	rv = io_destroy(ctx);
	if (rv == -1) {
		return -errno;
	}
	return 0;
}

int UvOsIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp)
{
	int rv;
	rv = io_submit(ctx, nr, iocbpp);
	if (rv == -1) {
		return -errno;
	}
	assert(rv == nr); /* TODO: can something else be returned? */
	return 0;
}

int UvOsIoGetevents(aio_context_t ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout)
{
	int rv;
	do {
		rv = io_getevents(ctx, min_nr, max_nr, events, timeout);
	} while (rv == -1 && errno == EINTR);

	if (rv == -1) {
		return -errno;
	}
	assert(rv >= min_nr);
	assert(rv <= max_nr);
	return rv;
}
