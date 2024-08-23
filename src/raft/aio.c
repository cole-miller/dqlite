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

int raft_aio_pwrite(raft_aio_context *ctx, int fd,
		    void *buf, size_t len, off_t off,
		    int rw_flags, int resfd, void *data)
{
	struct iocb *iocb;
	int rv;

	iocb = raft_calloc(1, sizeof(*iocb));
	if (iocb == NULL) {
		return -ENOMEM;
	}
	iocb->aio_lio_opcode = IOCB_CMD_PWRITE;
	iocb->aio_fildes = (uint32_t)fd;
	iocb->aio_buf = (uint64_t)buf;
	iocb->aio_nbytes = len;
	iocb->aio_offset = off;
	iocb->aio_rw_flags = rw_flags;
	iocb->aio_flags = resfd == -1 ? 0 : IOCB_FLAG_RESFD;
	iocb->aio_resfd = (uint32_t)resfd;
	iocb->aio_data = (uint64_t)data;
	iocb->aio_reqprio = 0;

	rv = (int)syscall(__NR_io_submit, ctx->inner, 1, &iocb);
	if (rv == -1) {
		raft_free(iocb);
		return -errno;
	}
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
