#include <errno.h>
#include <linux/aio_abi.h>
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
		    int flags, int resfd, void *data)
{
	struct iocb *iocb;
	int rw_flags;
	int rv;

	rw_flags = 0;
	if (flags & RAFT_AIO_NOWAIT) {
		rw_flags |= RWF_NOWAIT;
	}
	if (flags & RAFT_AIO_DSYNC) {
		rw_flags |= RWF_DSYNC;
	}

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
		    long max_nr,
		    struct raft_aio_event *events)
{
	struct io_event *kevents;
	int rv;

	kevents = raft_calloc((size_t)max_nr, sizeof(*kevents));
	if (kevents == NULL) {
		return -ENOMEM;
	}

	do {
		rv = (int)syscall(__NR_io_getevents, ctx->inner,
				  1, max_nr, kevents, NULL);
	} while (rv == -1 && errno == EINTR);

	if (rv == -1) {
		raft_free(kevents);
		return -errno;
	}
	assert(rv >= 1);
	assert(rv <= max_nr);

	for (int i = 0; i < rv; i++) {
		events[i].data = (void *)kevents[i].data;
		raft_free((struct iocb *)kevents[i].obj);
		events[i].res = (int)kevents[i].res;
	}
	raft_free(kevents);
	return rv;
}
