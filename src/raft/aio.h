#ifndef RAFT_AIO_H_
#define RAFT_AIO_H_

#include <linux/aio_abi.h>
#include <time.h>

int UvOsIoSetup(unsigned nr, aio_context_t *ctxp);
int UvOsIoDestroy(aio_context_t ctx);
int UvOsIoSubmit(aio_context_t ctx, long nr, struct iocb **iocbpp);
int UvOsIoGetevents(aio_context_t ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout);

#endif /* RAFT_AIO_H_ */
