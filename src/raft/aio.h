#ifndef RAFT_AIO_H_
#define RAFT_AIO_H_

#include <linux/aio_abi.h>
#include <time.h>

typedef struct raft_aio_context_s raft_aio_context;

int UvOsIoSetup(unsigned nr, raft_aio_context **out);
int UvOsIoDestroy(raft_aio_context *ctx);
int UvOsIoSubmit(raft_aio_context *ctx, long nr, struct iocb **iocbpp);
int UvOsIoGetevents(raft_aio_context *ctx,
		    long min_nr,
		    long max_nr,
		    struct io_event *events,
		    struct timespec *timeout);

#endif /* RAFT_AIO_H_ */
