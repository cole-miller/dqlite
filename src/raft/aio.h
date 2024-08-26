#ifndef RAFT_AIO_H_
#define RAFT_AIO_H_

#include <linux/aio_abi.h>
#include <sys/types.h> /* off_t */
#include <time.h> /* struct timespec */

typedef struct raft_aio_context_s raft_aio_context;

int UvOsIoSetup(unsigned nr, raft_aio_context **out);
int UvOsIoDestroy(raft_aio_context *ctx);
int raft_aio_pwrite(raft_aio_context *ctx, int fd,
		    void *buf, size_t len, off_t off,
		    int rw_flags, int resfd, void *data);
int UvOsIoGetevents(raft_aio_context *ctx,
		    long max_nr,
		    struct io_event *events);

#endif /* RAFT_AIO_H_ */
