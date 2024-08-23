#include "syscall.h"

#if HAVE_LINUX_AIO_ABI_H
#include <sys/syscall.h>
#include <unistd.h>

int io_setup(unsigned nr_events, aio_context_t *ctx_idp)
{
	return (int)syscall(__NR_io_setup, nr_events, ctx_idp);
}

int io_destroy(aio_context_t ctx_id)
{
	return (int)syscall(__NR_io_destroy, ctx_id);
}

int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp)
{
	return (int)syscall(__NR_io_submit, ctx_id, nr, iocbpp);
}

int io_getevents(aio_context_t ctx_id,
		 long min_nr,
		 long nr,
		 struct io_event *events,
		 struct timespec *timeout)
{
	return (int)syscall(__NR_io_getevents, ctx_id, min_nr, nr, events,
			    timeout);
}
#endif
