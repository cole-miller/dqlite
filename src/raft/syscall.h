/* Wrappers for system calls not yet defined in libc. */

#ifndef SYSCALL_H_
#define SYSCALL_H_

#if HAVE_LINUX_AIO_ABI_H
#include <linux/aio_abi.h>
#include <signal.h>
#include <time.h>

int io_setup(unsigned nr_events, aio_context_t *ctx_idp);

int io_destroy(aio_context_t ctx_id);

int io_submit(aio_context_t ctx_id, long nr, struct iocb **iocbpp);

int io_getevents(aio_context_t ctx_id,
		 long min_nr,
		 long nr,
		 struct io_event *events,
		 struct timespec *timeout);
#endif

#endif /* SYSCALL_ */
