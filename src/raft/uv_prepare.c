#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "heap.h"
#include "uv.h"
#include "uv_os.h"

/* The happy path for UvPrepare is:
 *
 * - If there is an unused open segment available, return its fd and counter
 *   immediately.
 *
 * - Otherwise, wait for the creation of a new open segment to complete,
 *   possibly kicking off the creation logic if no segment is being created
 *   currently.
 *
 * Possible failure modes are:
 *
 * - The create file request fails, in that case we fail all pending prepare
 *   requests and we mark the uv instance as errored.
 *
 * On close:
 *
 * - Cancel all pending prepare requests.
 * - Remove unused prepared open segments.
 * - Wait for any pending internal segment creation and then discard the newly
 *   created segment.
 */

/* Number of open segments that we try to keep ready for writing. */
#define UV__TARGET_POOL_SIZE 2

static void uvPrepareWorkCb(uv_work_t *work)
{
	struct ruv_segment *segment = work->data;
	struct uv *uv = segment->uv;
	int rv;

	rv = UvFsAllocateFile(uv->dir, segment->idle_filename, segment->idle_size,
			      &segment->idle_fd, uv->fallocate, segment->idle_errmsg);
	if (rv != 0) {
		goto err;
	}

	rv = UvFsSyncDir(uv->dir, segment->idle_errmsg);
	if (rv != 0) {
		goto err_after_allocate;
	}

	segment->idle_status = 0;
	return;

err_after_allocate:
	UvOsClose(segment->idle_fd);
err:
	assert(rv != 0);
	segment->idle_status = rv;
	return;
}

/* Flush all pending requests, invoking their callbacks with the given
 * status. */
static void uvPrepareFinishAllRequests(struct uv *uv, int status)
{
	while (!queue_empty(&uv->prepare_reqs)) {
		queue *head;
		struct uvPrepare *req;
		head = queue_head(&uv->prepare_reqs);
		req = QUEUE_DATA(head, struct uvPrepare, queue);
		queue_remove(&req->queue);
		req->cb(req, status);
	}
}

/* Pop the oldest prepared segment in the pool and return its fd and counter
 * through the given pointers. */
static void uvPrepareConsume(struct uv *uv, uv_file *fd, uvCounter *counter, struct ruv_segment **segment_out)
{
	/* Pop a segment from the pool. */
	queue *head = queue_head(&uv->prepare_pool);
	struct ruv_segment *segment = QUEUE_DATA(head, struct ruv_segment, idle_link);
	assert(segment->idle_fd >= 0);
	queue_remove(&segment->idle_link);
	*fd = segment->idle_fd;
	*counter = segment->idle_counter;
	*segment_out = segment;
}

/* Finish the oldest pending prepare request using the next available prepared
 * segment. */
static void uvPrepareFinishOldestRequest(struct uv *uv)
{
	queue *head;
	struct uvPrepare *req;

	assert(!uv->closing);
	assert(!queue_empty(&uv->prepare_reqs));
	assert(!queue_empty(&uv->prepare_pool));

	/* Pop the head of the prepare requests queue. */
	head = queue_head(&uv->prepare_reqs);
	req = QUEUE_DATA(head, struct uvPrepare, queue);
	queue_remove(&req->queue);

	/* Finish the request */
	uvPrepareConsume(uv, &req->fd, &req->counter, &req->segment);
	req->cb(req, 0);
}

/* Return the number of ready prepared open segments in the pool. */
static unsigned uvPrepareCount(struct uv *uv)
{
	queue *head;
	unsigned n;
	n = 0;
	QUEUE_FOREACH(head, &uv->prepare_pool)
	{
		n++;
	}
	return n;
}

static const struct sm_conf seg_states[SM_STATES_MAX] = {
	[SEG_INIT] = {
		.flags = SM_INITIAL|SM_FINAL,
		.allowed = BITS(SEG_PREPARED),
		.name = "init"
	},
	[SEG_PREPARED] = {
		.flags = SM_FINAL,
		.allowed = BITS(SEG_WRITTEN),
		.name = "prepared"
	},
	[SEG_WRITTEN] = {
		.flags = SM_FINAL,
		.allowed = BITS(SEG_WRITTEN),
		.name = "written"
	}
};

static bool seg_invariant(const struct sm *sm, int prev)
{
	/* TODO */
	(void)sm;
	(void)prev;
	return true;
}

static void uvPrepareAfterWorkCb(uv_work_t *work, int status);

/* Start creating a new segment file. */
static int uvPrepareStart(struct uv *uv)
{
	int rv;

	assert(uv->prepare_inflight == NULL);
	assert(uvPrepareCount(uv) < UV__TARGET_POOL_SIZE);

	struct ruv_segment *segment = RaftHeapMalloc(sizeof *segment);
	if (segment == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	memset(segment, 0, sizeof *segment);
	segment->uv = uv;
	segment->idle_counter = uv->prepare_next_counter;
	segment->idle_work.data = segment;
	segment->idle_fd = -1;
	segment->idle_size = uv->block_size * uvSegmentBlocks(uv);
	sprintf(segment->idle_filename, UV__OPEN_TEMPLATE, segment->idle_counter);
	sm_init(&segment->seg_sm, seg_invariant, NULL, seg_states, SEG_INIT);

	tracef("create open segment %s", segment->idle_filename);
	rv = uv_queue_work(uv->loop, &segment->idle_work, uvPrepareWorkCb,
			   uvPrepareAfterWorkCb);
	if (rv != 0) {
		/* UNTESTED: with the current libuv implementation this can't
		 * fail. */
		tracef("can't create segment %s: %s", segment->idle_filename,
		       uv_strerror(rv));
		rv = RAFT_IOERR;
		goto err_after_segment_alloc;
	}

	uv->prepare_inflight = segment;
	uv->prepare_next_counter++;

	return 0;

err_after_segment_alloc:
	RaftHeapFree(segment);
err:
	assert(rv != 0);
	return rv;
}

static void uvPrepareAfterWorkCb(uv_work_t *work, int status)
{
	struct ruv_segment *segment = work->data;
	struct uv *uv = segment->uv;
	int rv;
	assert(status == 0);

	uv->prepare_inflight =
	    NULL; /* Reset the creation in-progress marker. */

	/* If we are closing, let's discard the segment. All pending requests
	 * have already being fired with RAFT_CANCELED. */
	if (uv->closing) {
		assert(queue_empty(&uv->prepare_pool));
		assert(queue_empty(&uv->prepare_reqs));
		if (segment->idle_status == 0) {
			char errmsg[RAFT_ERRMSG_BUF_SIZE];
			UvOsClose(segment->idle_fd);
			UvFsRemoveFile(uv->dir, segment->idle_filename, errmsg);
		}
		tracef("canceled creation of %s", segment->idle_filename);
		RaftHeapFree(segment);
		uvMaybeFireCloseCb(uv);
		return;
	}

	/* If the request has failed, mark all pending requests as failed and
	 * don't try to create any further segment.
	 *
	 * Note that if there's no pending request, we don't set the error
	 * message, to avoid overwriting previous errors. */
	if (segment->idle_status != 0) {
		if (!queue_empty(&uv->prepare_reqs)) {
			ErrMsgTransferf(segment->idle_errmsg, uv->io->errmsg,
					"create segment %s", segment->idle_filename);
			uvPrepareFinishAllRequests(uv, segment->idle_status);
		}
		uv->errored = true;
		RaftHeapFree(segment);
		return;
	}

	assert(segment->idle_fd >= 0);

	tracef("completed creation of %s", segment->idle_filename);
	queue_insert_tail(&uv->prepare_pool, &segment->idle_link);

	/* Let's process any pending request. */
	if (!queue_empty(&uv->prepare_reqs)) {
		uvPrepareFinishOldestRequest(uv);
	}

	/* If we are already creating a segment, we're done. */
	if (uv->prepare_inflight != NULL) {
		return;
	}

	/* If we have already enough prepared open segments, we're done. There
	 * can't be any outstanding prepare requests, since if the request queue
	 * was not empty, we would have called uvPrepareFinishOldestRequest()
	 * above, thus reducing the pool size and making it smaller than the
	 * target size. */
	if (uvPrepareCount(uv) >= UV__TARGET_POOL_SIZE) {
		assert(queue_empty(&uv->prepare_reqs));
		return;
	}

	/* Let's start preparing a new open segment. */
	rv = uvPrepareStart(uv);
	if (rv != 0) {
		uvPrepareFinishAllRequests(uv, rv);
		uv->errored = true;
	}
}

/* Discard a prepared open segment, closing its file descriptor and removing the
 * underlying file. */
static void uvPrepareDiscard(struct uv *uv, uv_file fd, uvCounter counter, struct ruv_segment *segment)
{
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	char filename[UV__FILENAME_LEN];
	assert(counter > 0);
	assert(fd >= 0);
	sprintf(filename, UV__OPEN_TEMPLATE, counter);
	UvOsClose(fd);
	UvFsRemoveFile(uv->dir, filename, errmsg);
	RaftHeapFree(segment);
}

int UvPrepare(struct uv *uv,
	      struct uvPrepare *req,
	      uvPrepareCb cb)
{
	uv_file fd = -1;
	uvCounter counter = 0;
	struct ruv_segment *segment = NULL;
	int rv;

	assert(!uv->closing);

	if (!queue_empty(&uv->prepare_pool)) {
		uvPrepareConsume(uv, &fd, &counter, &segment);
		req->fd = fd;
		req->counter = counter;
		req->segment = segment;
		goto maybe_start;
	}

	req->cb = cb;
	queue_insert_tail(&uv->prepare_reqs, &req->queue);

maybe_start:
	/* If we are already creating a segment, let's just wait. */
	if (uv->prepare_inflight != NULL) {
		return 0;
	}

	rv = uvPrepareStart(uv);
	if (rv != 0) {
		goto err;
	}

	if (fd != -1) {
		req->cb(req, 0);
	}
	return 0;

err:
	if (fd != -1) {
		uvPrepareDiscard(uv, fd, counter, segment);
	} else {
		queue_remove(&req->queue);
	}
	assert(rv != 0);
	return rv;
}

void UvPrepareClose(struct uv *uv)
{
	assert(uv->closing);

	/* Cancel all pending prepare requests. */
	uvPrepareFinishAllRequests(uv, RAFT_CANCELED);

	/* Remove any unused prepared segment. */
	while (!queue_empty(&uv->prepare_pool)) {
		queue *head = queue_head(&uv->prepare_pool);
		struct ruv_segment *segment = QUEUE_DATA(head, struct ruv_segment, idle_link);
		queue_remove(&segment->idle_link);
		uvPrepareDiscard(uv, segment->idle_fd, segment->idle_counter, segment);
		RaftHeapFree(segment);
	}
}

