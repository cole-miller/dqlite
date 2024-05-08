#include "assert.h"
#include "heap.h"
#include "../lib/queue.h"
#include "uv.h"
#include "uv_os.h"

/* Run all blocking syscalls involved in closing a used open segment.
 *
 * An open segment is closed by truncating its length to the number of bytes
 * that were actually written into it and then renaming it. */
static void uvFinalizeWorkCb(uv_work_t *work)
{
	struct ruv_segment *segment = work->data;
	struct uv *uv = segment->uv;
	char filename1[UV__FILENAME_LEN];
	char filename2[UV__FILENAME_LEN];
	char errmsg[RAFT_ERRMSG_BUF_SIZE];
	int rv;

	sprintf(filename1, UV__OPEN_TEMPLATE, segment->dying_counter);
	sprintf(filename2, UV__CLOSED_TEMPLATE, segment->dying_first_index,
		segment->dying_last_index);

	ruv_record_event(uv, (struct ruv_segment_event){
		.type = RUV_EV_FINALIZE,
		.finalize = {
			.counter = segment->dying_counter,
			.first_index = segment->dying_first_index,
			.end_index = segment->dying_last_index
		}
	});
	tracef("RECORD FINALIZE ino=%lu counter=%llu first_index=%llu end_index=%llu",
			ruv_open_segment_ino(uv, segment->dying_counter),
			segment->dying_counter,
			segment->dying_first_index,
			segment->dying_last_index);

	/* If the segment hasn't actually been used (because the writer has been
	 * closed or aborted before making any write), just remove it. */
	if (segment->dying_used == 0) {
		tracef("remove unused segment file: %s", filename1);
		rv = UvFsRemoveFile(uv->dir, filename1, errmsg);
		if (rv != 0) {
			goto err;
		}
		goto sync;
	}

	/* Truncate and rename the segment.*/
	rv = UvFsTruncateAndRenameFile(uv->dir, segment->dying_used, filename1,
				       filename2, errmsg);
	if (rv != 0) {
		goto err;
	}

sync:
	rv = UvFsSyncDir(uv->dir, errmsg);
	if (rv != 0) {
		goto err;
	}

	segment->dying_status = 0;
	return;

err:
	tracef("truncate segment %s: %s", filename1, errmsg);
	assert(rv != 0);
	segment->dying_status = rv;
}

static int uvFinalizeStart(struct ruv_segment *segment);
static void uvFinalizeAfterWorkCb(uv_work_t *work, int status)
{
	struct ruv_segment *segment = work->data;
	struct uv *uv = segment->uv;
	tracef("uv finalize after work segment %p cb status:%d",
	       (void *)segment, status);
	queue *head;
	int rv;

	assert(status == 0); /* We don't cancel worker requests */
	uv->finalize_work.data = NULL;
	if (segment->dying_status != 0) {
		uv->errored = true;
	}
	RaftHeapFree(segment);

	assert(segment->dying_first_index = uv->last_closed_end_index + 1);
	uv->last_closed_end_index = segment->dying_last_index;

	/* If we have no more dismissed segments to close, check if there's a
	 * barrier to unblock or if we are done closing. */
	if (queue_empty(&uv->finalize_reqs)) {
		tracef("unblock barrier or close");
		if (uv->barrier != NULL && UvBarrierReady(uv)) {
			UvBarrierMaybeTrigger(uv->barrier);
		}
		uvMaybeFireCloseCb(uv);
		return;
	}

	/* Grab a new dismissed segment to close. */
	head = queue_head(&uv->finalize_reqs);
	segment = QUEUE_DATA(head, struct ruv_segment, dying_link);
	queue_remove(&segment->dying_link);

	rv = uvFinalizeStart(segment);
	if (rv != 0) {
		RaftHeapFree(segment);
		uv->errored = true;
	}
}

/* Start finalizing an open segment. */
static int uvFinalizeStart(struct ruv_segment *segment)
{
	struct uv *uv = segment->uv;
	int rv;

	assert(uv->finalize_work.data == NULL);
	assert(segment->dying_counter > 0);

	uv->finalize_work.data = segment;

	rv = uv_queue_work(uv->loop, &uv->finalize_work, uvFinalizeWorkCb,
			   uvFinalizeAfterWorkCb);
	if (rv != 0) {
		ErrMsgPrintf(uv->io->errmsg,
			     "start to truncate segment file %llu: %s",
			     segment->dying_counter, uv_strerror(rv));
		return RAFT_IOERR;
	}

	return 0;
}

int UvFinalize(struct ruv_segment *segment)
{
	struct uv *uv = segment->uv;
	uvCounter counter = segment->alive_counter;
	size_t used = segment->alive_written;
	raft_index first_index = segment->alive_first_index;
	raft_index last_index = segment->alive_last_index;
	int rv;

	if (used > 0) {
		assert(first_index > 0);
		assert(last_index >= first_index);
	}

	segment->dying_counter = counter;
	segment->dying_used = used;
	segment->dying_first_index = first_index;
	segment->dying_last_index = last_index;

	/* If we're already processing a segment, let's put the request in the
	 * queue and wait. */
	if (uv->finalize_work.data != NULL) {
		queue_insert_tail(&uv->finalize_reqs, &segment->dying_link);
		return 0;
	}

	rv = uvFinalizeStart(segment);
	if (rv != 0) {
		RaftHeapFree(segment);
		return rv;
	}

	return 0;
}

