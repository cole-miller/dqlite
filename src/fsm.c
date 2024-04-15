#include "lib/assert.h"
#include "lib/serialize.h"
#include "lib/threadpool.h"

#include "command.h"
#include "fsm.h"
#include "raft.h"
#include "tracing.h"
#include "vfs.h"
#include "vfs2.h"

#include <sys/mman.h>

struct fsm
{
	struct logger *logger;
	struct registry *registry;
	struct
	{
		unsigned n_pages;
		unsigned long *page_numbers;
		uint8_t *pages;
	} pending; /* For upgrades from V1 */
	pool_t *pool;
};

static int apply_open(struct fsm *f, const struct command_open *c)
{
	tracef("fsm apply open");
	(void)f;
	(void)c;
	return 0;
}

static int add_pending_pages(struct fsm *f,
			     unsigned long *page_numbers,
			     uint8_t *pages,
			     unsigned n_pages,
			     unsigned page_size)
{
	unsigned n = f->pending.n_pages + n_pages;
	unsigned i;

	f->pending.page_numbers = sqlite3_realloc64(
	    f->pending.page_numbers, n * sizeof *f->pending.page_numbers);

	if (f->pending.page_numbers == NULL) {
		return DQLITE_NOMEM;
	}

	f->pending.pages = sqlite3_realloc64(f->pending.pages, n * page_size);

	if (f->pending.pages == NULL) {
		return DQLITE_NOMEM;
	}

	for (i = 0; i < n_pages; i++) {
		unsigned j = f->pending.n_pages + i;
		f->pending.page_numbers[j] = page_numbers[i];
		memcpy(f->pending.pages + j * page_size,
		       (uint8_t *)pages + i * page_size, page_size);
	}
	f->pending.n_pages = n;

	return 0;
}

static int databaseReadLock(struct db *db)
{
	if (!db->read_lock) {
		db->read_lock = 1;
		return 0;
	} else {
		return -1;
	}
}

static int databaseReadUnlock(struct db *db)
{
	if (db->read_lock) {
		db->read_lock = 0;
		return 0;
	} else {
		return -1;
	}
}

static void maybeCheckpoint(struct db *db)
{
	tracef("maybe checkpoint");
	struct sqlite3_file *main_f;
	struct sqlite3_file *wal;
	volatile void *region;
	sqlite3_int64 size;
	unsigned page_size;
	unsigned pages;
	int wal_size;
	int ckpt;
	int i;
	int rv;

	/* Don't run when a snapshot is busy. Running a checkpoint while a
	 * snapshot is busy will result in illegal memory accesses by the
	 * routines that try to access database page pointers contained in the
	 * snapshot. */
	rv = databaseReadLock(db);
	if (rv != 0) {
		tracef("busy snapshot %d", rv);
		return;
	}

	assert(db->follower == NULL);
	rv = db__open_follower(db);
	if (rv != 0) {
		tracef("open follower failed %d", rv);
		goto err_after_db_lock;
	}

	page_size = db->config->page_size;
	/* Get the database wal file associated with this connection */
	rv = sqlite3_file_control(db->follower, "main",
				  SQLITE_FCNTL_JOURNAL_POINTER, &wal);
	assert(rv == SQLITE_OK); /* Should never fail */

	rv = wal->pMethods->xFileSize(wal, &size);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Calculate the number of frames. */
	pages = (unsigned)((size - 32) / (24 + page_size));

	/* Check if the size of the WAL is beyond the threshold. */
	if (pages < db->config->checkpoint_threshold) {
		tracef("wal size (%u) < threshold (%u)", pages,
		       db->config->checkpoint_threshold);
		goto err_after_db_open;
	}

	/* Get the database file associated with this db->follower connection */
	rv = sqlite3_file_control(db->follower, "main",
				  SQLITE_FCNTL_FILE_POINTER, &main_f);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Get the first SHM region, which contains the WAL header. */
	rv = main_f->pMethods->xShmMap(main_f, 0, 0, 0, &region);
	assert(rv == SQLITE_OK); /* Should never fail */

	rv = main_f->pMethods->xShmUnmap(main_f, 0);
	assert(rv == SQLITE_OK); /* Should never fail */

	/* Try to acquire all locks. */
	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		int flags = SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE;

		rv = main_f->pMethods->xShmLock(main_f, i, 1, flags);
		if (rv == SQLITE_BUSY) {
			tracef("busy reader or writer - retry next time");
			goto err_after_db_open;
		}

		/* Not locked. Let's release the lock we just
		 * acquired. */
		flags = SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE;
		main_f->pMethods->xShmLock(main_f, i, 1, flags);
	}

	rv = sqlite3_wal_checkpoint_v2(
	    db->follower, "main", SQLITE_CHECKPOINT_TRUNCATE, &wal_size, &ckpt);
	/* TODO assert(rv == 0) here? Which failure modes do we expect? */
	if (rv != 0) {
		tracef("sqlite3_wal_checkpoint_v2 failed %d", rv);
		goto err_after_db_open;
	}
	tracef("sqlite3_wal_checkpoint_v2 success");

	/* Since no reader transaction is in progress, we must be able to
	 * checkpoint the entire WAL */
	assert(wal_size == 0);
	assert(ckpt == 0);

err_after_db_open:
	sqlite3_close(db->follower);
	db->follower = NULL;
err_after_db_lock:
	rv = databaseReadUnlock(db);
	assert(rv == 0);
}

static int apply_frames(struct fsm *f, const struct command_frames *c, struct vfs2_wal_slice sl)
{
	tracef("fsm apply frames");
	/* XXX */
	(void)sl;
	struct db *db;
	sqlite3_vfs *vfs;
	unsigned long *page_numbers = NULL;
	void *pages;
	int exists;
	int rv;

	rv = registry__db_get(f->registry, c->filename, &db);
	if (rv != 0) {
		tracef("db get failed %d", rv);
		return rv;
	}

	vfs = sqlite3_vfs_find(db->config->name);

	/* Check if the database file exists, and create it by opening a
	 * connection if it doesn't. */
	rv = vfs->xAccess(vfs, db->path, 0, &exists);
	assert(rv == 0);

	if (!exists) {
		rv = db__open_follower(db);
		if (rv != 0) {
			tracef("open follower failed %d", rv);
			return rv;
		}
		sqlite3_close(db->follower);
		db->follower = NULL;
	}

	rv = command_frames__page_numbers(c, &page_numbers);
	if (rv != 0) {
		if (page_numbers != NULL) {
			sqlite3_free(page_numbers);
		}
		tracef("page numbers failed %d", rv);
		return rv;
	}

	command_frames__pages(c, &pages);

	/* If the commit marker is set, we apply the changes directly to the
	 * VFS. Otherwise, if the commit marker is not set, this must be an
	 * upgrade from V1, we accumulate uncommitted frames in memory until the
	 * final commit or a rollback. */
	if (c->is_commit) {
		if (f->pending.n_pages > 0) {
			rv = add_pending_pages(f, page_numbers, pages,
					       c->frames.n_pages,
					       db->config->page_size);
			if (rv != 0) {
				tracef("malloc");
				sqlite3_free(page_numbers);
				return DQLITE_NOMEM;
			}
			rv =
			    VfsApply(vfs, db->path, f->pending.n_pages,
				     f->pending.page_numbers, f->pending.pages);
			if (rv != 0) {
				tracef("VfsApply failed %d", rv);
				sqlite3_free(page_numbers);
				return rv;
			}
			sqlite3_free(f->pending.page_numbers);
			sqlite3_free(f->pending.pages);
			f->pending.n_pages = 0;
			f->pending.page_numbers = NULL;
			f->pending.pages = NULL;
		} else {
			rv = VfsApply(vfs, db->path, c->frames.n_pages,
				      page_numbers, pages);
			if (rv != 0) {
				tracef("VfsApply failed %d", rv);
				sqlite3_free(page_numbers);
				return rv;
			}
		}
	} else {
		rv =
		    add_pending_pages(f, page_numbers, pages, c->frames.n_pages,
				      db->config->page_size);
		if (rv != 0) {
			tracef("add pending pages failed %d", rv);
			sqlite3_free(page_numbers);
			return DQLITE_NOMEM;
		}
	}

	sqlite3_free(page_numbers);
	maybeCheckpoint(db);
	return 0;
}

static int apply_undo(struct fsm *f, const struct command_undo *c)
{
	tracef("apply undo %" PRIu64, c->tx_id);
	(void)c;

	if (f->pending.n_pages == 0) {
		return 0;
	}

	sqlite3_free(f->pending.page_numbers);
	sqlite3_free(f->pending.pages);
	f->pending.n_pages = 0;
	f->pending.page_numbers = NULL;
	f->pending.pages = NULL;

	return 0;
}

/* Checkpoints used to be coordinated cluster-wide, these days a node
 * checkpoints independently in `apply_frames`, the checkpoint command becomes a
 * no-op for modern nodes. */
static int apply_checkpoint(struct fsm *f, const struct command_checkpoint *c)
{
	(void)f;
	(void)c;
	tracef("apply no-op checkpoint");
	return 0;
}

struct commit_frames {
	pool_work_t work;
};

static void apply_frames_async(struct fsm *f,
			       struct command_frames *cmd,
			       struct vfs2_wal_slice sl,
			       struct raft_fsm_apply_async *raft_req,
			       raft_fsm_apply_cb cb)
{
	pool_queue_work(f->pool);
}

static void fsm_apply_async(struct raft_fsm *fsm,
			    struct raft_fsm_apply_async *req,
			    raft_fsm_apply_cb cb)
{
	struct fsm *f = fsm->data;
	int rv;

	int type;
	void *command;
	rv = command__decode(&req->buf, &type, &command);
	if (rv != 0) {
		tracef("fsm: decode command: %d", rv);
		goto resolve;
	}

	struct vfs2_wal_slice sl;
	switch (type) {
		case COMMAND_OPEN:
			rv = apply_open(f, command);
			break;
		case COMMAND_FRAMES:
			assert(req->local_buf.len == sizeof(sl));
			sl = *(struct vfs2_wal_slice *)(req->local_buf.base);
			apply_frames_async(f, command, sl, req, cb);
			return;
		case COMMAND_UNDO:
			rv = apply_undo(f, command);
			break;
		case COMMAND_CHECKPOINT:
			rv = apply_checkpoint(f, command);
			break;
		default:
			rv = RAFT_MALFORMED;
			break;
	}
	raft_free(command);

resolve:
	cb(req, NULL, rv);
}


#define SNAPSHOT_FORMAT 1

#define SNAPSHOT_HEADER(X, ...)          \
	X(uint64, format, ##__VA_ARGS__) \
	X(uint64, n, ##__VA_ARGS__)
SERIALIZE__DEFINE(snapshotHeader, SNAPSHOT_HEADER);
SERIALIZE__IMPLEMENT(snapshotHeader, SNAPSHOT_HEADER);

#define SNAPSHOT_DATABASE(X, ...)           \
	X(text, filename, ##__VA_ARGS__)    \
	X(uint64, main_size, ##__VA_ARGS__) \
	X(uint64, wal_size, ##__VA_ARGS__)
SERIALIZE__DEFINE(snapshotDatabase, SNAPSHOT_DATABASE);
SERIALIZE__IMPLEMENT(snapshotDatabase, SNAPSHOT_DATABASE);

/* Encode the global snapshot header. */
static int encodeSnapshotHeader(unsigned n, struct raft_buffer *buf)
{
	struct snapshotHeader header;
	char *cursor;
	header.format = SNAPSHOT_FORMAT;
	header.n = n;
	buf->len = snapshotHeader__sizeof(&header);
	buf->base = sqlite3_malloc64(buf->len);
	if (buf->base == NULL) {
		return RAFT_NOMEM;
	}
	cursor = buf->base;
	snapshotHeader__encode(&header, &cursor);
	return 0;
}

/* Encode the given database. */
static int encodeDatabase(struct db *db,
			  struct raft_buffer r_bufs[],
			  uint32_t n)
{
	struct snapshotDatabase header;
	sqlite3_vfs *vfs;
	uint32_t database_size = 0;
	uint8_t *page;
	char *cursor;
	struct dqlite_buffer *bufs = (struct dqlite_buffer *)r_bufs;
	int rv;

	header.filename = db->filename;

	vfs = sqlite3_vfs_find(db->config->name);
	rv = VfsShallowSnapshot(vfs, db->filename, &bufs[1], n - 1);
	if (rv != 0) {
		goto err;
	}

	/* Extract the database size from the first page. */
	page = bufs[1].base;
	database_size += (uint32_t)(page[28] << 24);
	database_size += (uint32_t)(page[29] << 16);
	database_size += (uint32_t)(page[30] << 8);
	database_size += (uint32_t)(page[31]);

	header.main_size =
	    (uint64_t)database_size * (uint64_t)db->config->page_size;
	header.wal_size = bufs[n - 1].len;

	/* Database header. */
	bufs[0].len = snapshotDatabase__sizeof(&header);
	bufs[0].base = sqlite3_malloc64(bufs[0].len);
	if (bufs[0].base == NULL) {
		rv = RAFT_NOMEM;
		goto err_after_snapshot;
	}
	cursor = bufs[0].base;
	snapshotDatabase__encode(&header, &cursor);

	return 0;

err_after_snapshot:
	/* Free the wal buffer */
	sqlite3_free(bufs[n - 1].base);
err:
	assert(rv != 0);
	return rv;
}

/* Decode the database contained in a snapshot. */
static int decodeDatabase(struct fsm *f, struct cursor *cursor)
{
	struct snapshotDatabase header;
	struct db *db;
	sqlite3_vfs *vfs;
	size_t n;
	int exists;
	int rv;

	rv = snapshotDatabase__decode(cursor, &header);
	if (rv != 0) {
		return rv;
	}
	rv = registry__db_get(f->registry, header.filename, &db);
	if (rv != 0) {
		return rv;
	}

	vfs = sqlite3_vfs_find(db->config->name);

	/* Check if the database file exists, and create it by opening a
	 * connection if it doesn't. */
	rv = vfs->xAccess(vfs, header.filename, 0, &exists);
	assert(rv == 0);

	if (!exists) {
		rv = db__open_follower(db);
		if (rv != 0) {
			return rv;
		}
		sqlite3_close(db->follower);
		db->follower = NULL;
	}

	tracef("main_size:%" PRIu64 " wal_size:%" PRIu64, header.main_size,
	       header.wal_size);
	if (header.main_size + header.wal_size > SIZE_MAX) {
		tracef("main_size + wal_size would overflow max DB size");
		return -1;
	}

	/* Due to the check above, this cast is safe. */
	n = (size_t)(header.main_size + header.wal_size);
	rv = VfsRestore(vfs, db->filename, cursor->p, n);
	if (rv != 0) {
		return rv;
	}
	cursor->p += n;

	return 0;
}

static unsigned dbNumPages(struct db *db)
{
	sqlite3_vfs *vfs;
	int rv;
	uint32_t n;

	vfs = sqlite3_vfs_find(db->config->name);
	rv = VfsDatabaseNumPages(vfs, db->filename, &n);
	assert(rv == 0);
	return n;
}

/* Determine the total number of raft buffers needed for a snapshot */
static unsigned snapshotNumBufs(struct fsm *f)
{
	struct db *db;
	queue *head;
	unsigned n = 1; /* snapshot header */

	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		n += 2; /* database header & wal */
		db = QUEUE_DATA(head, struct db, queue);
		n += dbNumPages(db); /* 1 buffer per page (zero copy) */
	}

	return n;
}

/* An example array of snapshot buffers looks like this:
 *
 * bufs:  SH DH1 P1 P2 P3 WAL1 DH2 P1 P2 WAL2
 * index:  0   1  2  3  4    5   6  7  8    9
 *
 * SH:   Snapshot Header
 * DHx:  Database Header
 * Px:   Database Page (not to be freed)
 * WALx: a WAL
 * */
static void freeSnapshotBufs(struct fsm *f,
			     struct raft_buffer bufs[],
			     unsigned n_bufs)
{
	queue *head;
	struct db *db;
	unsigned i;

	if (bufs == NULL || n_bufs == 0) {
		return;
	}

	/* Free snapshot header */
	sqlite3_free(bufs[0].base);

	i = 1;
	/* Free all database headers & WAL buffers */
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		if (i == n_bufs) {
			break;
		}
		db = QUEUE_DATA(head, struct db, queue);
		/* i is the index of the database header */
		sqlite3_free(bufs[i].base);
		/* i is now the index of the next database header (if any) */
		i += 1 /* db header */ + dbNumPages(db) + 1 /* WAL */;
		/* free WAL buffer */
		sqlite3_free(bufs[i - 1].base);
	}
}

static int fsm__snapshot(struct raft_fsm *fsm,
			 struct raft_buffer *bufs[],
			 unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	struct db *db;
	unsigned n_db = 0;
	unsigned i;
	int rv;

	/* First count how many databases we have and check that no transaction
	 * nor checkpoint nor other snapshot is in progress. */
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		if (db->tx_id != 0 || db->read_lock) {
			return RAFT_BUSY;
		}
		n_db++;
	}

	/* Lock all databases, preventing the checkpoint from running */
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		rv = databaseReadLock(db);
		assert(rv == 0);
	}

	*n_bufs = snapshotNumBufs(f);
	*bufs = sqlite3_malloc64(*n_bufs * sizeof **bufs);
	if (*bufs == NULL) {
		rv = RAFT_NOMEM;
		goto err;
	}

	rv = encodeSnapshotHeader(n_db, &(*bufs)[0]);
	if (rv != 0) {
		goto err_after_bufs_alloc;
	}

	/* Encode individual databases. */
	i = 1;
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		/* database_header + num_pages + wal */
		unsigned n = 1 + dbNumPages(db) + 1;
		rv = encodeDatabase(db, &(*bufs)[i], n);
		if (rv != 0) {
			goto err_after_encode_header;
		}
		i += n;
	}

	assert(i == *n_bufs);
	return 0;

err_after_encode_header:
	freeSnapshotBufs(f, *bufs, i);
err_after_bufs_alloc:
	sqlite3_free(*bufs);
err:
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		db = QUEUE_DATA(head, struct db, queue);
		databaseReadUnlock(db);
	}
	assert(rv != 0);
	return rv;
}

static int fsm__snapshot_finalize(struct raft_fsm *fsm,
				  struct raft_buffer *bufs[],
				  unsigned *n_bufs)
{
	struct fsm *f = fsm->data;
	queue *head;
	struct db *db;
	unsigned n_db;
	struct snapshotHeader header;
	int rv;

	if (bufs == NULL) {
		return 0;
	}

	/* Decode the header to determine the number of databases. */
	struct cursor cursor = {(*bufs)[0].base, (*bufs)[0].len};
	rv = snapshotHeader__decode(&cursor, &header);
	if (rv != 0) {
		tracef("decode failed %d", rv);
		return -1;
	}
	if (header.format != SNAPSHOT_FORMAT) {
		tracef("bad format");
		return -1;
	}

	/* Free allocated buffers */
	freeSnapshotBufs(f, *bufs, *n_bufs);
	sqlite3_free(*bufs);
	*bufs = NULL;
	*n_bufs = 0;

	/* Unlock all databases that were locked for the snapshot, this is safe
	 * because DB's are only ever added at the back of the queue. */
	n_db = 0;
	QUEUE_FOREACH(head, &f->registry->dbs)
	{
		if (n_db == header.n) {
			break;
		}
		db = QUEUE_DATA(head, struct db, queue);
		rv = databaseReadUnlock(db);
		assert(rv == 0);
		n_db++;
	}

	return 0;
}

static int fsm__restore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
	tracef("fsm restore");
	struct fsm *f = fsm->data;
	struct cursor cursor = {buf->base, buf->len};
	struct snapshotHeader header;
	unsigned i;
	int rv;

	rv = snapshotHeader__decode(&cursor, &header);
	if (rv != 0) {
		tracef("decode failed %d", rv);
		return rv;
	}
	if (header.format != SNAPSHOT_FORMAT) {
		tracef("bad format");
		return RAFT_MALFORMED;
	}

	for (i = 0; i < header.n; i++) {
		rv = decodeDatabase(f, &cursor);
		if (rv != 0) {
			tracef("decode failed");
			return rv;
		}
	}

	/* Don't use sqlite3_free as this buffer is allocated by raft. */
	raft_free(buf->base);

	return 0;
}

struct apply_uncommitted {
	pool_work_t work;
	struct raft_fsm_post_receive *raft_req;
	unsigned entry_index;
	unsigned entries_len;
	sqlite3 *conn;
	uint32_t cookie;
	uint32_t page_size;
	struct vfs2_wal_frame *frames;
	uint32_t frames_len;
	struct vfs2_wal_slice sl;
	struct raft_entry *entry;
	unsigned *count;
	raft_fsm_post_receive_cb cb;
};

static void apply_uncommitted_work_cb(pool_work_t *w)
{
	struct apply_uncommitted *req = CONTAINER_OF(w, struct apply_uncommitted, work);

	sqlite3_file *fp;
	sqlite3_file_control(req->conn, "main", SQLITE_FCNTL_FILE_POINTER, &fp);
	struct vfs2_wal_slice sl;
	int rv = vfs2_apply_uncommitted(fp, req->page_size, req->frames, req->frames_len, &sl);
	if (rv != 0) {
		req->work.rc = RAFT_IOERR;
		return;
	}

	struct vfs2_wal_slice *slp = raft_malloc(sizeof(sl));;
	if (slp == NULL) {
		req->work.rc = RAFT_NOMEM;
		return;
	}
	*slp = sl;
	req->entry->local_buf.base = slp;
	req->entry->local_buf.len = sizeof(*slp);
	req->work.rc = 0;
}

static void unapply_all(struct apply_uncommitted *apply_reqs, unsigned n)
{
	/* TODO */
	(void)apply_reqs;
	(void)n;
}

static void apply_uncommitted_after_work_cb(pool_work_t *w)
{
	struct apply_uncommitted *req = CONTAINER_OF(w, struct apply_uncommitted, work);

	sqlite3_free(req->frames);

	assert(*req->count > 0);
	*req->count -= 1;
	if (*req->count == 0) {
		sqlite3_free(req->count);

		struct raft_fsm_post_receive *raft_req = req->raft_req;
		unsigned entry_index = req->entry_index;
		unsigned entries_len = req->entries_len;
		raft_fsm_post_receive_cb cb = req->cb;
		struct apply_uncommitted *all_apply_reqs = req - entry_index;

		bool rollback = false;
		for (unsigned j = 0; j < entries_len; j++) {
			rollback |= (all_apply_reqs[j].work.rc != 0);
		}
		if (rollback) {
			unapply_all(all_apply_reqs, entries_len);
			return;
		}

		sqlite3_free(all_apply_reqs);
		cb(raft_req, 0);
	}
}

static int fsm_post_receive(struct raft_fsm *fsm, struct raft_fsm_post_receive *req, raft_fsm_post_receive_cb cb)
{
	struct fsm *f = fsm->data;
	int rv;

	struct apply_uncommitted *apply_reqs = sqlite3_malloc64(req->entries_len * sizeof(*apply_reqs));
	if (apply_reqs == NULL) {
		return RAFT_NOMEM;
	}
	unsigned *count = sqlite3_malloc(sizeof(*count));
	if (count == NULL) {
		return RAFT_NOMEM;
	}
	*count = 0;

	for (unsigned i = 0; i < req->entries_len; i++) {
		int type;
		void *cmd;
		rv = command__decode(&req->entries[i].buf, &type, &cmd);
		if (rv != 0) {
			return rv;
		}
		if (type != COMMAND_FRAMES) {
			apply_reqs[i] = (struct apply_uncommitted){};
			continue;
		}

		struct command_frames *cf = cmd;
		struct db *db;
		rv = registry__db_get(f->registry, cf->filename, &db);
		if (rv != 0) {
			return RAFT_NOMEM;
		}
		/* XXX 1<<16 issue */
		uint32_t page_size = cf->frames.page_size;

		uint32_t frames_len = cf->frames.n_pages;
		struct vfs2_wal_frame *frames = sqlite3_malloc64(frames_len * sizeof(*frames));
		if (frames == NULL) {
			return RAFT_NOMEM;
		}
		command_frames_fill_vfs2(cf, page_size, frames);

		/* XXX */
		assert(db->follower != NULL);

		apply_reqs[i].raft_req = req;
		apply_reqs[i].entry_index = i;
		apply_reqs[i].entries_len = req->entries_len;
		apply_reqs[i].conn = db->follower;
		apply_reqs[i].cookie = db->cookie;
		apply_reqs[i].page_size = page_size;
		apply_reqs[i].frames = frames;
		apply_reqs[i].frames_len = frames_len;
		apply_reqs[i].entry = &req->entries[i];
		apply_reqs[i].count = count;
		apply_reqs[i].cb = cb;

		*count += 1;
	}

	for (unsigned i = 0; i < req->entries_len; i++) {
		if (apply_reqs[i].conn == NULL) {
			continue;
		}
		pool_queue_work(f->pool, &apply_reqs[i].work, apply_reqs[i].cookie,
				WT_UNORD, apply_uncommitted_work_cb,
				apply_uncommitted_after_work_cb);
	}

	return 0;
}

int fsm__init(struct raft_fsm *fsm,
	      struct config *config,
	      struct registry *registry,
	      pool_t *pool)
{
	tracef("fsm init");
	struct fsm *f = raft_malloc(sizeof *f);

	if (f == NULL) {
		return DQLITE_NOMEM;
	}

	f->logger = &config->logger;
	f->registry = registry;
	f->pending.n_pages = 0;
	f->pending.page_numbers = NULL;
	f->pending.pages = NULL;
	f->pool = pool;

	fsm->version = 3;
	fsm->data = f;
	fsm->apply_async = fsm_apply_async;
	fsm->snapshot = fsm__snapshot;
	fsm->snapshot_finalize = fsm__snapshot_finalize;
	fsm->restore = fsm__restore;
	fsm->snapshot_async = NULL;
	fsm->post_receive = fsm_post_receive;

	return 0;
}

void fsm__close(struct raft_fsm *fsm)
{
	tracef("fsm close");
	struct fsm *f = fsm->data;
	raft_free(f);
}
