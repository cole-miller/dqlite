#ifndef DQLITE_VFS2_H
#define DQLITE_VFS2_H

#include <sqlite3.h>

#include <stddef.h>
#include <stdint.h>

/*
 * The VFS maintains dqlite's managed databases on disk. It interprets
 * "syscalls" made internally by SQLite as well as manual commands issued by
 * dqlite.
 *
 * Layout of files on disk
 * -----------------------
 *
 * Each managed database `foo.db` is typically represented by the following
 * files on disk:
 *
 * - foo.db
 * - foo.db-wal
 * - foo.db-xwal1
 * - foo.db-xwal2
 *
 * These four filenames represent only three distinct physical files:
 * foo.db-wal is a hard link that points to the same physical file as either
 * foo.db-xwal1 or foo.db-xwal2.
 *
 * foo.db is a SQLite database file in the conventional format. foo.db-wal is a
 * SQLite WAL file in the conventional format. It's intended that you can use
 * vanilla SQLite to open a (read-only, please!) connection to your dqlite
 * database this way.
 *
 * At any given time, the physical file pointed to by foo.db-wal contains the
 * most recent transactions, and the other physical WAL contains older
 * transactions. New transactions (whether originated locally or received from
 * a remote leader) go into foo.db-wal. Periodically, foo.db-wal is
 * checkpointed. When all the frames in foo.db-wal have been checkpointed, the
 * next new transaction will trigger a WAL swap.
 *
 * In a WAL swap, the target of the foo.db-wal hard link is changed from
 * foo.db-xwal1 to foo.db-xwal2, or vice versa.  The "incoming" WAL is
 * overwritten from the beginning. In vanilla SQLite, there would be only one
 * physical WAL, and the WAL swap would always start overwriting this same file
 * from the beginning.
 *
 * The motivation for toggling between two physical WALs in this way is to
 * maintain a buffer of individual transactions stored on disk, so that raft
 * can use them to construct AppendEntries messages. This reduces the incidence
 * of the expensive InstallSnapshot operation.
 *
 */

/**
 * Create a new VFS object that wraps the given VFS object.
 *
 * The returned VFS is allocated on the heap and lives until vfs2_destroy is
 * called. Its methods are thread-safe if those of the wrapped VFS are, but the
 * methods of the sqlite3_file objects it creates are not thread-safe.
 * Therefore, a database connection that's created using this VFS should only
 * be used on the thread that opened it. The functions below that operate on
 * sqlite3_file objects created by this VFS should also only be used on that
 * thread.
 */
sqlite3_vfs *vfs2_make(sqlite3_vfs *orig, const char *name);

/**
 * Salt values as stored in the header of a SQLite WAL.
 *
 * These should be treated as opaque. They uniquely identify a physical WAL
 * file.
 */
struct vfs2_salts { uint8_t salt1[4]; uint8_t salt2[4]; };

/**
 * Metadata about a write transaction.
 *
 * This can be used to read the whole transaction from disk, if the
 * corresponding physical WAL hasn't been overwritten yet.
 */
struct vfs2_wal_slice {
	struct vfs2_salts salts;
	uint32_t start;
	uint32_t len;
};

/**
 * In-memory copy of a single frame from a write transaction.
 *
 * This structure contains only the portable fields of a frame -- the ones that
 * can be sent between nodes in an AppendEntries message. The other fields are
 * filled in locally.
 */
struct vfs2_wal_frame {
	uint32_t page_number;
	uint32_t commit;
	void *page;
};

/**
 */
struct vfs2_wal_txn {
	struct vfs2_wal_slice meta;
	struct vfs2_wal_frame *frames;
};

/*
 * Every function below that takes an argument of type sqlite3_file* should obtain
 * that argument using the SQLITE_FCNTL_FILE_POINTER file control.
 */

/**
 * Append a single transaction to the WAL as a nonleader.
 *
 * The transaction will not be marked as committed. In nonleader state, the WAL
 * can contain several successive uncommitted transactions.
 *
 * The vfs2_wal_slice out parameter yields a lightweight "handle" that can be
 * used to identify this transaction later.
 *
 * The page size parameter exists only as a sanity check.
 */
int vfs2_apply_uncommitted(sqlite3_file *file, uint32_t page_size, const struct
		vfs2_wal_frame *frames, unsigned n, struct vfs2_wal_slice
		*out);

/**
 * Logically remove some transactions from the WAL as a nonleader.
 *
 * This is done in response to the raft log being truncated. The vfs2_wal_slice
 * argument identifies the first transaction that should be removed.
 */
int vfs2_unapply(sqlite3_file *file, struct vfs2_wal_slice first_to_unapply);

/**
 * Mark some transactions in the WAL as committed as a nonleader.
 *
 * The vfs2_wal_slice parameter identifies the last transaction that should be
 * marked as committed.
 */
int vfs2_commit(sqlite3_file *file, struct vfs2_wal_slice last_committed);

/**
 * Mark all transactions in the WAL as committed, and prepare to be a leader.
 *
 * This must be called when a new leader commits the first barrier entry in its
 * term. Only after this is called can the "leader family" of functions below be
 * used.
 */
int vfs2_commit_barrier(sqlite3_file *file);

/**
 * Retrieve frames that were appended to the WAL by the last write transaction as a leader.
 *
 * Polling the same transaction more than once is an error.
 */
int vfs2_poll(sqlite3_file *file, struct vfs2_wal_frame **frames, unsigned *n, struct vfs2_wal_slice *sl);

/**
 * Mark the last write transaction as being fully committed in raft as a leader.
 *
 * This allows the next write transaction to execute. It should be used in leader state
 * by the node that executed a write transaction. Call it on the database main file
 * object.
 */
int vfs2_unhide(sqlite3_file *file);

/**
 * Cancel a pending transaction as a leader.
 *
 * Calling this function when there is no pending transaction is an error.
 * It's okay to call it whether or not the transaction has been polled.
 */
int vfs2_abort(sqlite3_file *file);

/**
 * Synchronously read some transaction data directly from the WAL.
 *
 * Fill the `meta` field of each vfs2_wal_txn with a slice that was previously
 * returned by vfs2_shallow_poll. On return, this function will set the `frames`
 * field of each vfs2_wal_txn, using memory from the SQLite allocator that the
 * caller must free, if the transaction was read successfully. Setting this
 * field to NULL means that the transaction couldn't be read.
 */
int vfs2_read_wal(sqlite3_file *file,
		  struct vfs2_wal_txn *txns,
		  size_t txns_len);

/**
 * Set a read mark on the WAL.
 *
 * This can be used to control checkpoints.
 */
int vfs2_pseudo_read_begin(sqlite3_file *file, uint32_t target, unsigned *out);

/**
 * Unset a read mark that was previously set by vfs2_pseudo_read_begin.
 */
int vfs2_pseudo_read_end(sqlite3_file *file, unsigned i);

/**
 * Destroy the VFS object.
 *
 * Call this from the same thread that called vfs2_make. No connection may be
 * open that uses this VFS.
 */
void vfs2_destroy(sqlite3_vfs *vfs);

#endif
