#include "vfs2.h"

#include <sqlite3.h>

#include <stddef.h>

struct vfs2_data {
	sqlite3_vfs *orig;
};

struct vfs2_file {
	struct sqlite3_file base;
	sqlite3_file *orig;
	int flags;
};

struct vfs2_wal_file {
	struct vfs2_file base;
	sqlite3_file *wal_prev;
};

static int vfs2_open(sqlite3_vfs *vfs, sqlite3_filename name, sqlite3_file *out, int flags, int *out_flags) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xOpen(data->orig, name, out, flags, out_flags);
}

static int vfs2_delete(sqlite3_vfs *vfs, const char *name, int sync_dir) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDelete(data->orig, name, sync_dir);
}

static int vfs2_access(sqlite3_vfs *vfs, const char *name, int flags, int *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xAccess(data->orig, name, flags, out);
}

static int vfs2_full_pathname(sqlite3_vfs *vfs, const char *name, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xFullPathname(data->orig, name, n, out);
}

static void *vfs2_dl_open(sqlite3_vfs *vfs, const char *filename) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlOpen(data->orig, filename);
}

static void vfs2_dl_error(sqlite3_vfs *vfs, int n, char *msg) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlError(data->orig, n, msg);
}

typedef void (*vfs2_sym)(void);
static vfs2_sym vfs2_dl_sym(sqlite3_vfs *vfs, void *dl, const char *symbol) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlSym(data->orig, dl, symbol);
}

static void vfs2_dl_close(sqlite3_vfs *vfs, void *dl) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xDlClose(data->orig, dl);
}

static int vfs2_randomness(sqlite3_vfs *vfs, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xRandomness(data->orig, n, out);
}

static int vfs2_sleep(sqlite3_vfs *vfs, int microseconds) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xSleep(data->orig, microseconds);
}

static int vfs2_current_time(sqlite3_vfs *vfs, double *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xCurrentTime(data->orig, out);
}

static int vfs2_get_last_error(sqlite3_vfs *vfs, int n, char *out) {
	struct vfs2_data *data = vfs->pAppData;
	return data->orig->xGetLastError(data->orig, n, out);
}

static int vfs2_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *out) {
	struct vfs2_data *data = vfs->pAppData;
	if (data->orig->iVersion < 2) {
		return SQLITE_ERROR;
	}
	return data->orig->xCurrentTimeInt64(data->orig, out);
}

sqlite3_vfs *vfs2_make(sqlite3_vfs *orig) {
	struct vfs2_data *data = sqlite3_malloc(sizeof(*data));
	if (data == NULL) {
		goto err;
	}
	data->orig = orig;
	struct sqlite3_vfs *vfs = sqlite3_malloc(sizeof(*vfs));
	if (vfs == NULL) {
		goto err_after_alloc_data;
	}
	vfs->iVersion = 2;
	vfs->szOsFile = sizeof(struct vfs2_file);
	vfs->mxPathname = orig->mxPathname;
	vfs->zName = "dqlite-vfs2";
	vfs->pAppData = data;
	vfs->xOpen = vfs2_open;
	vfs->xDelete = vfs2_delete;
	vfs->xAccess = vfs2_access;
	vfs->xFullPathname = vfs2_full_pathname;
	vfs->xDlOpen = vfs2_dl_open;
	vfs->xDlError = vfs2_dl_error;
	vfs->xDlSym = vfs2_dl_sym;
	vfs->xDlClose = vfs2_dl_close;
	vfs->xRandomness = vfs2_randomness;
	vfs->xSleep = vfs2_sleep;
	vfs->xCurrentTime = vfs2_current_time;
	vfs->xGetLastError = vfs2_get_last_error;
	vfs->xCurrentTimeInt64 = vfs2_current_time_int64;
	return vfs;

err_after_alloc_data:
	sqlite3_free(data);
err:
	return NULL;
}
