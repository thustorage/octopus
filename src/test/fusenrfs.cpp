#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <string.h>
#include "nrfs.h"
#include "fuse.h"
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#include <mutex>
using namespace std;

nrfs fs;

mutex mtx;

static struct fuse_operations fuse_oper;

static int fuse_getattr(const char *path, struct stat *stbuf)
{
	FileMeta attr;
	int res;
	res = nrfsGetAttribute(fs, (nrfsFile)path, &attr);
	if(res)
		return -2;
	if(attr.count == MAX_FILE_EXTENT_COUNT)
	{
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_size = 0;
	}
	else
	{
		stbuf->st_size = attr.size;
		stbuf->st_mode = S_IFREG | 0755;
	}

    stbuf->st_nlink = 1;            /* Count of links, set default one link. */
    stbuf->st_uid = 0;              /* User ID, set default 0. */
    stbuf->st_gid = 0;              /* Group ID, set default 0. */
    stbuf->st_rdev = 0;             /* Device ID for special file, set default 0. */

	//stbuf->st_blksize = BLOCK_SIZE;

    stbuf->st_atime = 0;            /* Time of last access, set default 0. */
    stbuf->st_mtime = attr.timeLastModified; /* Time of last modification, set default 0. */
    stbuf->st_ctime = 0;            /* Time of last creation or status change, set default 0. */
	return 0;
}

static int fuse_access(const char *path, int mask)
{
	int res;
	res = nrfsAccess(fs, path);
	if(res == 0)
		return 0;
	else
		return -2;
}

static int fuse_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	nrfsfilelist list;
	uint32_t i;
	struct stat st;
	nrfsListDirectory(fs, path, &list);
	for(i = 0; i < list.count; i++)
	{
		memset(&st, 0, sizeof(st));
		st.st_mode = (list.tuple[i].isDirectories == 1) ? S_IFDIR : S_IFMT;
		if (filler(buf, list.tuple[i].names, &st, 0))
			break;
	}
	return 0;
}

static int fuse_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;
	res = nrfsMknod(fs, path);
	if(res == 0)
		return 0;
	else
		return -1;
}
static int fuse_mkdir(const char *path, mode_t mode)
{
	int res = nrfsCreateDirectory(fs, path);
	if(res == 0)
		return 0;
	else
		return -1;
}
static int fuse_rmdir(const char *path)
{
	int res = nrfsDelete(fs, path);
	if(res == 0)
		return 0;
	else 
		return -2;
}

static int fuse_unlink(const char *path)
{
	int res = nrfsDelete(fs, path);
	if(res == 0)
		return 0;
	else
		return -2;
}

static int fuse_release(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

static int fuse_truncate(const char *path, off_t size)
{
	return 0;
}
static int fuse_rename(const char *from, const char *to)
{
	int res = nrfsRename(fs, from, to);
	if(res == 0)
		return 0;
	else
		return -17;
}

static int fuse_chmod(const char *path, mode_t mode)
{
	return 0;
}

static int fuse_open(const char *path, struct fuse_file_info *fi)
{
	return 0;
}
static int fuse_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	int res;
	lock_guard<mutex> lock(mtx); 
	res = nrfsRead(fs, (nrfsFile)path, buf, (uint64_t)size, (uint64_t)offset);
	printf("res = %d\n", res);
	return res;
}
static int fuse_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	int res;
	lock_guard<mutex> lock(mtx); 
	res = nrfsWrite(fs, (nrfsFile)path, buf, (uint64_t)size, (uint64_t)offset);
	return res;
}
static int fuse_statfs(const char *path, struct statvfs *stbuf)
{
	stbuf->f_bsize = 1024 * 1024;
	return 0;
}

int main(int argc, char* argv[])
{
	fuse_oper.getattr = fuse_getattr;
	fuse_oper.access = fuse_access;
	fuse_oper.mknod = fuse_mknod;
	fuse_oper.mkdir = fuse_mkdir;
	fuse_oper.unlink = fuse_unlink;
	fuse_oper.rmdir = fuse_rmdir;
	fuse_oper.rename = fuse_rename;
	fuse_oper.open = fuse_open;
	fuse_oper.release = fuse_release;
	fuse_oper.read = fuse_read;
	fuse_oper.truncate = fuse_truncate;
	fuse_oper.write = fuse_write;
	fuse_oper.statfs = fuse_statfs;
	fuse_oper.readdir = fuse_readdir;
	fuse_oper.chmod = fuse_chmod;
	fs = nrfsConnect("default", 0, 0);
	fuse_main(argc, argv, &fuse_oper, NULL);
}