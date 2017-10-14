#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif

#include "api/glfs.h"
#include "api/glfs-handles.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define BUFFER_SIZE 0x100000

glfs_t *fs;
glfs_fd_t *fd;
char buf[BUFFER_SIZE];

int main(int argc, char **argv) {
	int size = 16, ret;
	fs = glfs_new (argv[1]);
    if (!fs) {
        fprintf (stderr, "glfs_new: returned NULL\n");
        return 1;
    }
    ret = glfs_set_volfile_server (fs, "rdma", argv[2], 24007);
    //ret = glfs_set_logging (fs, "/dev/stderr", 1);
    ret = glfs_init (fs);
    fprintf (stderr, "glfs_init: returned %d\n", ret);
    fd = glfs_open (fs, "/a", O_RDWR);
    for (int i = 0; i < 1000; i++) {
    	glfs_read(fd, buf, size, size * i);
    }
    glfs_close (fd);
    glfs_fini (fs);
}
