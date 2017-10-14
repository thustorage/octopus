#define HAS_NRFS
#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif
#include "mpi.h"

#ifdef HAS_NRFS
#include "nrfs.h"
#endif

#ifdef HAS_CEPH
#include <cephfs/libcephfs.h>
#endif

#ifdef HAS_GLFS
#include "api/glfs.h"
#include "api/glfs-handles.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#define BUFFER_SIZE 0x10000
#define WR_NUM 	(100)
int myid;
int numprocs;

#ifdef HAS_NRFS
nrfs fs;
#endif

#ifdef HAS_GLFS
glfs_t *fs;
glfs_fd_t *fd;
#endif

#ifdef HAS_CEPH
struct ceph_mount_info *fs;
int fd;
#endif

char buf[BUFFER_SIZE];
int collect_time(int cost)
{
	int i;
	char message[8];
	MPI_Status status;
	int *p = (int*)message;
	int max = cost;
	for(i = 1; i < numprocs; i++)
	{
		MPI_Recv(message, 8, MPI_CHAR, i, 99, MPI_COMM_WORLD, &status);
		if(*p > max)
			max = *p;
	}
	return max;
}

int locktest()
{
	char path[255];
	int i;
	double start, end;
	int time_cost;
	char message[8];
	int *p = (int*)message;

	sprintf(path, "/file_%d", 0);

	if(myid == 0)
	{
#ifdef HAS_NRFS
	nrfsOpenFile(fs, path, O_CREAT);
#endif
	}

#ifdef HAS_CEPH
	fd= ceph_open(fs, path, O_CREAT|O_RDWR,0777);
#endif

#ifdef HAS_GLFS
	fd = glfs_creat (fs, path, O_RDWR, 0644);
#endif

	MPI_Barrier ( MPI_COMM_WORLD );

	start = MPI_Wtime();
	for(i = 0; i < WR_NUM; i++)
	{
#ifdef HAS_NRFS
		//nrfsWrite(fs, path, buf, size, 0);
		for(i = 0; i < 1000; i++)
		nrfsRawRPC(fs);
#endif

#ifdef HAS_CEPH
		ceph_write(fs, fd, buf, size, size * i);
		ceph_fsync(fs, fd, 1);
#endif

#ifdef HAS_GLFS
		glfs_write(fd, buf, size, size * i);
		glfs_fdatasync(fd);
#endif
	}
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );

	*p = (int)((end - start) * 1000000);

	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		printf("write time cost: %d us\n", time_cost);
	}


	MPI_Barrier ( MPI_COMM_WORLD );

// 	start = MPI_Wtime();
// 	for(i = 0; i < WR_NUM; i++)
// 	{
// #ifdef HAS_NRFS
// 		nrfsRead(fs, path, buf, size, 0);
// #endif

// #ifdef HAS_CEPH
// 		ceph_read(fs, fd, buf, size, size * i);
// 		//ceph_fsync(fs, fd, 1);
// #endif

// #ifdef HAS_GLFS
// 		glfs_read(fd, buf, size, size * i);
// #endif
// 	}
// 	end = MPI_Wtime();

// 	MPI_Barrier ( MPI_COMM_WORLD );

// 	*p = (int)((end - start) * 1000000);

// 	if(myid != 0)
// 	{
// 		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
// 	}
// 	else
// 	{
// 		time_cost = collect_time(*p);
// 		printf("read time cost: %d us\n", time_cost);
// 	}
	return 0;
}

int main(int argc, char **argv)
{
#ifdef HAS_GLFS
	if(argc < 3)
	{
		fprintf(stderr, "Usage: ./locktest [vol] [host]\n");
		return -1;
	}
#endif
	MPI_Init( &argc, &argv);
	MPI_Comm_rank( MPI_COMM_WORLD, &myid );
	MPI_Comm_size( MPI_COMM_WORLD, &numprocs );
	MPI_Barrier ( MPI_COMM_WORLD );

	/* nrfs connection */
#ifdef HAS_NRFS
	fs = nrfsConnect("default", 0, 0);
#endif

#ifdef HAS_CEPH
    ret = ceph_create(&fs, NULL);
    if(ret)
    {
            fprintf(stderr, "ceph_create=%d\n", ret);
            exit(1);
    }
    printf("ceph create successful\n");
    ret = ceph_conf_read_file(fs, "/etc/ceph/ceph.conf");
    if(ret)
    {
            fprintf(stderr, "ceph_conf_read_file=%d\n", ret);
            exit(1);
    }
    printf("ceph read config successful\n");
    ret = ceph_mount(fs, NULL);
    if (ret)
    {
            fprintf(stderr, "ceph_mount=%d\n", ret);
            exit(1);
    }
    printf("ceph mount successful\n");
    ret = ceph_chdir(fs, "/");
    if (ret)
    {
            fprintf(stderr, "ceph_chdir=%d\n", ret);
            exit(1);
    }
#endif

#ifdef HAS_GLFS
    fs = glfs_new (argv[1]);
    if (!fs) {
        fprintf (stderr, "glfs_new: returned NULL\n");
        return 1;
    }
    ret = glfs_set_volfile_server (fs, "rdma", argv[2], 24007);
    ret = glfs_set_logging (fs, "/dev/stderr", 1);
    ret = glfs_init (fs);
    fprintf (stderr, "glfs_init: returned %d\n", ret);
#endif

	MPI_Barrier ( MPI_COMM_WORLD );

	locktest();

	MPI_Barrier ( MPI_COMM_WORLD );

#ifdef HAS_NRFS
	nrfsDisconnect(fs);
#endif

#ifdef HAS_CEPH
	ceph_unmount(fs);
    ceph_release(fs);
#endif

#ifdef HAS_GLFS
    glfs_fini (fs);
#endif

	MPI_Finalize();
}
