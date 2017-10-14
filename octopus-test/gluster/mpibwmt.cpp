#define HAS_GLFS
#ifndef __USE_FILE_OFFSET64
#define __USE_FILE_OFFSET64
#endif
#include "mpi.h"
#include <thread>
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
#define BUFFER_SIZE 0x100000
int myid, file_seq;
int numprocs;
using namespace std;
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

// char buf[BUFFER_SIZE];
int mask = 0;
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

int write_test(int size, int op_time, int id)
{
	//printf("id = %d\n", id);
	char buf[BUFFER_SIZE];
	char path[255];
	int i;
	double start, end, rate, num;
	int time_cost;
	char message[8];
	int *p = (int*)message;

	/* file open */
	sprintf(path, "/file_%d", id);
#ifdef HAS_NRFS
	nrfsOpenFile(fs, path, O_CREAT);
#endif

#ifdef HAS_CEPH
	fd= ceph_open(fs, path, O_CREAT|O_RDWR,0777);
#endif

#ifdef HAS_GLFS
	fd = glfs_creat (fs, path, O_RDWR, 0644);
#endif
	
	printf("create file: %s\n", path);
	memset(buf, 'a', BUFFER_SIZE);

	//MPI_Barrier ( MPI_COMM_WORLD );
	
	start = MPI_Wtime();
	for(i = 0; i < op_time; i++)
	{
#ifdef HAS_NRFS
		nrfsWrite(fs, path, buf, size, size * i);
#endif

#ifdef HAS_CEPH
		ceph_write(fs, fd, buf, size, size * i);
		ceph_fsync(fs, fd, 1);
#endif

#ifdef HAS_GLFS
		glfs_write(fd, buf, size, size * i);
#endif
	}
	end = MPI_Wtime();

	//MPI_Barrier ( MPI_COMM_WORLD );
	printf("w=%.2f\n", (end - start)*1000000);
	/**p = (int)((end - start) * 1000000);
	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		num = (double)(size * op_time * numprocs) / time_cost;
		rate = 1000000 * num / 1024 / 1024;
		printf("write file rate: %f MB/s\n", rate);
	}*/
#ifdef HAS_NRFS
		nrfsCloseFile(fs, path);
#endif

#ifdef HAS_CEPH
		ceph_close(fs, fd);
#endif

#ifdef HAS_GLFS
		glfs_close (fd);
#endif

	file_seq += 1;
	if(file_seq == numprocs)
		file_seq = 0;
}

int read_test(int size, int op_time, int id)
{
	char buf[BUFFER_SIZE];
	char path[255];
	int i;
	double start, end, rate, num;
	int time_cost;
	char message[8];
	int *p = (int*)message;

	memset(buf, '\0', BUFFER_SIZE);
	memset(path, '\0', 255);
	sprintf(path, "/file_%d", id);

#ifdef HAS_NRFS
	//nrfsOpenFile(fs, path, O_CREAT);
#endif

#ifdef HAS_CEPH
	fd= ceph_open(fs, path, O_CREAT|O_RDWR,0777);
#endif

#ifdef HAS_GLFS
    fd = glfs_open (fs, path, O_RDWR);
#endif

	//MPI_Barrier ( MPI_COMM_WORLD );
	
	start = MPI_Wtime();
	for(i = 0; i < op_time; i++)
	{
#ifdef HAS_NRFS
		nrfsRead(fs, path, buf, size, size * i);
#endif

#ifdef HAS_CEPH
		ceph_read(fs, fd, buf, size, size * i);
#endif

#ifdef HAS_GLFS
		glfs_read(fd, buf, size, size * i);
#endif
	}
	end = MPI_Wtime();
	printf("r=%.2f\n", (end - start)*1000000);
	//MPI_Barrier ( MPI_COMM_WORLD );
/*
	*p = (int)((end - start) * 1000000);

	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		num = (double)(size * op_time * numprocs) / time_cost;
		rate = 1000000 * num / 1024 / 1024;
		printf("read file rate: %f MB/s\n", rate);
	}

	MPI_Barrier ( MPI_COMM_WORLD );
	*/
#ifdef HAS_NRFS
		//nrfsCloseFile(fs, path);
#endif

#ifdef HAS_CEPH
		ceph_close(fs, fd);
#endif

#ifdef HAS_GLFS
		glfs_close (fd);
#endif
	file_seq += 1;
	if(file_seq == myid)
		file_seq = 0;
}


int main(int argc, char **argv)
{

	char path[255];
	if(argc < 3)
	{
		fprintf(stderr, "Usage: ./mpibw block_size\n");
		return -1;
	}
	thread th[20];
	int block_size = atoi(argv[1]);
	int op_time = atoi(argv[2]);
	int i, ret;
	MPI_Init( &argc, &argv);
	MPI_Comm_rank( MPI_COMM_WORLD, &myid );
	MPI_Comm_size( MPI_COMM_WORLD, &numprocs );
	file_seq = myid;
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
    fs = glfs_new (argv[3]);
    if (!fs) {
        fprintf (stderr, "glfs_new: returned NULL\n");
        return 1;
    }
    ret = glfs_set_volfile_server (fs, "rdma", argv[4], 24007);
    //ret = glfs_set_logging (fs, "/dev/stderr", 1);
    ret = glfs_init (fs);
    fprintf (stderr, "glfs_init: returned %d\n", ret);
#endif

	MPI_Barrier ( MPI_COMM_WORLD );
	for (int i = 0; i < 16; i++){
		th[i] = thread(write_test, 1024 * block_size, op_time, i);
	}
	for (int i = 0; i < 16; i++)
		th[i].join();
	/*for (int i = 0; i < 16; i++)
		th[i] = thread(read_test, 1024 * block_size, op_time, i);
	for (int i = 0; i < 16; i++)
		th[i].join();*/
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
