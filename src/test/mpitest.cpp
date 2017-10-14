/***
*
* 
*
***/

#include "mpi.h"
#include "nrfs.h"
#include <stdio.h>
int myid;
int numprocs;
nrfs fs;
int file_num = 0;
int directory_num = 0;
int tree_value = 2;
int max_depth = 5;

void create_directory(char* _path, int depth)
{
	char path[255];
	int i;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			nrfsCreateDirectory(fs, path);
			directory_num += 1;
			create_directory(path, depth + 1);
		}
	}
}

void create_file(char* _path, int depth)
{
	char path[255];
	char file[255];
	int i, j;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			for(j = 0; j < tree_value; j++)
			{
				sprintf(file, "%s/dir_%d.%d/file_%d.%d", _path, myid, i, myid, j);
				nrfsMknod(fs, file);
				file_num += 1;
			}
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			create_file(path, depth + 1);
		}
	}
}
void file_stat(char* _path, int depth)
{
	char path[255];
	char file[255];
	FileMeta attr;
	int i, j;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			for(j = 0; j < tree_value; j++)
			{
				sprintf(file, "%s/dir_%d.%d/file_%d.%d", _path, myid, i, myid, j);
				nrfsGetAttribute(fs, file, &attr);
			}
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			file_stat(path, depth + 1);
		}
	}
}
void directory_stat(char* _path, int depth)
{
	char path[255];
	FileMeta attr;
	int i;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			nrfsGetAttribute(fs, path, &attr);
			directory_stat(path, depth + 1);
		}
	}
}
void file_remove(char* _path, int depth)
{
	char path[255];
	char file[255];
	int i, j;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			for(j = 0; j < tree_value; j++)
			{
				sprintf(file, "%s/dir_%d.%d/file_%d.%d", _path, myid, i, myid, j);
				nrfsDelete(fs, file);
			}
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			file_remove(path, depth + 1);
		}
	}
}
void directory_remove(char* _path, int depth)
{
	char path[255];
	int i;
	if(depth < max_depth)
	{
		for(i = 0; i < tree_value; i++)
		{
			sprintf(path, "%s/dir_%d.%d", _path, myid, i);
			directory_remove(path, depth + 1);
			nrfsDelete(fs, path);
		}
	}
}

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
void time_counter(double diff, int op_num)
{
	char message[8];
	int *p = (int*)message;
	double num, rate;
	int time_cost;
	*p = (int)(diff * 1000000);
	if(myid != 0)
	{
		MPI_Send(message, 8, MPI_CHAR, 0, 99, MPI_COMM_WORLD);
	}
	else
	{
		time_cost = collect_time(*p);
		num = (double)(op_num * numprocs) / time_cost;
		rate = 1000000 * num;
		printf("%d\t\t%d\t\t%.2f\n", op_num * numprocs, time_cost, rate);
		//printf("%d dir created with time: %d, rate: %f\n", op_num * numprocs, time_cost, rate);
	}
}
int main(int argc, char **argv)
{
	char path[255];
	double start, end;

	MPI_Init( &argc, &argv);
	MPI_Comm_rank( MPI_COMM_WORLD, &myid );
	MPI_Comm_size( MPI_COMM_WORLD, &numprocs );
	/* file system connect */
	MPI_Barrier ( MPI_COMM_WORLD );
	fs = nrfsConnect("default", 0, 0); 
	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
	{
		printf("operation\t\tnumber\t\ttime(us)\tIOPS\n");
	}
	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );
	/* dir creation */
	path[0] = '\0';
	start = MPI_Wtime();
	create_directory(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("dir create\t\t");
	time_counter(end - start, directory_num);


	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	/* file creation */
	start = MPI_Wtime();
	create_file(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("file create\t\t");
	time_counter(end - start, file_num);


	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	/* file stat */
	start = MPI_Wtime();
	file_stat(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("file stat\t\t");
	time_counter(end - start, file_num);



	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	/* directory stat */
	start = MPI_Wtime();
	directory_stat(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("dir stat\t\t");
	time_counter(end - start, directory_num);



	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	/* file remove */
	start = MPI_Wtime();
	file_remove(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("file remove\t\t");
	time_counter(end - start, file_num);



	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	/* dir remove */
	start = MPI_Wtime();
	directory_remove(path, 0);
	end = MPI_Wtime();

	MPI_Barrier ( MPI_COMM_WORLD );
	if(myid == 0)
		printf("dir remove\t\t");
	time_counter(end - start, directory_num);


	MPI_Barrier ( MPI_COMM_WORLD );
	//usleep(1000000);
	MPI_Barrier ( MPI_COMM_WORLD );

	nrfsDisconnect(fs);

	MPI_Finalize();
}
