#include "nrfs.h"
#include <sys/time.h>
#define COUNT 50
char name[][10] = {"/file1", "/file2", "/file3", "/file4", "/file5", "/file6"};
nrfs fs;
nrfsFile file;
struct  timeval    tv;
long long t1, t2;
int thread_num;
int filesize;
char buf[0x10000010];
void filesystem_test()
{
    int op, i;
    char path[255] = {0};
    struct  timeval    tv;
    long long t1, t2;
    while(true)
    {
        scanf("%d", &op);
        switch(op)
        {
            case 1:
            {
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/file_%d", i);
                    nrfsMknod(fs, path);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("mknod 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 2:
            {
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/file_%d", 1);
                    nrfsAccess(fs, path);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("access 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 3:
            {
                FileMeta attr;
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/file_%d", 1);
                    nrfsGetAttribute(fs, path, &attr);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("getattr 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 4:
            {
                memset(path, '\0', 255);
                sprintf(path, "/dir");
                nrfsCreateDirectory(fs, path);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/dir/dir_%d", i);
                    nrfsCreateDirectory(fs, path);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("mkdir 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 5:
            {
                char newpath[255] = {0};
                memset(path, '\0', 255);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/file_%d", i);
                    sprintf(newpath, "/file_%d", (i + 100));
                    nrfsRename(fs, path, newpath);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("rename 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 6:
            {
                memset(path, '\0', 255);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/file_%d", i + 100);
                    nrfsDelete(fs, path);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("remove 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 7:
            {
                memset(path, '\0', 255);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/dir/dir_%d", i);
                    nrfsDelete(fs, path);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("rmdir 100 times, time cost: %d\n", (int)(t2 - t1));
                nrfsDelete(fs, "/dir");
                break;
            }
            case 8:
            {
                memset(path, '\0', 255);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/dir/dir_%d", i);
                    nrfsTest(fs, 0);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("test rpc 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            case 9:
            {
                memset(path, '\0', 255);
                gettimeofday(&tv, NULL);
                t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                for(i = 0; i < COUNT; i++)
                {
                    sprintf(path, "/dir/dir_%d", i);
                    nrfsRawRPC(fs);
                }
                gettimeofday(&tv, NULL);
                t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
                printf("test RawRPC 100 times, time cost: %d\n", (int)(t2 - t1));
                break;
            }
            default:
                break;
        }
    }
}

void test(int i)
{
    char path[25];
    nrfsFile file;
    sprintf(path, "/a%d", i);
    int j;
    for(j = 0; j < 0x10000010; j++)
        buf[j] = 'a';
    buf[1000] = 'b';
    buf[10000] = 'b';
    buf[0x1000000] = 'b';
    buf[0x10000001] = 'b';
	file = nrfsOpenFile(fs, path, O_CREAT);
    nrfsWrite(fs, file, buf, 0x10000010, 0);
    memset(buf, '\0', 0x10000010);
    nrfsRead(fs, file, buf, 0x10000010, 0);
    printf("%c %c %c %c", buf[1001], buf[10000], buf[0x1000001], buf[0x10000001]);
	nrfsCloseFile(fs, file);
    nrfsDelete(fs, path);
}
void test_client()
{
	int i;
	gettimeofday(&tv, NULL);
    t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
	for(i = 0; i < 1024; i++)
		;//nrfsTest(fs, name[0]);
	gettimeofday(&tv, NULL);
    t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
    printf("time cost: %d\n", (int)(t2 - t1));
}
int main(int argc, char* argv[])
{
    if(argc < 2)
    {
        printf("Usage: ./nrfstest filesize");
        return 0;
    }
    filesize = atoi(argv[1]);
	fs = nrfsConnect("default", 0, 0);
	// file = nrfsOpenFile(fs, name[0], 0);
	// thread t[20];
	// int i, ret;
    // nrfsMknod(fs, "/a");
    // nrfsWrite(fs, "/a", "a", 1, 1024 * 1024 -1);
    // nrfsWrite(fs, "/a", "a", 1, 2 * 1024 * 1024 -1);
    // nrfsWrite(fs, "/a", "a", 1, 3 * 1024 * 1024 -1);
    // nrfsWrite(fs, "/a", "a", 1, 4 * 1024 * 1024 -1);
    filesystem_test();
    gettimeofday(&tv, NULL);
    t1 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);

    //test(0);
   
    gettimeofday(&tv, NULL);
    t2 = (tv.tv_sec * 1000 * 1000) + (tv.tv_usec);
    printf("time cost: %d\n", (int)(t2 - t1));
	// nrfsCloseFile(fs, file);
	//filesystem_test();
	nrfsDisconnect(fs);
    // FileMeta attr;
    // fs = nrfsConnect("default", 10086, 13);
    // nrfsMknod(fs, "/a");
    // nrfsGetAttribute(fs, "/a", &attr);
    // nrfsDelete(fs, "/a");
    // nrfsDisconnect(fs);
}
