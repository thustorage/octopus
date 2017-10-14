#include "nrfs.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#define PATH_LEN 500
#define CMD_LEN 20
static char path[PATH_LEN] = "/";
static char workPath[PATH_LEN];
static char cmd[CMD_LEN];
nrfs fs;

void usage();
void getCmd();
void doCmd();
void do_cd();
void do_ls();
void do_touch();
void do_write();
void do_cat();
void do_rm();
void do_mkdir();
void do_clear();
void do_mv();
void do_pwd();
int main()
{
	fs = nrfsConnect("default", 0, 0);
	usage();
    while(true)
    {
        printf("\033[0;31m[nrfs @ %s]\033[0m", path);
        getCmd();
        if((strcmp(cmd, "q") == 0) || (strcmp(cmd, "quit") == 0) || (strcmp(cmd, "exit") == 0))
        {
        	nrfsDisconnect(fs);
            break;
        }
        doCmd();
    }
}
void usage()
{
    fprintf( stdout, "+----------------------------------------------------------------------------+\n");
    fprintf( stdout, "|                                                                            |\n");
    fprintf( stdout, "|**********************************  nrfs  **********************************|\n");
    fprintf( stdout, "|      Info:                                                                 |\n");
    fprintf( stdout, "|       A RDMA & NVDIMM based distributed file system                        |\n");
    fprintf( stdout, "|                                                                            |\n");
    fprintf( stdout, "|      Options:                                                              |\n");
    fprintf( stdout, "|       -ls [path], list the file and diretory.                              |\n");
    fprintf( stdout, "|       -pwd, print working directory.                                       |\n");
    fprintf( stdout, "|       -write filename, write data to the file.                             |\n");
    fprintf( stdout, "|       -cat filename, read file data.                                       |\n");
    fprintf( stdout, "|       -cd [..|diretory], change working path.                              |\n");
    fprintf( stdout, "|       -touch filename, create file.                                        |\n");
    fprintf( stdout, "|       -mkdir dirname, create directory.                                    |\n");
    fprintf( stdout, "|       -rm file|dirname, remove file | diretory.                            |\n");
    fprintf( stdout, "|       -mv oldfilename, change file name.                                   |\n");
    fprintf( stdout, "|       -clear, flush the screen.                                            |\n");
    fprintf( stdout, "+----------------------------------------------------------------------------+\n");
}
void getCmd()
{
	char temp[PATH_LEN + CMD_LEN];
    memset(cmd, '\0', CMD_LEN);
    memset(workPath, '\0', PATH_LEN);
	memset(temp, '\0', PATH_LEN + CMD_LEN);
    // while((ch = getchar()) != '\n')
    // 	temp[i++] = ch;
    fgets(temp, PATH_LEN + CMD_LEN, stdin);
    if(strlen(temp) <= 1)
    	return;
    temp[strlen(temp) - 1] = '\0';
	const char *d = " ";
	char *element;
	element = strtok(temp, d);
	strcpy(cmd, element);
	element = strtok(NULL, d);
	if(element)
	{
		strcpy(workPath, element);
	}
}

void doCmd()
{
    if(strcmp(cmd, "cd") == 0)
        do_cd();
    else if(strcmp(cmd, "ls") == 0)
        do_ls();
    else if(strcmp(cmd, "touch") == 0)
        do_touch();
    else if(strcmp(cmd, "mkdir") == 0)
        do_mkdir();
    else if(strcmp(cmd, "write") == 0)
        do_write();
    else if(strcmp(cmd, "cat") == 0)
        do_cat();
    else if(strcmp(cmd, "rm") == 0)
        do_rm();
    else if(strcmp(cmd, "clear") == 0)
        do_clear();
    else if(strcmp(cmd, "mv") == 0)
        do_mv();
    else if(strcmp(cmd, "pwd") == 0)
        do_pwd();
    else if (cmd[0] != '\0')
        printf("\033[0;33minvalid command\n\033[0m");
}

void do_cd()
{
	FileMeta attr;
	char oldPath[PATH_LEN] = {0};
	int i;
	strcpy(oldPath, path);
    if(workPath[0] == '\0') /* to the default path */
    {
        path[0] = '/';
    }
    else if(strcmp(workPath, "..") == 0) /* to the father directory */
    {
        i = PATH_LEN - 1;
        while(path[i] != '/')
        {
        	path[i] = '\0';
        	i--;
        }
        path[i] = '\0';
        if(path[0] == '\0')
        path[0] = '/';
    }
    else if(workPath[0] != '/') /* relative path */
    {
    	if(path[strlen(path)-1] != '/')
    		strcat(path, "/");
    	strcat(path, workPath);
    }
    else if(workPath[0] == '/')
    {
    	memset(path, '\0', PATH_LEN);
    	strcpy(path, workPath);
    }
    if(nrfsGetAttribute(fs, (nrfsFile)path, &attr))
    {
    	printf("invalid path\n");
    	strcpy(path, oldPath);
    }
}
void do_ls()
{
	nrfsfilelist list;
	uint32_t i;
	nrfsListDirectory(fs, path, &list);
	for(i = 0; i < list.count; i++)
	{
		if(list.tuple[i].isDirectories == 0) /* file */
			printf("%s\t", list.tuple[i].names);
		else
			printf("\033[0;34m%s\t\033[0m", list.tuple[i].names);
	}
	printf("\n");
}
void do_touch()
{
	char temp[PATH_LEN] = {0};
	strcpy(temp, path);
	if(strcmp(path, "/") != 0)
		strcat(temp, "/");
	strcat(temp, workPath);
	nrfsFile file = nrfsOpenFile(fs, temp, O_CREAT);
	nrfsCloseFile(fs, file);
}
void do_mkdir()
{
	char temp[PATH_LEN] = {0};
	strcpy(temp, path);
	strcat(temp, "/");
	strcat(temp, workPath);
	nrfsCreateDirectory(fs, temp);
}
void do_write()
{
	char temp[PATH_LEN] = {0};
	char value[1000], ch;
	int i = 0;
	strcpy(temp, path);
	if(strcmp(path, "/") != 0)
		strcat(temp, "/");
	strcat(temp, workPath);
	printf("input for file write:\n");
	while((ch = getchar()) != '\n')
		value[i++] = ch;
	nrfsFile file = nrfsOpenFile(fs, temp, O_CREAT);
	nrfsWrite(fs, file, value, i, 0);
	nrfsCloseFile(fs, file);
}
void do_cat()
{
	char temp[PATH_LEN] = {0};
	char value[1000];
	int i;
	FileMeta attr;
	strcpy(temp, path);
	if(strcmp(path, "/") != 0)
		strcat(temp, "/");
	strcat(temp, workPath);
	nrfsFile file = nrfsOpenFile(fs, temp, 0);
	nrfsGetAttribute(fs, file, &attr);
	if(attr.size == 0)
		return;
	nrfsRead(fs, file, value, attr.size, 0);
	nrfsCloseFile(fs, file);
	for(i = 0; i < (int)attr.size; i++)
		printf("%c", value[i]);
	printf("\n");
}
void do_rm()
{
	char temp[PATH_LEN] = {0};
	strcpy(temp, path);
	if(strcmp(path, "/") != 0)
		strcat(temp, "/");
	strcat(temp, workPath);
	nrfsDelete(fs, temp);
}
void do_clear()
{
	printf("\033c");
}

void do_mv()
{
    char pathOld[PATH_LEN] = {0}, pathNew[PATH_LEN] = {0};
    char newName[25] = {0}, ch;
    int i = 0;
    strcpy(pathOld, path);
    if(strcmp(path, "/") != 0)
        strcat(pathOld, "/");
    strcat(pathOld, workPath);
    printf("input for new file name:\n");
    while((ch = getchar()) != '\n')
        newName[i++] = ch;
    strcpy(pathNew, path);
    if(strcmp(path, "/") != 0)
        strcat(pathNew, "/");
    strcat(pathNew, newName);
    nrfsRename(fs, pathOld, pathNew);
}

void do_pwd()
{
    printf("%s\n", path);
}