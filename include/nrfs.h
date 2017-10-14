/****************************************************************
*
* 
*
* g++ -o nrfs nrfs.cpp test.cpp -std=c++14 -lpthread
*
****************************************************************/
#ifndef NRFS_LIB_H
#define NRFS_LIB_H
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include "common.hpp"
using namespace std;

/**
*nrfsConnect - Connect to a nrfs file system
*@param host A string containing a ip address of native machine,
*if you want to connect to local filesystem, host should be passed as "default".
*@param port default is 10086.
*@param size shared memory size
**/
nrfs nrfsConnect(const char* host, int port, int size);

/**
*nrfsDisconnect - Disconnect to a nrfs file system
*@param fs the configured filesystem handle.
*return 0 on success, -1 on error.
**/
int nrfsDisconnect(nrfs fs);

/**
*nrfsOpenFile - Open a nrfs file.
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @param flags - an | of bits - supported flags are O_RDONLY, O_WRONLY, O_RDWR...
* @return Returns the handle to the open file or NULL on error.
**/
nrfsFile nrfsOpenFile(nrfs fs, const char* path, int flags);

/**
*nrfsCloseFile - Close an open file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsCloseFile(nrfs fs, nrfsFile file);

/**
*nrfsMknod - create a file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsMknod(nrfs fs, const char* path);

/**
*nrfsAccess - access a file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsAccess(nrfs fs, const char* path);

/**
*nrfsGetAttribute - Get the attribute of the file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.
**/
int nrfsGetAttribute(nrfs fs, nrfsFile file, FileMeta *attr);

/**
*nrfsWrite - Write data into an open file.
* @param fs The configured filesystem handle.
* @param file The file handle.
* @param buffer The data.
* @param size The no. of bytes to write. 
* @param offset The offset of the file where to write. 
* @return Returns the number of bytes written, -1 on error. 
**/
int nrfsWrite(nrfs fs, nrfsFile file, const void* buffer, uint64_t size, uint64_t offset);

/**
*nrfsRead - Read data into an open file.
* @param fs The configured filesystem handle.
* @param file The file handle.
* @param buffer The buffer to copy read bytes into.
* @param size The no. of bytes to read. 
* @param offset The offset of the file where to read. 
* @return Returns the number of bytes actually read, -1 on error. 
**/
int nrfsRead(nrfs fs, nrfsFile file, void* buffer, uint64_t size, uint64_t offset);

/** 
* nrfsCreateDirectory - Make the given file and all non-existent
* parents into directories.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
int nrfsCreateDirectory(nrfs fs, const char* path);

/** 
* nrfsDelete - Delete file.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
int nrfsDelete(nrfs fs, const char* path);
int nrfsFreeBlock(uint16_t node_id, uint64_t startBlock, uint64_t  countBlock);
/**
* nrfsRename - Rename file. 
* @param fs The configured filesystem handle.
* @param oldPath The path of the source file. 
* @param newPath The path of the destination file. 
* @return Returns 0 on success, -1 on error. 
*/
int nrfsRename(nrfs fs, const char* oldpath, const char* newpath);

/** 
* nrfsListDirectory - Get list of files/directories for a given
* directory-path.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns the number of the entries or -1 if the path not exsit.
*/
int nrfsListDirectory(nrfs fs, const char* path, nrfsfilelist *list);

/**
* for performance test
*/
int nrfsTest(nrfs fs, int offset);


/**
* Ignore these two function.
**/
int nrfsRawWrite(nrfs fs, nrfsFile file, const void* buffer, uint64_t size, uint64_t offset);
int nrfsRawRead(nrfs fs, nrfsFile file, void* buffer, uint64_t size, uint64_t offset);
int nrfsRawRPC(nrfs fs);
#endif