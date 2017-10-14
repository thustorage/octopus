//#include "libnrfs.h"
#include "common.hpp"
#include "nrfs.h"
#ifdef __cplusplus
extern "C" {
#endif

/**
*nrfsConnect - Connect to a nrfs file system
*@param host A string containing a ip address of native machine,
*if you want to connect to local filesystem, host should be passed as "default".
*@param port default is 10086.
*@param size shared memory size
**/
nrfs libnrfsConnect(const char* host, int port, int size)
{
	return nrfsConnect(host, port, size);
}

/**
*nrfsDisconnect - Disconnect to a nrfs file system
*@param fs the configured filesystem handle.
*return 0 on success, -1 on error.
**/
int libnrfsDisconnect(nrfs fs)
{
	return nrfsDisconnect(fs);
}

/**
*nrfsOpenFile - Open a nrfs file.
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @param flags - an | of bits - supported flags are O_RDONLY, O_WRONLY, O_RDWR...
* @return Returns the handle to the open file or NULL on error.
**/
nrfsFile libnrfsOpenFile(nrfs fs, const char* path, int flags)
{
	return nrfsOpenFile(fs, path, flags);
}

/**
*nrfsCloseFile - Close an open file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int libnrfsCloseFile(nrfs fs, nrfsFile file)
{
	return nrfsCloseFile(fs, file);
}

/**
*nrfsMknod - create a file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int libnrfsMknod(nrfs fs, const char* path)
{
	return nrfsMknod(fs, path);
}

/**
*nrfsAccess - access a file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int libnrfsAccess(nrfs fs, const char* path)
{
	return nrfsAccess(fs, path);
}

/**
*nrfsGetAttribute - Get the attribute of the file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.
**/
int libnrfsGetAttribute(nrfs fs, nrfsFile file, FileMeta *attr)
{
	return nrfsGetAttribute(fs, file, attr);
}

/**
*nrfsWrite - Write data into an open file.
* @param fs The configured filesystem handle.
* @param file The file handle.
* @param buffer The data.
* @param size The no. of bytes to write. 
* @param offset The offset of the file where to write. 
* @return Returns the number of bytes written, -1 on error. 
**/
int libnrfsWrite(nrfs fs, nrfsFile file, const void* buffer, uint64_t size, uint64_t offset)
{
	return nrfsWrite(fs, file, buffer, size, offset);
}

/**
*nrfsRead - Read data into an open file.
* @param fs The configured filesystem handle.
* @param file The file handle.
* @param buffer The buffer to copy read bytes into.
* @param size The no. of bytes to read. 
* @param offset The offset of the file where to read. 
* @return Returns the number of bytes actually read, -1 on error. 
**/
int libnrfsRead(nrfs fs, nrfsFile file, void* buffer, uint64_t size, uint64_t offset)
{
	return nrfsRead(fs, file, buffer, size, offset);
}

/** 
* nrfsCreateDirectory - Make the given file and all non-existent
* parents into directories.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
int libnrfsCreateDirectory(nrfs fs, const char* path)
{
	return nrfsCreateDirectory(fs, path);
}

/** 
* nrfsDelete - Delete file.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
int libnrfsDelete(nrfs fs, const char* path)
{
	return nrfsDelete(fs, path);
}

/**
* nrfsRename - Rename file. 
* @param fs The configured filesystem handle.
* @param oldPath The path of the source file. 
* @param newPath The path of the destination file. 
* @return Returns 0 on success, -1 on error. 
*/
int libnrfsRename(nrfs fs, const char* oldpath, const char* newpath)
{
	return nrfsRename(fs, oldpath, newpath);
}

/** 
* nrfsListDirectory - Get list of files/directories for a given
* directory-path.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns the number of the entries or -1 if the path not exsit.
*/
int libnrfsListDirectory(nrfs fs, const char* path, nrfsfilelist *list)
{
	return nrfsListDirectory(fs, path, list);
}

/**
* for performance test
*/
int libnrfsTest(nrfs fs, int offset)
{
	return nrfsTest(fs, offset);
}

#ifdef __cplusplus
}
#endif