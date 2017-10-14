#include "global.h"
#include "debug.hpp"
#include <atomic>
#include <mutex>
#include <random>
#include <thread>
#include "nrfs.h"
#include "RPCClient.hpp"
#include "storage.hpp"

using namespace std;
mutex key_m;
uint64_t key;
atomic<bool> isConnected;
RPCClient *client;
uint64_t DmfsDataOffset;

struct  timeval start1, end1;
uint64_t diff;
uint64_t WriteTime1 = 0, WriteTime2 = 0, WriteTime3 = 0, WriteTime4 = 0, ReadTime1 = 0, ReadTime2 = 0, ReadTime3 = 0, ReadTime4 = 0;
uint16_t get_node_id_by_path(char* path)
{
	UniqueHash hashUnique;
	HashTable::getUniqueHash(path, strlen(path), &hashUnique);
	return ((hashUnique.value[3] % client->getConfInstance()->getServerCount()) + 1);
}

bool sendMessage(uint16_t node_id, void* sendBuffer, long unsigned int sendLength,
								   void* recvBuffer, long unsigned int recvLength)
{
	Debug::debugItem("sendMessage: dst node id: %d", node_id);
	/* one request per time */
	return client->RdmaCall(node_id, (char*)sendBuffer, (uint64_t)sendLength,
							  (char*)recvBuffer, (uint64_t)recvLength);
}

void correct(const char *old_path, char *new_path)
{
	int i, count = 0;
	int len = strlen(old_path);
	for (i = 0; i < len; i++) {
		if ((old_path[i] == '/') && (old_path[i + 1] == '/'))
			continue;
		new_path[count++] = old_path[i];
	}
	if (new_path[count - 1] == '/') {
		new_path[count - 1] = '\0';
	} else {
		new_path[count] = '\0';
	}
	if(new_path[0] == '\0')
	{
		new_path[0] = '/';
		new_path[1] = '\0';
	}
}

/* Get parent directory. 
   Examples:    "/parent/file" -> "/parent" return true
                "/file"        -> "/" return true
                "/"            -> return false
   @param   path    Path.
   @param   parent  Buffer to hold parent path.
   @return          If succeed return true, otherwise return false. */
bool getParentDirectory(const char *path, char *parent) { /* Assume path is valid. */
    if ((path == NULL) || (parent == NULL)) { /* Actually there is no need to check. */
        return false;
    } else {
        strcpy(parent, path);           /* Copy path to parent buffer. Though it is better to use strncpy later but current method is simpler. */
        uint64_t lengthPath = strlen(path); /* FIXME: Might cause memory leak. */
        if ((lengthPath == 1) && (path[0] == '/')) { /* Actually there is no need to check '/' if path is assumed valid. */
            return false;               /* Fail due to root directory has no parent. */
        } else {
            bool resultCut = false;
            for (int i = lengthPath - 1; i >= 0; i--) { /* Limit in range of signed integer. */
                if (path[i] == '/') {
                    parent[i] = '\0';   /* Cut string. */
                    resultCut = true;
                    break;
                } 
            }
            if (resultCut == false) {
                return false;           /* There is no '/' in string. It is an extra check. */
            } else {
                if (parent[0] == '\0') { /* If format is '/path' which contains only one '/' then parent is '/'. */
                    parent[0] = '/';
                    parent[1] = '\0';
                    return true;        /* Succeed. Parent is root directory. */ 
                } else {
                    return true;        /* Succeed. */
                }
            }
        }
    }
}

/* Get file name from path. 
   Examples:    '/parent/file' -> 'file' return true
                '/file'        -> 'file' return true
                '/'            -> return false
   @param   path    Path.
   @param   name    Buffer to hold file name.
   @return          If succeed return true, otherwise return false. */
bool getNameFromPath(const char *path, char *name) { /* Assume path is valid. */
    if ((path == NULL) || (name == NULL)) { /* Actually there is no need to check. */
        return false;
    } else {
        uint64_t lengthPath = strlen(path); /* FIXME: Might cause memory leak. */
        if ((lengthPath == 1) && (path[0] == '/')) { /* Actually there is no need to check '/' if path is assumed valid. */
            return false;               /* Fail due to root directory has no parent. */
        } else {
            bool resultCut = false;
            int i;
            for (i = lengthPath - 1; i >= 0; i--) { /* Limit in range of signed integer. */
                if (path[i] == '/') {
                    resultCut = true;
                    break;
                }
            }
            if (resultCut == false) {
                return false;           /* There is no '/' in string. It is an extra check. */
            } else {
                strcpy(name, &path[i + 1]); /* Copy name to name buffer. path[i] == '/'. Though it is better to use strncpy later but current method is simpler. */
                return true;
            }
        }
    }
}

/**
*nrfsConnect - Connect to a nrfs file system
*@param host A string containing a ip address of native machine,
*if you want to connect to local filesystem, host should be passed as "default".
*@param port default is 10086.
*@param size shared memory size
*return client node_id
**/
nrfs nrfsConnect(const char* host, int port, int size)
{
	Debug::debugTitle("nrfsConnect");
    client = new RPCClient();
    DmfsDataOffset =  CLIENT_MESSAGE_SIZE * MAX_CLIENT_NUMBER;
	DmfsDataOffset += SERVER_MASSAGE_SIZE * SERVER_MASSAGE_NUM * client->getConfInstance()->getServerCount();
    DmfsDataOffset += METADATA_SIZE;
    printf("FileMetaSize = %ld, DirMetaSize = %ld\n", sizeof(FileMeta), sizeof(DirectoryMeta));
    usleep(100000);
	return (nrfs)0;
}

/**
*nrfsDisconnect - Disconnect to a nrfs file system
*@param fs the configured filesystem handle.
*return 0 on success, -1 on error.
*FIXME. More details are needed
**/

int nrfsDisconnect(nrfs fs)
{
	Debug::debugTitle("nrfsDisconnect");
	GeneralSendBuffer sendBuffer;
	GeneralReceiveBuffer receiveBuffer;
	sendBuffer.message = MESSAGE_DISCONNECT;
	for(int i = 1; i <= client->getConfInstance()->getServerCount(); i++)
	{
		sendMessage(i, &sendBuffer, sizeof(GeneralSendBuffer),
					&receiveBuffer, sizeof(GeneralReceiveBuffer));
	}
	isConnected.store(false);
	// client->getRdmaSocketInstance()->NotifyPerformance();
	// Debug::notifyInfo("WriteTime1 =  %d, WriteTime2  = %d, WriteTime3 = %d WriteTime4 = %d", 
	// 	WriteTime1, WriteTime2, WriteTime3, WriteTime4);
	// Debug::notifyInfo("ReadTime1 =  %d, ReadTime2  = %d, ReadTime3 = %d, ReadTime4 = %d\n", 
	// 	ReadTime1, ReadTime2, ReadTime3, ReadTime4);
	return 0;
}

int nrfsAddMetaToDirectory(nrfs fs, char* parent, char* name, bool isDirectory)
{
    Debug::debugTitle("nrfsAddMetaToDirectory");
    int result = 0;
    AddMetaToDirectorySendBuffer bufferAddMetaToDirectorySend; /* Send buffer. */
    bufferAddMetaToDirectorySend.message = MESSAGE_ADDMETATODIRECTORY; /* Assign message type. */
    strcpy(bufferAddMetaToDirectorySend.path, parent);  /* Assign path. */
    strcpy(bufferAddMetaToDirectorySend.name, name);  /* Assign name. */
    bufferAddMetaToDirectorySend.isDirectory = isDirectory; /* Assign state of directory. */

    uint16_t hashNode = get_node_id_by_path(parent);
    
    UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
    
    if (sendMessage(hashNode,
                    &bufferAddMetaToDirectorySend, 
                    sizeof(AddMetaToDirectorySendBuffer), 
                    &bufferGeneralReceive, 
                    sizeof(UpdataDirectoryMetaReceiveBuffer)) == false) {
        result = 1;               /* Fail due to send message error. */
    } else {
        if (bufferGeneralReceive.result == false) {
            result = 1;           /* Fail due to remote function returns false. */
        } else {
        	DoRemoteCommitSendBuffer bufferSend;
            strcpy(bufferSend.path, parent);
            bufferSend.message = MESSAGE_DOCOMMIT;
            bufferSend.TxID = bufferGeneralReceive.TxID;
            bufferSend.srcBuffer = bufferGeneralReceive.srcBuffer;
            bufferSend.desBuffer = bufferGeneralReceive.desBuffer;
            bufferSend.size = bufferGeneralReceive.size;
            bufferSend.key = bufferGeneralReceive.key;
            bufferSend.offset = bufferGeneralReceive.offset;
            GeneralReceiveBuffer bufferReceive;
            if (sendMessage(hashNode,
                    &bufferSend, 
                    sizeof(DoRemoteCommitSendBuffer), 
                    &bufferReceive, 
                    sizeof(GeneralReceiveBuffer)) == false) {
            	Debug::notifyError("Do commit failed.");
		        result = 1;               /* Fail due to send message error. */
		    } else {
		    	Debug::debugItem("Do commit success.");
            	if (bufferReceive.result) {            /* Succeed due to remote function returns true. */ 
		    		return 0;
		    	} else {
		    		return 1;
		    	}
		    }
        }
    }
    return result;
}

int nrfsRemoveMetaFromDirectory(nrfs fs, char* path, char* name)
{
	Debug::debugTitle("nrfsRemoveMetaFromDirectory");
    int result = 0;
    RemoveMetaFromDirectorySendBuffer bufferRemoveMetaFromDirectorySend; /* Send buffer. */
    bufferRemoveMetaFromDirectorySend.message = MESSAGE_REMOVEMETAFROMDIRECTORY; /* Assign message type. */
    strcpy(bufferRemoveMetaFromDirectorySend.path, path);  /* Assign path. */
    strcpy(bufferRemoveMetaFromDirectorySend.name, name);  /* Assign name. */

    uint16_t hashNode = get_node_id_by_path(path);

    UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
    if (sendMessage(hashNode, 
                    &bufferRemoveMetaFromDirectorySend, 
                    sizeof(RemoveMetaFromDirectorySendBuffer), 
                    &bufferGeneralReceive, 
                    sizeof(UpdataDirectoryMetaReceiveBuffer)) == false) {
        result = 1;               /* Fail due to send message error. */
    } else {
        if (bufferGeneralReceive.result == false) {
        	Debug::debugItem("result == false");
            result = 1;            /* Fail due to remote function returns false. */
        } else {
            result = 0;            /* Succeed due to remote function returns true. */ 
        }
    }
    return result;
}

int nrfsMknodWithMeta(nrfs fs, char *path, FileMeta *metaFile)
{
	Debug::debugTitle("nrfsMknodWithMeta");
	/* The path has already formated. */
	int result;
	char *parent = (char *)malloc(strlen(path) + 1);
	char *name = (char *)malloc(strlen(path) + 1);
	if(getParentDirectory(path, parent) == false)
		result = 1;
	else
	{
		FileMeta meta;
		if(nrfsGetAttribute(fs, parent, &meta))
			result = 1;
		else
		{
			if(meta.count != MAX_FILE_EXTENT_COUNT)
				result = 1;
			else
			{
	            MakeNodeWithMetaSendBuffer bufferMakeNodeWithMetaSend; /* Send buffer. */
	            bufferMakeNodeWithMetaSend.message = MESSAGE_MKNODWITHMETA; /* Assign message type. */
	            strcpy(bufferMakeNodeWithMetaSend.path, path); /* Assign path. */
	            bufferMakeNodeWithMetaSend.metaFile = *metaFile; /* Assign file meta. */

				uint16_t hashNode = get_node_id_by_path(path);

	            GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
	            if (sendMessage(hashNode, 
	                            &bufferMakeNodeWithMetaSend, 
	                            sizeof(MakeNodeWithMetaSendBuffer), 
	                            &bufferGeneralReceive, 
	                            sizeof(GeneralReceiveBuffer)) == false) {
	                result = 1;           /* Fail due to send message error. */
	            } else {
	                if (bufferGeneralReceive.result == false) {
	                    result = 1;       /* Fail due to remote function returns false. */
	                } else {
	                	getNameFromPath(path, name);
	                	if(nrfsAddMetaToDirectory(fs, parent, name, false))
	                	{
	                		Debug::notifyError("nrfsAddMetaToDirectory failed.");
	                		result = 1;
	                	}
	                	else
	                    	result = 0;        /* Succeed due to remote function returns true. */ 
	                }
	            }
			}
		}
	}
	return result;
	free(parent);
	free(name);
}

/**
*nrfsOpenFile - Open a nrfs file.
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @param flags - an | of bits - supported flags are O_RDONLY, O_WRONLY, O_RDWR...
* @return Returns the handle to the open file or NULL on error.
**/
nrfsFile nrfsOpenFile(nrfs fs, const char* _path, int flags)
{
	Debug::debugTitle("nrfsOpenFile");
	char *file;
	int result;
	result = nrfsAccess(fs, _path);
	if((flags & O_CREAT) && result)	/* Access falied and no creation */
	{
		result = nrfsMknod(fs, _path);
	}
	if(result == 0)
	{
		file = (char*)malloc(MAX_FILE_NAME_LENGTH);
		correct(_path, file);
		return (nrfsFile)file;
	}
	else
		return NULL;
}


/**
*nrfsMknod - create a file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsMknod(nrfs fs, const char* _path)
{
	Debug::debugTitle("nrfsMknod");
	Debug::debugItem("nrfsMknod: %s", _path);
	GeneralSendBuffer sendBuffer;
	GeneralReceiveBuffer receiveBuffer;
	sendBuffer.message = MESSAGE_MKNOD;
	int result;

	correct(_path, sendBuffer.path);
	uint16_t node_id  = get_node_id_by_path(sendBuffer.path);
	sendMessage(node_id, &sendBuffer, sizeof(GeneralSendBuffer), 
		&receiveBuffer, sizeof(GeneralReceiveBuffer));
	if(receiveBuffer.result == false) {
		result = 1;
	} else {
		result = 0;
	}
	return result;
}

/**
*nrfsCloseFile - Close an open file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsCloseFile(nrfs fs, nrfsFile file)
{
	Debug::debugTitle("nrfsCloseFile");
	Debug::debugItem("nrfsCloseFile: %s", file);
	/* always success */
	//free(file);
	return 0;
}

/**
*nrfsGetAttribute - Get the attribute of the file. 
* @param fs The configured filesystem handle.
* @param file The full path to the file.
* @return Returns 0 on success, -1 on error.
**/
int nrfsGetAttribute(nrfs fs, nrfsFile _file, FileMeta *attr)
{
	Debug::debugTitle("nrfsGetAttribute");
	Debug::debugItem("nrfsGetAttribute: %s", _file);
	GeneralSendBuffer bufferGeneralSend; /* Send buffer. */
    bufferGeneralSend.message = MESSAGE_GETATTR;

    correct((char*)_file, bufferGeneralSend.path);

    uint16_t node_id = get_node_id_by_path(bufferGeneralSend.path);

    GetAttributeReceiveBuffer bufferGetAttributeReceive;
    
    sendMessage(node_id, &bufferGeneralSend, sizeof(GeneralSendBuffer), 
					&bufferGetAttributeReceive, sizeof(GetAttributeReceiveBuffer));
	
	*attr = bufferGetAttributeReceive.attribute;
	Debug::debugItem("nrfsGetAttribute, META.size = %d", attr->size);
	if(bufferGetAttributeReceive.result)
		return 0;
	else
		return -1;
}

/**
*nrfsAccess - access an file. 
* @param fs The configured filesystem handle.
* @param path The full path to the file.
* @return Returns 0 on success, -1 on error.  
**/
int nrfsAccess(nrfs fs, const char* _path)
{
	Debug::debugTitle("nrfsAccess");
	Debug::debugItem("nrfsAccess: %s", _path);
	GeneralSendBuffer sendBuffer;
	GeneralReceiveBuffer receiveBuffer;
	
	sendBuffer.message = MESSAGE_ACCESS;

	correct(_path, sendBuffer.path);
	uint16_t node_id = get_node_id_by_path(sendBuffer.path);
	
	sendMessage(node_id, &sendBuffer, sizeof(GeneralSendBuffer), 
					&receiveBuffer, sizeof(GeneralReceiveBuffer));
	if(receiveBuffer.result)
		return 0;
	else
		return -1;
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
int nrfsWrite(nrfs fs, nrfsFile _file, const void* buffer, uint64_t size, uint64_t offset)
{
	Debug::debugTitle("nrfsWrite");
	Debug::debugItem("Write size: %lx, offset: %lx", size, offset);
	
	gettimeofday(&start1, NULL);

	file_pos_info fpi;
	uint64_t length_copied = 0;
    ExtentWriteSendBuffer bufferExtentWriteSend; /* Send buffer. */
    ExtentWriteReceiveBuffer bufferExtentWriteReceive;

    bufferExtentWriteSend.message = MESSAGE_EXTENTWRITE; /* Assign message type. */
    
    correct((char*)_file, bufferExtentWriteSend.path);
	uint16_t node_id = get_node_id_by_path(bufferExtentWriteSend.path);
    
    bufferExtentWriteSend.size = size; /* Assign size. */
    bufferExtentWriteSend.offset = offset; /* Assign offset. */
	
	gettimeofday(&end1, NULL);
	diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
	WriteTime1 += diff;

	gettimeofday(&start1, NULL);
	sendMessage(node_id, &bufferExtentWriteSend, sizeof(ExtentWriteSendBuffer), 
					&bufferExtentWriteReceive, sizeof(ExtentWriteReceiveBuffer));
	gettimeofday(&end1, NULL);
	diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
	WriteTime2 += diff;

	if(bufferExtentWriteReceive.result == true) {
		fpi = bufferExtentWriteReceive.fpi;
		gettimeofday(&start1, NULL);
		for(int i = 0; i < (int)fpi.len; i++)
		{
			Debug::debugItem("fpi: i = %d, node_id = %d,offset = %x, size = %d", 
				i, fpi.tuple[i].node_id, fpi.tuple[i].offset, fpi.tuple[i].size);
			if(/*fpi.node_id[i] == (uint16_t)fs*/0)
			{
				// memcpy((void*)(mem.get_storage_addr() + fpi.offset[i]),
				// 	   (void*)((char*)buffer + length_copied), 
				// 	   fpi.size[i]);
			}
			else
			{
				client->getRdmaSocketInstance()->RemoteWrite((uint64_t)((char*)buffer + length_copied), 
					              fpi.tuple[i].node_id,
                                  fpi.tuple[i].offset + DmfsDataOffset,
                                  fpi.tuple[i].size);
			}
			length_copied += fpi.tuple[i].size;
		}
		gettimeofday(&end1, NULL);
		diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
		WriteTime3 += diff;

		gettimeofday(&start1, NULL);
		UpdateMetaSendBuffer bufferUpdateMetaSend; /* Send buffer. */
        bufferUpdateMetaSend.message = MESSAGE_UPDATEMETA; /* Assign message type. */
		bufferUpdateMetaSend.key = bufferExtentWriteReceive.key; /* Assign key. */
		bufferUpdateMetaSend.offset = bufferExtentWriteReceive.offset;
        GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */

		sendMessage(node_id, &bufferUpdateMetaSend, sizeof(UpdateMetaSendBuffer), 
					&bufferGeneralReceive, sizeof(GeneralReceiveBuffer));
		gettimeofday(&end1, NULL);
		diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
		WriteTime4 += diff;
		if(bufferGeneralReceive.result == true)
			return (int)length_copied;
		else
			return 1;
	} else {
		return -1;
	}
	return (int)length_copied;
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
int nrfsRead(nrfs fs, nrfsFile _file, void* buffer, uint64_t size, uint64_t offset)
{
	Debug::debugTitle("nrfsRead");
	Debug::debugCur("size: %d, offset: %d", size, offset);

	gettimeofday(&start1, NULL);
	file_pos_info fpi;
	uint64_t length_copied = 0;
    ExtentReadSendBuffer bufferExtentReadSend; /* Send buffer. */
    ExtentReadReceiveBuffer bufferExtentReadReceive;

    bufferExtentReadSend.message = MESSAGE_EXTENTREAD;
    
    correct((char*)_file, bufferExtentReadSend.path);
	uint16_t node_id = get_node_id_by_path(bufferExtentReadSend.path);
    
    bufferExtentReadSend.size = size; /* Assign size. */
    bufferExtentReadSend.offset = offset; /* Assign offset. */

	gettimeofday(&end1, NULL);
	diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
	ReadTime1 += diff;
	gettimeofday(&start1, NULL);
	sendMessage(node_id, &bufferExtentReadSend, sizeof(ExtentReadSendBuffer), 
					&bufferExtentReadReceive, sizeof(ExtentReadReceiveBuffer));
	gettimeofday(&end1, NULL);
	diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
	ReadTime2 += diff;
	if(bufferExtentReadReceive.result == true) {
		fpi = bufferExtentReadReceive.fpi;
		gettimeofday(&start1, NULL);
		for(int i = 0; i < (int)fpi.len; i++)
		{
			Debug::debugItem("fpi: i = %d, node_id = %d,offset = %x, size = %d", 
				i, fpi.tuple[i].node_id, fpi.tuple[i].offset, fpi.tuple[i].size);
			if(/*fpi.node_id[i] == (uint16_t)fs*/0)
			{
				// memcpy((void*)((char*)buffer + length_copied), 
				// 	   (void*)(mem.get_storage_addr() + fpi.offset[i]),
				// 	   fpi.size[i]);
			}
			else
			{
				client->getRdmaSocketInstance()->RemoteRead((uint64_t)((char*)buffer + length_copied), 
					              fpi.tuple[i].node_id,
                                  fpi.tuple[i].offset + DmfsDataOffset, 
                                  fpi.tuple[i].size);
			}
			length_copied += fpi.tuple[i].size;
		}
		// net.read_unlock(node_id, bufferExtentReadReceive.key, bufferExtentReadReceive.offset);
		gettimeofday(&end1, NULL);
		diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
		ReadTime3 += diff;
		gettimeofday(&start1, NULL);
		ExtentReadEndSendBuffer bufferExtentReadEndSend; /* Send buffer. */
        bufferExtentReadEndSend.message = MESSAGE_EXTENTREADEND; /* Assign message type. */
        bufferExtentReadEndSend.key = bufferExtentReadReceive.key;  /* Assign key. */
		bufferExtentReadEndSend.offset = bufferExtentReadReceive.offset;
        GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */

		sendMessage(node_id, &bufferExtentReadEndSend, sizeof(ExtentReadEndSendBuffer), 
					&bufferGeneralReceive, sizeof(GeneralReceiveBuffer));
		gettimeofday(&end1, NULL);
		diff = 1000000 * (end1.tv_sec - start1.tv_sec) + end1.tv_usec - start1.tv_usec;
		ReadTime4 += diff;
		return (int)length_copied;
	} else {
		return -1;
	}
}

/** 
* nrfsCreateDirectory - Make the given file and all non-existent
* parents into directories.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
#define RECURSIVE_CREATE 1
#ifdef RECURSIVE_CREATE
int nrfsCreateDirectory(nrfs fs, const char* _path)
{
	Debug::debugTitle("nrfsCreateDirectory");
	Debug::debugItem("nrfsCreateDirctory, path = %s", _path);
	GeneralSendBuffer bufferGeneralSend; /* Send buffer. */
	char path[255];
	memset(path, '\0', 255);
	int result, pos[20], count = 0;
	correct(_path, path);
	int len = strlen(path);
	Debug::debugItem("Path = %s, length = %d, ", path, len);
	for (int i = 1; i < len; i++) {
		if (path[i] == '/') {
			pos[count] = i;
			count += 1;
		}
	}
	pos[count] = len;
	count += 1;
	for (int i = 0; i < count; i++) {
		memcpy(bufferGeneralSend.path, path, pos[i]);
		bufferGeneralSend.path[pos[i]] = '\0';
		Debug::debugItem("Recursively access parent directory, path = %s", bufferGeneralSend.path);
		if (nrfsAccess(fs, bufferGeneralSend.path) == 0) {
			continue; /* The directory already exist. */
		}
		bufferGeneralSend.message = MESSAGE_MKDIR; /* Assign message type. */
		
		GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
		uint16_t node_id = get_node_id_by_path(bufferGeneralSend.path);
		sendMessage(node_id, &bufferGeneralSend, sizeof(GeneralSendBuffer), 
			&bufferGeneralReceive, sizeof(GeneralReceiveBuffer));
		if(bufferGeneralReceive.result == false) {
			return 1;
		} else {
			result = 0;
		}
	}
	return result;
}
#else
int nrfsCreateDirectory(nrfs fs, const char* _path)
{
	Debug::debugTitle("nrfsCreateDirectory");
	GeneralSendBuffer bufferGeneralSend; /* Send buffer. */
    bufferGeneralSend.message = MESSAGE_MKDIR; /* Assign message type. */
	int result;

    GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */

	correct(_path, bufferGeneralSend.path);
	uint16_t node_id = get_node_id_by_path(bufferGeneralSend.path);
	sendMessage(node_id, &bufferGeneralSend, sizeof(GeneralSendBuffer), 
		&bufferGeneralReceive, sizeof(GeneralReceiveBuffer));
	if(bufferGeneralReceive.result == false)
		result = 1;
	else
	{
		result = 0;
	}
	return result;
}
#endif
/** 
* nrfsDelete - Delete file or directory.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns 0 on success, -1 on error. 
*/
int nrfsDelete(nrfs fs, const char* _path)
{
	Debug::debugTitle("nrfsDelete");
	int result = 0;

	GeneralSendBuffer bufferGeneralSend; /* Send buffer. */
    bufferGeneralSend.message = MESSAGE_REMOVE; /* Assign message type. */

    GetAttributeReceiveBuffer bufferReceive; /* Receive buffer. */

	correct(_path, bufferGeneralSend.path);
	uint16_t node_id = get_node_id_by_path(bufferGeneralSend.path);

	sendMessage(node_id, &bufferGeneralSend, sizeof(GeneralSendBuffer), 
					&bufferReceive, sizeof(GetAttributeReceiveBuffer));
	if (bufferReceive.result == false) {
		Debug::notifyError("Remove file failed.");
		result = 1;
	} else if (bufferReceive.attribute.count != MAX_FILE_EXTENT_COUNT) {
		for (uint64_t i = 0; i < bufferReceive.attribute.count; i++) {
			if (((uint16_t)(bufferReceive.attribute.tuple[i].hashNode) != node_id) && (bufferReceive.attribute.tuple[i].hashNode != 0)) {
				nrfsFreeBlock((uint16_t)(bufferReceive.attribute.tuple[i].hashNode),
						bufferReceive.attribute.tuple[i].indexExtentStartBlock, 
						bufferReceive.attribute.tuple[i].countExtentBlock);
			}
		}
		result = 0;
	}
	return result;
}
int nrfsFreeBlock(uint16_t nodeHash, uint64_t startBlock, uint64_t countBlock)
{
	BlockFreeSendBuffer bufferSend;
	GeneralReceiveBuffer bufferReceive;
	bufferSend.message = MESSAGE_FREEBLOCK;
	bufferSend.startBlock = startBlock;
	bufferSend.countBlock = countBlock;
	sendMessage(nodeHash, &bufferSend, sizeof(BlockFreeSendBuffer), 
					&bufferReceive, sizeof(GeneralReceiveBuffer));
	if(bufferReceive.result == false) {
		return 1;
	} else {
		return 0;
	}
}
int renameDirectory(nrfs fs, const char *oldPath, const char *newPath)
{
	nrfsfilelist list;
	FileMeta attr;
	uint32_t i;
	int result = 0;
	char tempoldPath[MAX_PATH_LENGTH];
	char tempnewPath[MAX_PATH_LENGTH];
	result |= nrfsCreateDirectory(fs, newPath);
	nrfsListDirectory(fs, oldPath, &list);
	for(i = 0; i < list.count; i++)
	{
		memset(tempoldPath, '\0', MAX_PATH_LENGTH);
		memset(tempnewPath, '\0', MAX_PATH_LENGTH);
		strcpy(tempoldPath, oldPath);
		strcpy(tempnewPath, newPath);
		strcat(tempoldPath, "/");
		strcat(tempnewPath, "/");
		strcat(tempoldPath, list.tuple[i].names);
		strcat(tempnewPath, list.tuple[i].names);
		result |= nrfsGetAttribute(fs, tempoldPath, &attr);
		if(attr.count == MAX_FILE_EXTENT_COUNT)
		{
			result |= renameDirectory(fs, tempoldPath, tempnewPath);
		}
		else
		{
			result |= nrfsRename(fs, tempoldPath, tempnewPath);
		}
		if(result)
			break;
	}
	result |= nrfsDelete(fs, oldPath);
	return 0;
}
/**
* nrfsRename - Rename file. 
* @param fs The configured filesystem handle.
* @param oldPath The path of the source file. 
* @param newPath The path of the destination file. 
* @return Returns 0 on success, -1 on error. 
*/
int nrfsRename(nrfs fs, const char* _oldpath, const char* _newpath)
{
	Debug::debugTitle("nrfsRename");
	int result;

	RenameSendBuffer bufferRenameSend; /* Send buffer. */
    bufferRenameSend.message = MESSAGE_RENAME; /* Assign message type. */

    GeneralReceiveBuffer bufferGeneralReceive; /* Receive buffer. */

	correct(_oldpath, bufferRenameSend.pathOld);
	correct(_newpath, bufferRenameSend.pathNew);

	FileMeta meta;
	if(nrfsGetAttribute(fs, bufferRenameSend.pathOld, &meta))
		result = 1;
	else
	{
		if(meta.count == MAX_FILE_EXTENT_COUNT)
		{
			result = renameDirectory(fs, _oldpath, _newpath);
			return result;
		}
		/* Rename for a directory is not implemented */

		if(nrfsMknodWithMeta(fs, bufferRenameSend.pathNew, &meta))
		{
			Debug::notifyError("nrfsMknodWithMeta failed.");
			result = 1;
		}
		else
		{
			uint16_t node_id = get_node_id_by_path(bufferRenameSend.pathOld);

			sendMessage(node_id, &bufferRenameSend, sizeof(RenameSendBuffer), 
							&bufferGeneralReceive, sizeof(GeneralReceiveBuffer));
			if(bufferGeneralReceive.result == false)
			{
				result = 1;
			}
			else
			{
				result = 0;
			}
		}
	}
	return result;
}

/** 
* nrfsListDirectory - Get list of files/directories for a given
* directory-path.
* @param fs The configured filesystem handle.
* @param path The path of the directory.
* @return Returns the number of the entries or -1 if the path not exsit.
*/
int nrfsListDirectory(nrfs fs, const char* _path, nrfsfilelist *list)
{
	Debug::debugTitle("nrfsListDirectory");
	GeneralSendBuffer bufferGeneralSend; /* Send buffer. */
    bufferGeneralSend.message = MESSAGE_READDIR; /* Assign message type. */

    ReadDirectoryReceiveBuffer bufferReadDirectoryReceive; /* Receive buffer. */

	correct(_path, bufferGeneralSend.path);
	
	uint16_t node_id = get_node_id_by_path(bufferGeneralSend.path);

	sendMessage(node_id, &bufferGeneralSend, sizeof(GeneralSendBuffer), 
					&bufferReadDirectoryReceive, sizeof(ReadDirectoryReceiveBuffer));
	*list = bufferReadDirectoryReceive.list;
	if(bufferReadDirectoryReceive.result)
		return 0;
	else
		return -1;
}

/**
* for performance test
*/
int nrfsTest(nrfs fs, const int offset)
{
	//_cmd.atomicTest(offset);
	Debug::debugTitle("nrfsTest");
	char _path[] = "/sdafsf";
	GeneralSendBuffer sendBuffer;
	GeneralReceiveBuffer receiveBuffer;
	
	sendBuffer.message = MESSAGE_TEST;

	correct(_path, sendBuffer.path);

	uint16_t node_id = get_node_id_by_path(sendBuffer.path);
	
	sendMessage(node_id, &sendBuffer, sizeof(GeneralSendBuffer),
					&receiveBuffer, sizeof(GeneralReceiveBuffer));
	if(receiveBuffer.result)
		return 0;
	else
		return -1;
	return 0;
}



/***********
for testing:
	-nrfsRawRead
	-nrfsRawWrite
***********/
int nrfsRawWrite(nrfs fs, nrfsFile _file, const void* buffer, uint64_t size, uint64_t offset)
{
	Debug::debugTitle("nrfsRawWrite");
	Debug::debugCur("Write size: %lx, offset: %lx", size, offset);

	//file_pos_info fpi;
	//uint64_t length_copied = 0;
    ExtentWriteSendBuffer bufferExtentWriteSend; /* Send buffer. */
    ExtentWriteReceiveBuffer bufferExtentWriteReceive;

    bufferExtentWriteSend.message = MESSAGE_RAWWRITE; /* Assign message type. */
    
    correct((char*)_file, bufferExtentWriteSend.path);
	uint16_t node_id = get_node_id_by_path(bufferExtentWriteSend.path);
    
	/*
	* Copy data to registered buffer and post remote write.
	*/
	//uint64_t dst_addr = rdma.find_res_by_id(node_id)->remote_props.socket_addr + 1024 * 1024 * 1024 + 4 * 1024 * 1024 * rdma.get_node_id();
	// uint32_t *v = (uint32_t*)(mem.get_storage_addr() + size - sizeof(uint32_t));
	//memcpy((void*)mem.get_storage_addr(), buffer, size);
	// *v = rdma.get_node_id();
	//net.post_write(rdma.find_res_by_id(node_id), size, mem.get_storage_addr(), dst_addr, -1);

    bufferExtentWriteSend.size = size; /* Assign size. */
    bufferExtentWriteSend.offset = offset; /* Assign offset. */
    uint64_t *value = (uint64_t *)(client->mm + 2 * 4096);
	memcpy((void *)(client->mm + 2 * 4096), (void *)buffer, size);
    *value = 1;
	sendMessage(node_id, &bufferExtentWriteSend, sizeof(ExtentWriteSendBuffer),
					&bufferExtentWriteReceive, sizeof(ExtentWriteReceiveBuffer));
	return 0;
}
int nrfsRawRead(nrfs fs, nrfsFile _file, void* buffer, uint64_t size, uint64_t offset)
{
	Debug::debugTitle("nrfsRawRead");
	Debug::debugCur("size: %d, offset: %d", size, offset);
	//file_pos_info fpi;
	//uint64_t length_copied = 0;
    ExtentReadSendBuffer bufferExtentReadSend; /* Send buffer. */
    ExtentReadReceiveBuffer bufferExtentReadReceive;

    bufferExtentReadSend.message = MESSAGE_RAWREAD;
    
    correct((char*)_file, bufferExtentReadSend.path);
	uint16_t node_id = get_node_id_by_path(bufferExtentReadSend.path);
    
    bufferExtentReadSend.size = size; /* Assign size. */
    bufferExtentReadSend.offset = offset; /* Assign offset. */
	uint64_t *value = (uint64_t *)(client->mm + 2 * 4096);
	*value = 0;
	sendMessage(node_id, &bufferExtentReadSend, sizeof(ExtentReadSendBuffer), 
					&bufferExtentReadReceive, sizeof(ExtentReadReceiveBuffer));	
	while (*value == 0);
	memcpy((void *)buffer, (void *)(client->mm + 2 * 4096), size);
	// uint32_t *v = (uint32_t*)(mem.get_storage_addr() + size - sizeof(uint32_t));
	// while(*v != (uint32_t)rdma.get_node_id())
	// 	;
	//uint64_t dst_addr = rdma.find_res_by_id(node_id).remote_props.socket_addr + 1024 * 1024 * 1024 + 4 * 1024 * 1024 * rdma.get_node_id();
	//memcpy(buffer, (void*)mem.get_storage_addr(), size);
	// *v = 0;
	//net.post_write(rdma.find_res_by_id(node_id), size, mem.get_storage_addr(), dst_addr, -1);
	return 0;
}

int nrfsRawRPC(nrfs fs)
{
	//uint16_t NodeID = client->getRdmaSocketInstance()->getNodeID();
	//uint64_t dst_addr = rdma.find_res_by_id(1)->remote_props.socket_addr + 1024 * 1024 * 1024 + 1024 * NodeID;
	// uint32_t *v = (uint32_t*)(mem.get_storage_addr() + size - sizeof(uint32_t));
	//uint64_t src_addr = mem.get_storage_addr();
	// *v = rdma.get_node_id();
	//net.post_write(rdma.find_res_by_id(1), 1024, src_addr, dst_addr, -1);
	return 0;
}
