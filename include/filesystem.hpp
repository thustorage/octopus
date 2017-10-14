/*** File system header. ***/

/** Version 2 + modifications for functional model. **/

/** Redundance check. **/
#ifndef FILESYSTEM_HEADER
#define FILESYSTEM_HEADER

/** Included files. **/
#include <stdint.h>                     /* Standard integers. E.g. uint16_t */
#include <stdlib.h>                     /* Standard library. */
#include <string.h>                     /* String operations. */
#include <stdio.h>                      /* Standard I/O. */
#include <time.h>                       /* Time functions. */
#include <mutex>                        /* Mutex functions. */
#include "storage.hpp"                  /* Storage class, definition of node hash and and hash functions. */
#include "debug.hpp"                    /* Debug class. */
#include "global.h"                     /* Global header. */
#include "lock.h"

/** Classes. **/
class FileSystem
{
private: 
    Storage *storage;                   /* Storage. */
    NodeHash hashLocalNode;             /* Local node hash. */
    LockService *lock;
    uint64_t addressHashTable;
    bool checkLocal(NodeHash hashNode); /* Check if node hash is local. */
    bool getParentDirectory(const char *path, char *parent); /* Get parent directory. */
    bool getNameFromPath(const char *path, char *name); /* Get file name from path. */
    bool sendMessage(NodeHash hashNode, void *bufferSend, uint64_t lengthSend, /* Send message. */
                     void *bufferReceive, uint64_t lengthReceive);
    void fillFilePositionInformation(uint64_t size, uint64_t offset, file_pos_info *fpi, FileMeta *metaFile); /* Fill file position information for read and write. */
    
public:
    void rootInitialize(NodeHash LocalNode);
    /* Internal functions. No parameter check. Must be called by message handler or functions in this class. */
    bool addMetaToDirectory(const char *path, const char *name, bool isDirectory, uint64_t *TxID, 
        uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size, uint64_t *key, uint64_t *offset); /* Internal add meta to directory function. Might cause overhead. */
    bool removeMetaFromDirectory(const char *path, const char *name, 
        uint64_t *TxID, uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size,  uint64_t *key, uint64_t *offset); /* Internal remove meta from directory function. Might cause overhead. */
    bool updateDirectoryMeta(const char *path, uint64_t TxID, uint64_t srcBuffer, 
        uint64_t desBuffer, uint64_t size, uint64_t key, uint64_t offset);
    bool mknodWithMeta(const char *path, FileMeta *metaFile); /* Make node (file) with file meta. */
    /* External functions. */
    void parseMessage(char *bufferRequest, char *bufferResponse); /* Parse message. */
    bool mknod(const char *path);       /* Make node (file). */
    bool mknod2pc(const char *path);
    bool mknodcd(const char *path);
    bool getattr(const char *path, FileMeta *attribute); /* Get attributes. */
    bool access(const char *path);      /* Check accessibility. */
    bool mkdir(const char *path);       /* Make directory. */
    bool mkdir2pc(const char *path);
    bool mkdircd(const char *path);
    bool readdir(const char *path, nrfsfilelist *list); /* Read directory. */
    bool recursivereaddir(const char *path, int depth);
    bool readDirectoryMeta(const char *path, DirectoryMeta *meta, uint64_t *hashAddress, uint64_t *metaAddress, uint16_t *parentNodeID);
    bool extentRead(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key); /* Allocate read extent. */
    bool extentReadEnd(uint64_t key, char* path);
    bool extentWrite(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key); /* Allocate write extent. Unlock is implemented in updateMeta. */
    bool updateMeta(const char *path, FileMeta *metaFile, uint64_t key); /* Update meta. Only unlock path due to lock in extentWrite. */
    bool truncate(const char *path, uint64_t size); /* Truncate. */
    bool remove(const char *path, FileMeta *metaFile);      /* Remove file or empty directory. */
    bool remove2pc(const char *path, FileMeta *metaFile);
    bool removecd(const char *path, FileMeta *metaFile);
    bool blockFree(uint64_t startBlock, uint64_t countBlock);
    bool rmdir(const char *path);       /* Remove directory. */
    bool rename(const char *pathOld, const char *pathNew); /* Rename file. */
    uint64_t lockWriteHashItem(NodeHash hashNode, AddressHash hashAddressIndex); /* Lock hash item for write. */
    void unlockWriteHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddressIndex); /* Unlock hash item. */
    uint64_t lockReadHashItem(NodeHash hashNode, AddressHash hashAddressIndex); /* Lock hash item for read. */
    void unlockReadHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddressIndex); /* Unlock hash item. */
    void updateRemoteMeta(uint16_t parentNodeID, DirectoryMeta *meta, uint64_t parentMetaAddress, uint64_t parentHashAddress);
    FileSystem(char *buffer, char *bufferBlock, uint64_t countFile, /* Constructor of file system. */
               uint64_t countDirectory, uint64_t countBlock, 
               uint64_t countNode, NodeHash hashLocalNode); 
    ~FileSystem();                      /* Destructor of file system. */
};

/** Redundance check. **/
#endif
