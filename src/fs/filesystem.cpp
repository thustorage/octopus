/*** File system class. ***/

/** Version 2 + modifications for functional model. **/

/** Included files. **/
#include "filesystem.hpp"
#include "RPCServer.hpp"
extern RPCServer *server;
bool Dotx = true;
uint64_t TxLocalBegin() {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->TxLocalBegin();
}

void TxWriteData(uint64_t TxID, uint64_t address, uint64_t size) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxWriteData(TxID, address, size);
}

uint64_t getTxWriteDataAddress(uint64_t TxID) {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->getTxWriteDataAddress(TxID);
}

void TxLocalCommit(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxLocalCommit(TxID, action);
}

uint64_t TxDistributedBegin() {
    if (!Dotx)
        return 0;
    return server->getTxManagerInstance()->TxDistributedBegin();
}

void TxDistributedPrepare(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxDistributedPrepare(TxID, action);
}

void TxDistributedCommit(uint64_t TxID, bool action) {
    if (!Dotx)
        return;
    server->getTxManagerInstance()->TxDistributedCommit(TxID, action);
}

void FileSystem::updateRemoteMeta(uint16_t parentNodeID, DirectoryMeta *meta, uint64_t parentMetaAddress, uint64_t parentHashAddress) {
	Debug::debugTitle("updateRemoteMeta");
	/* Prepare imm data. */
	uint32_t imm, temp;
	/*
	|  12b  |     20b    |
	+-------+------------+
	| 0XFFF | HashAdress |
	+-------+------------+
	*/
	temp = 0XFFF;
	imm = (temp << 20);
	imm += (uint32_t)parentHashAddress;
	/* Remote write with imm. */
	uint64_t SendBuffer;
	server->getMemoryManagerInstance()->getServerSendAddress(parentNodeID, &SendBuffer);
	uint64_t RemoteBuffer = parentMetaAddress;
	Debug::debugItem("imm = %x, SendBuffer = %lx, RemoteBuffer = %lx", imm, SendBuffer, RemoteBuffer);
	if (parentNodeID == server->getRdmaSocketInstance()->getNodeID()) {
		memcpy((void *)(RemoteBuffer + server->getMemoryManagerInstance()->getDmfsBaseAddress()), (void *)meta, sizeof(DirectoryMeta));
		//unlockWriteHashItem(0, parentNodeID, parentHashAddress);
		return;
	}
    uint64_t size = sizeof(DirectoryMeta) - sizeof(DirectoryMetaTuple) * (MAX_DIRECTORY_COUNT - meta->count);
	memcpy((void *)SendBuffer, (void *)meta, size);
    server->getRdmaSocketInstance()->RdmaWrite(parentNodeID, SendBuffer, RemoteBuffer, size, -1, 1);
	server->getRdmaSocketInstance()->RdmaRead(parentNodeID, SendBuffer, RemoteBuffer, size, 1);
	/* Data will be written to the remote address, and lock will be released with the assist of imm data. */
	/* WRITE READ will be send after that, flushing remote data. */
}

void RdmaCall(uint16_t NodeID, char *bufferSend, uint64_t lengthSend, char *bufferReceive, uint64_t lengthReceive) {
    server->getRPCClientInstance()->RdmaCall(NodeID, bufferSend, lengthSend, bufferReceive, lengthReceive);
}

/** Implemented functions. **/ 
/* Check if node hash is local. 
   @param   hashNode    Given node hash.
   @return              If given node hash points to local node return true, otherwise return false. */
bool FileSystem::checkLocal(NodeHash hashNode)
{
    if (hashNode == hashLocalNode) {
        return true;                    /* Succeed. Local node. */
    } else {
        return false;                   /* Succeed. Remote node. */
    }
}

/* Get parent directory. 
   Examples:    "/parent/file" -> "/parent" return true
                "/file"        -> "/" return true
                "/"            -> return false
   @param   path    Path.
   @param   parent  Buffer to hold parent path.
   @return          If succeed return true, otherwise return false. */
bool FileSystem::getParentDirectory(const char *path, char *parent) { /* Assume path is valid. */
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
bool FileSystem::getNameFromPath(const char *path, char *name) { /* Assume path is valid. */
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

/* Lock hash item for write. */
uint64_t FileSystem::lockWriteHashItem(NodeHash hashNode, AddressHash hashAddress)
{
    //NodeHash hashNode = storage->getNodeHash(hashUnique); /* Get node hash. */
    //AddressHash hashAddress = hashUnique->value[0] & 0x00000000000FFFFF; /* Get address hash. */
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before write lock: %lx", *value);
    uint64_t ret = lock->WriteLock((uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after write lock, address: %lx, key: %lx", value, ret);
    return ret;
}

/* Unlock hash item. */
void FileSystem::unlockWriteHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before write unlock: %lx", *value);
    lock->WriteUnlock(key, (uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after write unlock: %lx address = %lx", value, *value);
}

/* Lock hash item for read. */
uint64_t FileSystem::lockReadHashItem(NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before read lock: %lx", *value);
    uint64_t ret = lock->ReadLock((uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after read lock, address: %lx, key: %lx", *value, ret);
    return ret;
}
/* Unlock hash item. */
void FileSystem::unlockReadHashItem(uint64_t key, NodeHash hashNode, AddressHash hashAddress)
{
    uint64_t *value = (uint64_t *)(this->addressHashTable + hashAddress * sizeof(HashItem));
    Debug::debugItem("value before read unlock: %lx", *value);
    lock->ReadUnlock(key, (uint16_t)(hashNode), (uint64_t)(sizeof(HashItem) * hashAddress));
    Debug::debugItem("value after read unlock: %lx", *value);
}

/* Send message. */
bool FileSystem::sendMessage(NodeHash hashNode, void *bufferSend, uint64_t lengthSend, 
    void *bufferReceive, uint64_t lengthReceive)
{
    return true;//return _cmd.sendMessage((uint16_t)hashNode, (char *)bufferSend, lengthSend, (char *)bufferReceive, lengthReceive); /* Actual send message. */
}

/* Parse message. */
void FileSystem::parseMessage(char *bufferRequest, char *bufferResponse) 
{
    /* No check on parameters. */
    GeneralSendBuffer *bufferGeneralSend = (GeneralSendBuffer *)bufferRequest; /* Send and request. */
    GeneralReceiveBuffer *bufferGeneralReceive = (GeneralReceiveBuffer *)bufferResponse; /* Receive and response. */
    bufferGeneralReceive->message = MESSAGE_RESPONSE; /* Fill response message. */
    switch(bufferGeneralSend->message) {
        case MESSAGE_ADDMETATODIRECTORY: 
        {
            AddMetaToDirectorySendBuffer *bufferSend = 
                (AddMetaToDirectorySendBuffer *)bufferGeneralSend;
            UpdataDirectoryMetaReceiveBuffer *bufferReceive = (UpdataDirectoryMetaReceiveBuffer *)bufferResponse;
            bufferReceive->result = addMetaToDirectory(
                bufferSend->path, bufferSend->name, 
                bufferSend->isDirectory, 
                &(bufferReceive->TxID),
                &(bufferReceive->srcBuffer),
                &(bufferReceive->desBuffer),
                &(bufferReceive->size),
                &(bufferReceive->key),
                &(bufferReceive->offset));
            break;
        }
        case MESSAGE_REMOVEMETAFROMDIRECTORY: 
        {
            RemoveMetaFromDirectorySendBuffer *bufferSend = 
                (RemoveMetaFromDirectorySendBuffer *)bufferGeneralSend;
            UpdataDirectoryMetaReceiveBuffer *bufferReceive = (UpdataDirectoryMetaReceiveBuffer *)bufferResponse;
            bufferGeneralReceive->result = removeMetaFromDirectory(
                bufferSend->path, bufferSend->name,
                &(bufferReceive->TxID),
                &(bufferReceive->srcBuffer),
                &(bufferReceive->desBuffer),
                &(bufferReceive->size),
                &(bufferReceive->key),
                &(bufferReceive->offset));
            break;
        }
        case MESSAGE_DOCOMMIT:
        {
            DoRemoteCommitSendBuffer *bufferSend = (DoRemoteCommitSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = updateDirectoryMeta(
                bufferSend->path, bufferSend->TxID, bufferSend->srcBuffer, 
                bufferSend->desBuffer, bufferSend->size, bufferSend->key, bufferSend->offset);
            break;
        }
        case MESSAGE_MKNOD: 
        {
            bufferGeneralReceive->result = mknod(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_GETATTR: 
        {
            GetAttributeReceiveBuffer *bufferReceive = 
                (GetAttributeReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = getattr(bufferGeneralSend->path, &(bufferReceive->attribute));
            break;
        }
        case MESSAGE_ACCESS: 
        {
            bufferGeneralReceive->result = access(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_MKDIR: 
        {
            bufferGeneralReceive->result = mkdir(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_READDIR: 
        {
            ReadDirectoryReceiveBuffer *bufferReceive = 
                (ReadDirectoryReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = readdir(bufferGeneralSend->path, &(bufferReceive->list));
            break;
        }
        case MESSAGE_READDIRECTORYMETA:
        {
        	ReadDirectoryMetaReceiveBuffer *bufferReceive = 
        		(ReadDirectoryMetaReceiveBuffer *)bufferGeneralReceive;
        	bufferReceive->result = readDirectoryMeta(bufferGeneralSend->path, 
        		&(bufferReceive->meta), 
        		&(bufferReceive->hashAddress),
        		&(bufferReceive->metaAddress),
        		&(bufferReceive->parentNodeID));
        	break;
        }
        case MESSAGE_EXTENTREAD:
        {
            ExtentReadSendBuffer *bufferSend = 
                (ExtentReadSendBuffer *)bufferGeneralSend;
            ExtentReadReceiveBuffer *bufferReceive = 
                (ExtentReadReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentRead(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi),
                 &(bufferReceive->offset), &(bufferReceive->key));
            unlockReadHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_EXTENTWRITE:
        {
            ExtentWriteSendBuffer *bufferSend = 
                (ExtentWriteSendBuffer *)bufferGeneralSend;
            ExtentWriteReceiveBuffer *bufferReceive = 
                (ExtentWriteReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentWrite(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi), 
                &(bufferReceive->offset), &(bufferReceive->key));
            unlockWriteHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_UPDATEMETA:
        {
            // UpdateMetaSendBuffer *bufferSend = 
            //     (UpdateMetaSendBuffer *)bufferGeneralSend;
            // bufferGeneralReceive->result = updateMeta(bufferSend->path, 
            //     &(bufferSend->metaFile), bufferSend->key);
            break;
        }
        case MESSAGE_EXTENTREADEND:
        {
            // ExtentReadEndSendBuffer *bufferSend = 
            //     (ExtentReadEndSendBuffer *)bufferGeneralSend;
            // bufferGeneralReceive->result = extentReadEnd(bufferSend->key, bufferSend->path);
            break;
        }
        case MESSAGE_TRUNCATE: 
        {
            TruncateSendBuffer *bufferSend = 
                (TruncateSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = truncate(bufferSend->path, bufferSend->size);
            break;
        }
        case MESSAGE_RMDIR: 
        {
            bufferGeneralReceive->result = rmdir(bufferGeneralSend->path);
            break;
        }
        case MESSAGE_REMOVE:
        {
            GetAttributeReceiveBuffer *bufferReceive = 
                (GetAttributeReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = remove(bufferGeneralSend->path, &(bufferReceive->attribute));
            break;
        }
        case MESSAGE_FREEBLOCK:
        {
            BlockFreeSendBuffer *bufferSend = 
                (BlockFreeSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = blockFree(bufferSend->startBlock, bufferSend->countBlock);
            break;
        }
        case MESSAGE_MKNODWITHMETA: 
        {
            MakeNodeWithMetaSendBuffer *bufferSend = 
                (MakeNodeWithMetaSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = mknodWithMeta(bufferSend->path, &(bufferSend->metaFile));
            break;
        }
        case MESSAGE_RENAME: 
        {
            RenameSendBuffer *bufferSend = 
                (RenameSendBuffer *)bufferGeneralSend;
            bufferGeneralReceive->result = rename(bufferSend->pathOld, bufferSend->pathNew);
            break;
        }
        case MESSAGE_RAWWRITE:
        {
            ExtentWriteSendBuffer *bufferSend = 
                (ExtentWriteSendBuffer *)bufferGeneralSend;
            ExtentWriteReceiveBuffer *bufferReceive = 
                (ExtentWriteReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentWrite(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi), 
                &(bufferReceive->offset), &(bufferReceive->key));
            unlockWriteHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        case MESSAGE_RAWREAD:
        {
            ExtentReadSendBuffer *bufferSend = 
                (ExtentReadSendBuffer *)bufferGeneralSend;
            ExtentReadReceiveBuffer *bufferReceive = 
                (ExtentReadReceiveBuffer *)bufferGeneralReceive;
            bufferReceive->result = extentRead(bufferSend->path, 
                bufferSend->size, bufferSend->offset, &(bufferReceive->fpi),
                 &(bufferReceive->offset), &(bufferReceive->key));
            unlockReadHashItem(bufferReceive->key, (NodeHash)bufferSend->sourceNodeID, (AddressHash)(bufferReceive->offset));
            break;
        }
        default:
            break;
    }
}


/* Internal add meta to directory function. Might cause overhead. No check on parameters for internal function.
   @param   path            Path of directory.
   @param   name            Name of meta.
   @param   isDirectory     Judge if it is directory.
   @return                  If succeed return true, otherwise return false. */
bool FileSystem::addMetaToDirectory(const char *path, const char *name, bool isDirectory, 
    uint64_t *TxID, uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size, uint64_t *key, uint64_t *offset)
{
    Debug::debugTitle("FileSystem::addMetaToDirectory");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
    uint64_t LocalTxID;
    if (checkLocal(hashNode) == true) { /* If local node. */
        // return true;
        bool result;
        *key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
        *offset = (uint64_t)hashAddress;
        Debug::debugItem("key = %lx, offset = %lx", *key, *offset);
        {
            Debug::debugItem("Stage 2. Check directory.");
            uint64_t indexDirectoryMeta; /* Meta index of directory. */
            bool isDirectoryTemporary; /* Different from parameter isDirectory. */
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectoryTemporary) == false) { /* If directory does not exist. */
                result = false;         /* Fail due to directory path does not exist. In future detail error information should be returned and independent access() and create() functions should be offered. */
            } else {
                if (isDirectoryTemporary == false) { /* If not a directory. */
                    result = false;     /* Fail due to path is not directory. */
                } else {
                    DirectoryMeta metaDirectory;
                    if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) { /* Get directory meta. */
                        result = false; /* Fail due to get directory meta error. */
                    } else {
                        LocalTxID = TxLocalBegin();
                        metaDirectory.count++; /* Add count of names under directory. */
                        //printf("metaDirectory.count: %d, name len: %d\n", metaDirectory.count, (int)strlen(name));
                        strcpy(metaDirectory.tuple[metaDirectory.count - 1].names, name); /* Add name. */
                        metaDirectory.tuple[metaDirectory.count - 1].isDirectories = isDirectory; /* Add directory state. */
                        /* Write to log first. */
                        TxWriteData(LocalTxID, (uint64_t)&metaDirectory, (uint64_t)sizeof(DirectoryMeta));
                        *srcBuffer = getTxWriteDataAddress(LocalTxID);
                        *size = (uint64_t)sizeof(DirectoryMeta);
                        // printf("%s ", path);
                        *TxID = LocalTxID;
                        if (storage->tableDirectoryMeta->put(indexDirectoryMeta, &metaDirectory, desBuffer) == false) { /* Update directory meta. */
                            result = false; /* Fail due to put directory meta error. */
                        } else {
                            // printf("addmeta, desBuffer = %lx, srcBuffer = %lx, size = %d\n", *desBuffer, *srcBuffer, *size);
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
        }
        if (result == false) {
            TxLocalCommit(LocalTxID, false);
        } else {
            TxLocalCommit(LocalTxID, true);
        }
        // unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
        Debug::debugItem("Stage end.");
        return result;                  /* Return specific result. */
    } else {                            /* If remote node. */
        AddMetaToDirectorySendBuffer bufferAddMetaToDirectorySend; /* Send buffer. */
	    bufferAddMetaToDirectorySend.message = MESSAGE_ADDMETATODIRECTORY; /* Assign message type. */
	    strcpy(bufferAddMetaToDirectorySend.path, path);  /* Assign path. */
	    strcpy(bufferAddMetaToDirectorySend.name, name);  /* Assign name. */
        bufferAddMetaToDirectorySend.isDirectory = isDirectory;
        UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive;
        RdmaCall((uint16_t)hashNode, 
                 (char *)&bufferAddMetaToDirectorySend, 
                 (uint64_t)sizeof(AddMetaToDirectorySendBuffer),
                 (char *)&bufferGeneralReceive,
                 (uint64_t)sizeof(UpdataDirectoryMetaReceiveBuffer));
        *srcBuffer = bufferGeneralReceive.srcBuffer;
        *desBuffer = bufferGeneralReceive.desBuffer;
        *TxID = bufferGeneralReceive.TxID;
        *size = bufferGeneralReceive.size;
        *key = bufferGeneralReceive.key;
        *offset = bufferGeneralReceive.offset;
        return bufferGeneralReceive.result;
    }
}

/* Internal remove meta from directory function. Might cause overhead. No check on parameters for internal function.
   @param   path            Path of directory.
   @param   name            Name of meta.
   @param   isDirectory     Judge if it is directory.
   @return                  If succeed return true, otherwise return false. */
bool FileSystem::removeMetaFromDirectory(const char *path, const char *name, 
    uint64_t *TxID, uint64_t *srcBuffer, uint64_t *desBuffer, uint64_t *size,  uint64_t *key, uint64_t *offset)
{
    Debug::debugTitle("FileSystem::removeMetaFromDirectory");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    UniqueHash hashUnique;
    HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
    AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
    uint64_t LocalTxID;
    if (checkLocal(hashNode) == true) { /* If local node. */
        bool result;
        *key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
        *offset = (uint64_t)hashAddress;
        {
            Debug::debugItem("Stage 2. Check directory.");
            uint64_t indexDirectoryMeta; /* Meta index of directory. */
            bool isDirectory;
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If directory does not exist. */
            	Debug::notifyError("Directory does not exist.");
                result = false;         /* Fail due to directory path does not exist. In future detail error information should be returned and independent access() and create() functions should be offered. */
            } else {
                if (isDirectory == false) { /* If not a directory. */
                    result = false;     /* Fail due to path is not directory. */
                } else {
                    DirectoryMeta metaDirectory;
                    if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) { /* Get directory meta. */
                    	Debug::notifyError("Get directory meta failed.");
                        result = false; /* Fail due to get directory meta error. */
                    } else {
                        LocalTxID = TxLocalBegin();
                        DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
                        bool found = false;
                        uint64_t indexModifiedNames = 0;
                        for (uint64_t i = 0; i < metaDirectory.count; i++) {
                            if (strcmp(metaDirectory.tuple[i].names, name) == 0) { /* If found selected name. */
                                found = true; /* Mark and continue. */
                            } else {
                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, metaDirectory.tuple[i].names); /* Copy original name to modified meta. */
                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = metaDirectory.tuple[i].isDirectories; /* Add directory state. */
                                indexModifiedNames++;
                            }
                        }
                        metaModifiedDirectory.count = indexModifiedNames; /* No need to +1. Current point to index after last one. Can be adapted to multiple removes. However it should not occur. */
                        if (found == false) {
                        	Debug::notifyError("Fail due to no selected name.");
                            TxLocalCommit(LocalTxID, false);
                            result = false; /* Fail due to no selected name. */
                        } else {
                            /* Write to log first. */
                            TxWriteData(LocalTxID, (uint64_t)&metaModifiedDirectory, (uint64_t)sizeof(DirectoryMeta));
                            *srcBuffer = getTxWriteDataAddress(LocalTxID);
                            *size = (uint64_t)sizeof(DirectoryMeta);
                            *TxID = LocalTxID;
                            if (storage->tableDirectoryMeta->put(indexDirectoryMeta, &metaModifiedDirectory, desBuffer) == false) { /* Update directory meta. */
                                result = false; /* Fail due to put directory meta error. */
                            } else {
                                Debug::debugItem("Item. ");
                                result = true; /* Succeed. */
                            }
                        }
                    }
                }
            }
        }
        if (result == false) {
            TxLocalCommit(LocalTxID, false);
        } else {
            TxLocalCommit(LocalTxID, true);
        }
        //unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
        Debug::debugItem("Stage end.");
        return result;                  /* Return specific result. */
    } else {                            /* If remote node. */
        RemoveMetaFromDirectorySendBuffer bufferRemoveMetaFromDirectorySend; /* Send buffer. */
        bufferRemoveMetaFromDirectorySend.message = MESSAGE_REMOVEMETAFROMDIRECTORY; /* Assign message type. */
        strcpy(bufferRemoveMetaFromDirectorySend.path, path);  /* Assign path. */
        strcpy(bufferRemoveMetaFromDirectorySend.name, name);  /* Assign name. */
        UpdataDirectoryMetaReceiveBuffer bufferGeneralReceive; /* Receive buffer. */
        RdmaCall((uint16_t)hashNode,
                 (char *)&bufferRemoveMetaFromDirectorySend,
                 (uint64_t)sizeof(RemoveMetaFromDirectorySendBuffer),
                 (char *)&bufferGeneralReceive,
                 (uint64_t)sizeof(UpdataDirectoryMetaReceiveBuffer));
        *srcBuffer = bufferGeneralReceive.srcBuffer;
        *desBuffer = bufferGeneralReceive.desBuffer;
        *TxID = bufferGeneralReceive.TxID;
        *size = bufferGeneralReceive.size;
        *key = bufferGeneralReceive.key;
        *offset = bufferGeneralReceive.offset;
        return bufferGeneralReceive.result;
    }
}

bool FileSystem::updateDirectoryMeta(const char *path, uint64_t TxID, uint64_t srcBuffer, 
        uint64_t desBuffer, uint64_t size, uint64_t key, uint64_t offset) {
    Debug::debugTitle("FileSystem::updateDirectoryMeta");
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        Debug::debugItem("path = %s, TxID = %d, srcBuffer = %lx, desBuffer = %lx, size = %ld", path, TxID, srcBuffer, desBuffer, size);
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        // AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) {
            Debug::debugItem("Stage 2. Check Local.");
            bool result = true;
            // uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            uint64_t indexDirectoryMeta;
            bool isDirectory;
            if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path exists. */
                Debug::notifyError("The path does not exists.");
                result = false; /* Fail due to existence of path. */
            } else {
                result = true;
                // printf("%s ", path);
                Debug::debugItem("update, desbuf = %lx, srcbuf = %lx, size = %d",desBuffer, srcBuffer, size);
                memcpy((void *)desBuffer, (void *)srcBuffer, size);
                Debug::debugItem("copied");
            }
            Debug::debugItem("key = %lx, offset = %lx", key, offset);
            unlockWriteHashItem(key, hashNode, (AddressHash)offset);  /* Unlock hash item. */
            TxLocalCommit(TxID, true);
            if (result == false) {
                Debug::notifyError("DOCOMMIT With Error.");
            }
            return result;
        } else {
            DoRemoteCommitSendBuffer bufferSend;
            strcpy(bufferSend.path, path);
            bufferSend.message = MESSAGE_DOCOMMIT;
            bufferSend.TxID = TxID;
            bufferSend.srcBuffer = srcBuffer;
            bufferSend.desBuffer = desBuffer;
            bufferSend.size = size;
            bufferSend.key = key;
            bufferSend.offset = offset;
            GeneralReceiveBuffer bufferReceive;
            RdmaCall((uint16_t)hashNode,
                    (char *)&bufferSend,
                    (uint64_t)sizeof(DoRemoteCommitSendBuffer),
                    (char *)&bufferReceive,
                    (uint64_t)sizeof(GeneralReceiveBuffer));
            if (bufferReceive.result == false) {
                Debug::notifyError("Remote Call on DOCOMMIT With Error.");
            }
            return bufferReceive.result;
        }
    }
}

/* Make node (file) with file meta.
   @param   path        Path of file.
   @param   metaFile    File meta. 
   @return              If succeed return true, otherwise return false. */
bool FileSystem::mknodWithMeta(const char *path, FileMeta *metaFile)
{
    Debug::debugTitle("FileSystem::mknodWithMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (metaFile == NULL)) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            Debug::debugItem("Stage 2. Check parent.");
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 3. Create file meta from old.");
                    uint64_t indexFileMeta;
                    metaFile->timeLastModified = time(NULL); /* Set last modified time. */
                    if (storage->tableFileMeta->create(&indexFileMeta, metaFile) == false) {
                        result = false; /* Fail due to create error. */
                    } else {
                        if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                            result = false; /* Fail due to hash table put. No roll back. */
                        } else {
                            result = true;
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
} 

/* Make node. That is to create an empty file. 
   @param   path    Path of file.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::mknod(const char *path) 
{
#ifdef TRANSACTION_2PC
    return mknod2pc(path);
#endif
#ifdef TRANSACTION_CD
    return mknodcd(path);
#endif
}

bool FileSystem::mknodcd(const char *path) 
{
    Debug::debugTitle("FileSystem::mknod");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                // DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Update parent directory metadata.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                DirectoryMeta parentMeta;
                uint64_t parentHashAddress, parentMetaAddress;
                uint16_t parentNodeID;
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                    Debug::notifyError("readDirectoryMeta failed.");
                    // TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        Debug::notifyError("addMetaToDirectory failed.");
                        // TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                    	
                    	/* Update directory meta first. */
                    	parentMeta.count++; /* Add count of names under directory. */
                        strcpy(parentMeta.tuple[parentMeta.count - 1].names, name); /* Add name. */
                        parentMeta.tuple[parentMeta.count - 1].isDirectories = isDirectory; /* Add directory state. */
                        
                        Debug::debugItem("Stage 3. Create file meta.");
                        uint64_t indexFileMeta;
                        FileMeta metaFile;
                        metaFile.timeLastModified = time(NULL); /* Set last modified time. */
                        metaFile.count = 0; /* Initialize count of extents as 0. */
                        metaFile.size = 0;
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        // TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        updateRemoteMeta(parentNodeID, &parentMeta, parentMetaAddress, parentHashAddress);
                        /* Only allocate momery, write to log first. */
                        if (storage->tableFileMeta->create(&indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
            	free(name);
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}
bool FileSystem::mknod2pc(const char *path) 
{
    Debug::debugTitle("FileSystem::mknod");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Update parent directory metadata.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (addMetaToDirectory(parent, name, false, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    Debug::notifyError("addMetaToDirectory failed.");
                    TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        Debug::notifyError("addMetaToDirectory failed.");
                        TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                        Debug::debugItem("Stage 3. Create file meta.");
                        uint64_t indexFileMeta;
                        FileMeta metaFile;
                        metaFile.timeLastModified = time(NULL); /* Set last modified time. */
                        metaFile.count = 0; /* Initialize count of extents as 0. */
                        metaFile.size = 0;
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&metaFile, (uint64_t)sizeof(FileMeta));
                        /* Receive remote prepare with (OK) */
                        TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        Debug::debugItem("mknod, key = %lx, offset = %lx", remotekey, offset);
                        updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                        /* Only allocate momery, write to log first. */
                        if (storage->tableFileMeta->create(&indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            if (storage->hashtable->put(&hashUnique, indexFileMeta, false) == false) { /* false for file. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
                free(name);
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}
/* Get attributes. 
   @param   path        Path of file or folder.
   @param   attribute   Attribute buffer of file to get.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::getattr(const char *path, FileMeta *attribute)
{
    Debug::debugTitle("FileSystem::getattr");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (attribute == NULL)) /* Judge if path and attribute buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            // return true;
            bool result;
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                Debug::debugItem("Stage 1.1.");
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    Debug::debugItem("Stage 1.2.");
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                        if (storage->tableFileMeta->get(indexMeta, attribute) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            Debug::debugItem("FileSystem::getattr, meta.size = %d", attribute->size);
                            result = true; /* Succeed. */
                        }
                    } else {
                    	attribute->count = MAX_FILE_EXTENT_COUNT; /* Inter meaning, representing directoiries */
                        result = true;
                    }
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Access. That is to judge the existence of file or folder.
   @param   path    Path of file or folder.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::access(const char *path)
{
    Debug::debugTitle("FileSystem::access");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL)                   /* Judge if path is valid. */
        return false;                   /* Null path error. */
    else {
        Debug::debugItem("Stage 2. Check access.");
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        Debug::debugItem("Stage 3.");
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            Debug::debugItem("Stage 4.");
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    result = true;      /* Succeed. Do not need to check parent. */
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Make directory. 
   @param   path    Path of folder. 
   @return          If operation succeeds then return true, otherwise return false. */
// bool FileSystem::mkdir(const char *path)
// {
//     Debug::debugTitle("FileSystem::mkdir");
//     Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
//     char *parent = (char *)malloc(strlen(path) + 1);
//     getParentDirectory(path, parent);
//     nrfsfileattr attribute;
//     Debug::debugItem("Stage 2. Check parent.");
//     getattr(parent, &attribute);
//     char *name = (char *)malloc(strlen(path) + 1); /* Allocate buffer of parent path. Omit failure. */
//     getNameFromPath(path, name);
//     Debug::debugItem("Stage 3. Add name.");
//     addMetaToDirectory(parent, name, true);
//     free(parent);
//     free(name);
//     return true;
// }

bool FileSystem::mkdir(const char *path) 
{
#ifdef TRANSACTION_2PC
    return mkdir2pc(path);
#endif
#ifdef TRANSACTION_CD
    return mkdircd(path);
#endif
}

bool FileSystem::mkdircd(const char *path)
{
    Debug::debugTitle("FileSystem::mkdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                // DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Check parent.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                DirectoryMeta parentMeta;
                uint64_t parentHashAddress, parentMetaAddress;
                uint16_t parentNodeID;
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                    // TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        // TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {

                    	/* Update directory meta first. */
                    	parentMeta.count++; /* Add count of names under directory. */
                        strcpy(parentMeta.tuple[parentMeta.count - 1].names, name); /* Add name. */
                        parentMeta.tuple[parentMeta.count - 1].isDirectories = true; /* Add directory state. */
                        
                        Debug::debugItem("Stage 3. Write directory meta.");
                        uint64_t indexDirectoryMeta;
                        DirectoryMeta metaDirectory;
                        metaDirectory.count = 0; /* Initialize count of names as 0. */
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        // TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        updateRemoteMeta(parentNodeID, &parentMeta, parentMetaAddress, parentHashAddress);
                        
                        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            Debug::debugItem("indexDirectoryMeta = %d", indexDirectoryMeta);
                            if (storage->hashtable->put(&hashUnique, indexDirectoryMeta, true) == false) { /* true for directory. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
            	free(name);
            }

            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }

            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::mkdir2pc(const char *path)
{
    Debug::debugTitle("FileSystem::mkdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                DistributedTxID = TxDistributedBegin();
                LocalTxID = TxLocalBegin();
                Debug::debugItem("Stage 2. Check parent.");
                char *parent = (char *)malloc(strlen(path) + 1);
                char *name = (char *)malloc(strlen(path) + 1);
                getParentDirectory(path, parent);
                getNameFromPath(path, name);
                if (addMetaToDirectory(parent, name, true, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    TxDistributedPrepare(DistributedTxID, false);
                    result = false;
                } else {
                    uint64_t indexMeta;
                    bool isDirectory;
                    if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == true) { /* If path exists. */
                        TxDistributedPrepare(DistributedTxID, false);
                        result = false; /* Fail due to existence of path. */
                    } else {
                        Debug::debugItem("Stage 3. Write directory meta.");
                        uint64_t indexDirectoryMeta;
                        DirectoryMeta metaDirectory;
                        metaDirectory.count = 0; /* Initialize count of names as 0. */
                        /* Apply updated data to local log. */
                        TxWriteData(LocalTxID, (uint64_t)&metaDirectory, (uint64_t)sizeof(DirectoryMeta));
                        /* Receive remote prepare with (OK) */
                        TxDistributedPrepare(DistributedTxID, true);
                        /* Start phase 2, commit it. */
                        Debug::debugItem("mknod, key = %lx, offset = %lx", remotekey, offset);
                        updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);

                        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to create error. */
                        } else {
                            Debug::debugItem("indexDirectoryMeta = %d", indexDirectoryMeta);
                            if (storage->hashtable->put(&hashUnique, indexDirectoryMeta, true) == false) { /* true for directory. */
                                result = false; /* Fail due to hash table put. No roll back. */
                            } else {
                                result = true;
                            }
                        }
                    }
                }
                free(parent);
                free(name);
            }

            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }

            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Read filenames in directory. 
   @param   path    Path of folder.
   @param   list    List buffer of names in directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::readdir(const char *path, nrfsfilelist *list)
{
    Debug::debugTitle("FileSystem::readdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (list == NULL)) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                        result = false; /* Fail due to not directory. */
                    } else {
                        DirectoryMeta metaDirectory; /* Copy operation of meta might cause overhead. */
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                            list->count = metaDirectory.count; /* Assign count of names in directory. */
                            for (uint64_t i = 0; i < metaDirectory.count; i++) {
                                strcpy(list->tuple[i].names, metaDirectory.tuple[i].names);
                                if (metaDirectory.tuple[i].isDirectories == true) {
                                    list->tuple[i].isDirectories = true; /* Mode 1 for directory. */
                                } else { 
                                    list->tuple[i].isDirectories = false; /* Mode 0 for file. */
                                }
                            }
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Read filenames in directory. 
   @param   path    Path of folder.
   @param   list    List buffer of names in directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::recursivereaddir(const char *path, int depth)
{
    if (path == NULL) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    if (isDirectory == false) { /* If file meta. */
                        result = false; /* Fail due to not directory. */
                    } else {
                        DirectoryMeta metaDirectory; /* Copy operation of meta might cause overhead. */
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                            for (uint64_t i = 0; i < metaDirectory.count; i++) {
				for (int nn = 0; nn < depth; nn++)
					printf("\t");
                                if (metaDirectory.tuple[i].isDirectories == true) {
					printf("%d DIR %s\n", (int)i, metaDirectory.tuple[i].names);
					char *childPath = (char *)malloc(sizeof(char) * 
						(strlen(path) + strlen(metaDirectory.tuple[i].names) + 2));
					strcpy(childPath, path);
					if (strcmp(childPath, "/") != 0)
						strcat(childPath, "/");
					strcat(childPath, metaDirectory.tuple[i].names);
					recursivereaddir(childPath, depth + 1);
					free(childPath);
                                } else {
					printf("%d FILE %s\n", (int)i, metaDirectory.tuple[i].names);
				}
                            }
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}


/* Read directory meta. 
   @param   path    Path of folder.
   @param   meta    directory meta pointer.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::readDirectoryMeta(const char *path, DirectoryMeta *meta, uint64_t *hashAddress, uint64_t *metaAddress, uint16_t *parentNodeID) {
    Debug::debugTitle("FileSystem::readDirectoryMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) /* Judge if path and list buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
    	bool result;
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        *hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        *parentNodeID = (uint16_t)hashNode;
        if (checkLocal(hashNode) == true) { /* If local node. */
            uint64_t key = lockReadHashItem(hashNode, *hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                	Debug::notifyError("path does not exist");
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If file meta. */
                    	Debug::notifyError("Not a directory");
                        result = false; /* Fail due to not directory. */
                    } else {
                        if (storage->tableDirectoryMeta->get(indexDirectoryMeta, meta, metaAddress) == false) {
                        	Debug::notifyError("Fail due to get directory meta error.");
                            result = false; /* Fail due to get directory meta error. */
                        } else {
                        	Debug::debugItem("metaAddress = %lx, getDmfsBaseAddress = %lx", *metaAddress, server->getMemoryManagerInstance()->getDmfsBaseAddress());
                        	*metaAddress = *metaAddress - server->getMemoryManagerInstance()->getDmfsBaseAddress();
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockReadHashItem(key, hashNode, *hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
        	GeneralSendBuffer bufferSend;
        	bufferSend.message = MESSAGE_READDIRECTORYMETA;
        	strcpy(bufferSend.path, path);
        	ReadDirectoryMetaReceiveBuffer bufferReceive;
        	RdmaCall((uint16_t)hashNode,
                    (char *)&bufferSend,
                    (uint64_t)sizeof(GeneralSendBuffer),
                    (char *)&bufferReceive,
                    (uint64_t)sizeof(ReadDirectoryMetaReceiveBuffer));
            if (bufferReceive.result == false) {
            	Debug::notifyError("Remote Call readDirectoryMeta failed");
                result = false;
            } else {
            	memcpy((void *)meta, (void *)&(bufferReceive.meta), sizeof(DirectoryMeta));
            	*hashAddress = bufferReceive.hashAddress;
            	*metaAddress = bufferReceive.metaAddress;
            	*parentNodeID = bufferReceive.parentNodeID;
            	return bufferReceive.result;
            }
            return false;
        }
    }
}


 /*There are 2 schemas of blocks.
 * 
 *      Schema 1, only one block: ("[" means start byte and "]" means end byte.)
 *
 *      Bound of Block     + start_block = end_block 
 *           Index         |
 *      -------------------+----------------------------------------+--------------------
 *                         |                                        |
 *        Previous Blocks  |        Start Block  (End Block)        |  Following Blocks
 *                         |                                        |
 *      -------------------+-------[-----------------------]--------+--------------------
 *                                 |                       |
 *      Relative Address           + offset                + offset + size - 1
 *          in File                \                      /
 *                                  -------  size  -------
 *
 *      Schema 2, several blocks:
 *
 *      Bound of Block     + start_block                                         + end_block
 *           Index         |                                                     |
 *      -------------------+---------------+-------------------------------------+-------------+--------------------
 *                         |               |          Middle Blocks              |             |
 *        Previous Blocks  |  Start Block  +-----+-----+---------+-----+---------+  End Block  |  Following Blocks
 *                         |               |  0  |  1  |   ...   |  i  |   ...   |             |
 *      -------------------+----[----------[-----+-----+---------[-----+---------+---------]---+--------------------
 *                              |          |                     |                         |
 *     start_block * BLOCK_SIZE +          + (start_block + 1)   + (start_block + 1 + i)   + start_block * BLOCK_SIZE 
 *      + offset % BLOCK_SIZE   \             * BLOCK_SIZE          * BLOCK_SIZE          /   + offset % BLOCK_SIZE + size - 1
 *                               ------------------------ size  --------------------------
 *
 *                              +----------------------------------------------------------+
 *                              |                                                          |
 *                              |                       Buffer                             |
 *                              |                                                          |
 *                              [----------[-----+-----+---------[-----+---------[---------]
 *                              |          |     |     |   ...   |     |   ...   |         |
 *                              + 0        |     +     +         |     +         |         + size - 1
 *                                         |                     |               |
 *                                         |                     |             + BLOCK_SIZE - offset % BLOCK_SIZE + (end_block - start_block - 1) * BLOCK_SIZE
 *                                         |                     |
 *                                         |                     + BLOCK_SIZE - offset % BLOCK_SIZE + i * BLOCK_SIZE
 *                                         |
 *                                         + BLOCK_SIZE - offset % BLOCK_SIZE
 *
 *      start_block = offset / BLOCK_SIZE
 *      end_block = (offset + size - 1) / BLOCK_SIZE
 */

/* Fill file position information for read and write. No check on parameters. 
   @param   size        Size to operate.
   @param   offset      Offset to operate. 
   @param   fpi         File position information.
   @param   metaFile    File meta. */
/* FIXME: review logic here. */
void FileSystem::fillFilePositionInformation(uint64_t size, uint64_t offset, file_pos_info *fpi, FileMeta *metaFile)
{
    Debug::debugItem("Stage 8.");
    uint64_t offsetStart, offsetEnd;
    offsetStart = offset;  /* Relative offset of start byte to operate in file. */
    offsetEnd = size + offset - 1; /* Relative offset of end byte to operate in file. */
    uint64_t boundStartExtent, boundEndExtent, /* Bound of start extent and end extent. */
             offsetInStartExtent, offsetInEndExtent, /* Offset of start byte in start extent and end byte in end extent. */
             sizeInStartExtent, sizeInEndExtent; /* Size to operate in start extent and end extent. */
    uint64_t offsetStartOfCurrentExtent = 0; /* Relative offset of start byte in current extent. */
    Debug::debugItem("Stage 9.");
    for (uint64_t i = 0; i < metaFile->count; i++) {
        if ((offsetStartOfCurrentExtent + metaFile->tuple[i].countExtentBlock * BLOCK_SIZE - 1) >= offsetStart) { /* A -1 is needed to locate offset of end byte in current extent. */
            boundStartExtent = i;       /* Assign bound of extent containing start byte. */
            offsetInStartExtent = offsetStart - offsetStartOfCurrentExtent; /* Assign relative offset of start byte in start extent. */
            sizeInStartExtent = metaFile->tuple[i].countExtentBlock * BLOCK_SIZE - offsetInStartExtent; /* Assign size to opreate in start extent. */
            break;
        }
        offsetStartOfCurrentExtent += metaFile->tuple[i].countExtentBlock * BLOCK_SIZE; /* Add count of blocks in current extent. */
    }
    offsetStartOfCurrentExtent = 0;     /* Relative offset of end byte in current extent. */
    Debug::debugItem("Stage 10. metaFile->count = %lu", metaFile->count);
    for (uint64_t i = 0; i < metaFile->count; i++) {
        if ((offsetStartOfCurrentExtent + metaFile->tuple[i].countExtentBlock * BLOCK_SIZE - 1) >= offsetEnd) { /* A -1 is needed to locate offset of end byte in current extent. */
            boundEndExtent = i;         /* Assign bound of extent containing end byte. */
            offsetInEndExtent = offsetEnd - offsetStartOfCurrentExtent; /* Assign relative offset of end byte in end extent. */
            sizeInEndExtent = offsetInEndExtent + 1; /* Assign size to opreate in end extent. */
            break;
        }
        offsetStartOfCurrentExtent += metaFile->tuple[i].countExtentBlock * BLOCK_SIZE; /* Add count of blocks in current extent. */
    }
    Debug::debugItem("Stage 11. boundStartExtent = %lu, boundEndExtent = %lu", boundStartExtent, boundEndExtent);
    if (boundStartExtent == boundEndExtent) { /* If in one extent. */
        fpi->len = 1;                   /* Assign length. */
        fpi->tuple[0].node_id = metaFile->tuple[boundStartExtent].hashNode; /* Assign node ID. */
        fpi->tuple[0].offset = metaFile->tuple[boundStartExtent].indexExtentStartBlock * BLOCK_SIZE + offsetInStartExtent; /* Assign offset. */
        fpi->tuple[0].size = size;
    } else {                            /* Multiple extents. */
        Debug::debugItem("Stage 12.");
        fpi->len = boundEndExtent - boundStartExtent + 1; /* Assign length. */
        fpi->tuple[0].node_id = metaFile->tuple[boundStartExtent].hashNode; /* Assign node ID of start extent. */
        fpi->tuple[0].offset = metaFile->tuple[boundStartExtent].indexExtentStartBlock * BLOCK_SIZE + offsetInStartExtent; /* Assign offset. */
        fpi->tuple[0].size = sizeInStartExtent; /* Assign size. */
        for (int i = 1; i <= ((int)(fpi->len) - 2); i++) { /* Start from second extent to one before last extent. */
            fpi->tuple[i].node_id = metaFile->tuple[boundStartExtent + i].hashNode; /* Assign node ID of start extent. */
            fpi->tuple[i].offset = metaFile->tuple[boundStartExtent + i].indexExtentStartBlock * BLOCK_SIZE; /* Assign offset. */
            fpi->tuple[i].size = metaFile->tuple[boundStartExtent + i].countExtentBlock * BLOCK_SIZE; /* Assign size. */
        }
        fpi->tuple[fpi->len - 1].node_id= metaFile->tuple[boundEndExtent].hashNode; /* Assign node ID of start extent. */
        fpi->tuple[fpi->len - 1].offset = 0;  /* Assign offset. */
        fpi->tuple[fpi->len - 1].size = sizeInEndExtent; /* Assign size. */
        Debug::debugItem("Stage 13.");
    }
}  

/* Read extent. That is to parse the part to read in file position information.
   @param   path    Path of file.
   @param   size    Size of data to read.
   @param   offset  Offset of data to read.
   @param   fpi     File position information buffer.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::extentRead(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key)
{
    //printf("fs-read, path: %s, size: %lx, offset: %lx\n", path, size, offset);
    Debug::debugTitle("FileSystem::read");
    Debug::debugCur("read, size = %x, offset = %x", size, offset);
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (fpi == NULL) || (size == 0) || (key == NULL)) /* Judge if path and file position information buffer are valid or size to read is valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            *key = lockReadHashItem(hashNode, hashAddress); /* Lock hash item. */
            *key_offset = hashAddress;
            //printf("read lock succeed\n");
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        Debug::notifyError("Directory meta.");
                        result = false; /* Fail due to not file. */
                    } else {
                        FileMeta metaFile;
                        Debug::debugItem("Stage 3. Get Filemeta index.");
                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
                            Debug::notifyError("Fail due to get file meta error.");
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            Debug::debugItem("Stage 3-1. meta.size = %d, size = %d, offset = %d", metaFile.size, size, offset);
                            if (offset + 1 > metaFile.size) { /* Judge if offset and size are valid. */
                                fpi->len = 0;
                                return true;
                            }
                            else if((metaFile.size - offset) < size)
                            {
                                size = metaFile.size - offset;
                            }
                            fillFilePositionInformation(size, offset, fpi, &metaFile); /* Fill file position information. */
                            result = true; /* Succeed. */
                        }
                    } 
                }
            }
            /* Do not unlock here. */
            // unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Read extent end. Only unlock path due to lock in extentRead.
   @param   Key         key obtained from read lock.
   @param   path        Path of file or folder.*/
bool FileSystem::extentReadEnd(uint64_t key, char* path)
{
    Debug::debugTitle("FileSystem::extentReadEnd");

    if (path == NULL)                   /* Judge if path and file meta buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            /* Do not lock. It has been locked in extentRead. */
            // uint64_t key = lockHashItem(path); /* Lock hash item. */
            uint64_t indexFileMeta;
            bool isDirectory;
            storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory);
            if(isDirectory)
                Debug::notifyError("Directory is read.");
            unlockReadHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return true;                     /* Return. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Write extent. That is to parse the part to write in file position information. Attention: do not unlock here. Unlock is implemented in updateMeta.
   @param   path        Path of file.
   @param   size        Size of data to write.
   @param   offset      Offset of data to write.
   @param   fpi         File position information.
   @param   metaFile    File meta buffer.
   @param   key         Key buffer to unlock.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::extentWrite(const char *path, uint64_t size, uint64_t offset, file_pos_info *fpi, uint64_t *key_offset, uint64_t *key) /* Write. */
{
    Debug::debugTitle("FileSystem::write");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    Debug::debugItem("write, size = %x, offset = %x", size, offset);
    if ((path == NULL) || (fpi == NULL) || (key == NULL)) /* Judge if path, file position information buffer and key buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            *key = lockWriteHashItem(hashNode, hashAddress);  /* Lock hash item. */
            *key_offset = hashAddress;
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                FileMeta *metaFile = (FileMeta *)malloc(sizeof(FileMeta));
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false; /* Fail due to not file. */
                    } else {
                        if (storage->tableFileMeta->get(indexFileMeta, metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            if (((0xFFFFFFFFFFFFFFFF - offset) < size) || (size == 0)) {
                                result = false; /* Fail due to offset + size will cause overflow or size is zero. */
                            } else {
                                Debug::debugItem("Stage 3.");
                                if ((metaFile->size == 0) || ((offset + size - 1) / BLOCK_SIZE > (metaFile->size - 1) / BLOCK_SIZE)) { /* Judge if new blocks need to be created. */
                                    uint64_t countExtraBlock; /* Count of extra blocks. At least 1. */
                                    int64_t boundCurrentExtraExtent; /* Bound of current extra extent. */
                                    /* Consider 0 size file. */
                                    Debug::debugItem("Stage 4. metaFile->size = %lu", metaFile->size);
                                    if (metaFile->size == 0) {
                                        boundCurrentExtraExtent = -1; /* Bound current extra extent start from 0. */
                                        countExtraBlock = (offset + size - 1) / BLOCK_SIZE + 1;
                                        Debug::debugItem("Stage 4-1, countExtraBlock = %d, BLOCK_SIZE = %d", countExtraBlock, BLOCK_SIZE);
                                    } else {
                                        boundCurrentExtraExtent = (int64_t)metaFile->count - 1;  /* Bound current extra extent start from metaFile->count. */
                                        countExtraBlock = (offset + size - 1) / BLOCK_SIZE - (metaFile->size - 1) / BLOCK_SIZE;
                                    }
                                    uint64_t indexCurrentExtraBlock; /* Index of current extra block. */
                                    uint64_t indexLastCreatedBlock;
                                    Debug::debugItem("Stage 5.");
                                    bool resultFor = true;
                                    for (uint64_t i = 0; i < countExtraBlock; i++) { /* No check on count of extra blocks. */
                                        Debug::debugItem("for loop, i = %d", i);
                                        if (storage->tableBlock->create(&indexCurrentExtraBlock) == false) {
                                            resultFor = false; /* Fail due to no enough space. Might cause inconsistency. */
                                            break;
                                        } else { /* So we need to modify the allocation way in table class. */
                                            indexLastCreatedBlock = -1;
                                            if(boundCurrentExtraExtent >= 0) {
                                                indexLastCreatedBlock = metaFile->tuple[boundCurrentExtraExtent].indexExtentStartBlock 
                                                + metaFile->tuple[boundCurrentExtraExtent].countExtentBlock;
                                            }
                                            Debug::debugItem("%d, %d, %d", 
                                            boundCurrentExtraExtent,
                                            indexLastCreatedBlock,
                                            indexCurrentExtraBlock);

                                            if(boundCurrentExtraExtent >= 0 && 
                                                (indexCurrentExtraBlock == indexLastCreatedBlock))
                                            {
                                                metaFile->tuple[boundCurrentExtraExtent].countExtentBlock++; /* Increment of count of blocks in current extent. */
                                            } else if (boundCurrentExtraExtent == -1 || 
                                            indexCurrentExtraBlock != indexLastCreatedBlock) { /* Separate block index generates a new extent. */
                                                boundCurrentExtraExtent++;
                                                metaFile->tuple[boundCurrentExtraExtent].hashNode = hashLocalNode; /* Assign local node hash. */
                                                metaFile->tuple[boundCurrentExtraExtent].indexExtentStartBlock = indexCurrentExtraBlock; /* Assign start block in extent. */
                                                metaFile->tuple[boundCurrentExtraExtent].countExtentBlock = 1; /* Assign count of block in extent. */
                                                metaFile->count++; /* Update count of extents. */
                                            } else{ /* Continuous blocks in one extent. */
                                                resultFor = false;
                                                break;
                                            }
                                        }
                                    }
                                    if(resultFor == false)
                                        result = false;
                                    else
                                    {
                                        metaFile->size = offset + size;
                                        Debug::debugItem("meta.size: %lx", metaFile->size);
                                        fillFilePositionInformation(size, offset, fpi, metaFile); /* Fill file position information. */
                                        result = true;
                                    }
                                } else {
                                    metaFile->size = (offset + size) > metaFile->size ? (offset + size) : metaFile->size; /* Update size of file in meta. */
                                    fillFilePositionInformation(size, offset, fpi, metaFile); /* Fill file position information. */
                                     /*printf("(int)(fpi->len) = %d, (int)(fpi->offset[0]) = %d, (int)(fpi->size[0]) = %d\n", 
                                                (int)(fpi->len), (int)(fpi->offset[0]), (int)(fpi->size[0]));*/
                                    result = true; /* Succeed. */
                                }
                            }
                        }
                    } 
                }
                if(result)
                {
                    metaFile->timeLastModified = time(NULL);
                    storage->tableFileMeta->put(indexFileMeta, metaFile);
                }
                for(int i = 0; i < (int)metaFile->count; i++)
                    Debug::debugCur("at %ld, start: %lx, len: %lx", i, metaFile->tuple[i].indexExtentStartBlock, metaFile->tuple[i].countExtentBlock);
                for(unsigned int i = 0; i < fpi->len; i++)
                    Debug::debugCur("fpi, at %d, offset: %lx, size: %lx", i, fpi->tuple[i].offset, fpi->tuple[i].size);
                Debug::debugItem("Stage 8. meta.size = %d", metaFile->size);
                free(metaFile);
            }
            /* Do not unlock hash item here. */
            // unlockHashItem(key, hashNode, hashAddress); /* Unlock hash item. Temporarily put it here. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        }
    }
    return false;
}

/* Update meta. Only unlock path due to lock in extentWrite.
   @param   path        Path of file or folder.
   @param   metaFile    File meta to update.
   @oaram   key         Key to unlock.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::updateMeta(const char *path, FileMeta *metaFile, uint64_t key)
{
    Debug::debugTitle("FileSystem::updateMeta");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if ((path == NULL) || (metaFile == NULL)) /* Judge if path and file meta buffer are valid. */
        return false;                   /* Null parameter error. */
    else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            /* Do not lock. It has been locked in extentWrite. */
            // uint64_t key = lockHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false;     /* Fail due to path does not exist. */
                } else {
                    Debug::debugItem("Stage 2. Put meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false;
                    } else {
                    	metaFile->timeLastModified = time(NULL); /* Set last modified time. */
                        if (storage->tableFileMeta->put(indexFileMeta, metaFile) == false) {
                            result = false; /* Fail due to put file meta error. */
                        } else {
                            Debug::debugItem("stage 3. Update Meta Succeed.");
                            result = true; /* Succeed. */
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress); /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Truncate file. Currently only support shrink file but cannot enlarge.
   @param   path    Path of file.
   @param   size    Size of file. 
   @return          If operation succeeds then return 0, otherwise return negative value. */
bool FileSystem::truncate(const char *path, uint64_t size) /* Truncate size of file to 0. */
{
    Debug::debugTitle("FileSystem::truncate");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false;
                    } else {
                        FileMeta metaFile;
                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            if (size >= metaFile.size) { /* So metaFile.size won't be 0. At least one block. */
                                result = false; /* Fail due to no bytes need to removed. */
                            } else {
                                uint64_t countTotalBlockTillLastExtentEnd = 0; /* Count of total blocks till end of last extent. */
                                uint64_t countNewTotalBlock; /* Count of total blocks in new file. */
                                if (size == 0) {
                                    countNewTotalBlock = 0; /* For 0 size file. */
                                } else {
                                    countNewTotalBlock = (size - 1) / BLOCK_SIZE + 1; /* For normal file. */
                                }
                                Debug::debugItem("Stage 3. Remove blocks.");
                                /* Current assume all blocks in local node. */
                                bool found = false;
                                uint64_t i;
                                for (i = 0; i < metaFile.count; i++) {
                                    if ((countTotalBlockTillLastExtentEnd + metaFile.tuple[i].countExtentBlock 
                                        >= countNewTotalBlock)) {
                                        found = true;
                                        break;
                                    }
                                    countTotalBlockTillLastExtentEnd += metaFile.tuple[i].countExtentBlock;
                                }
                                if (found == false) {
                                    result = false; /* Fail due to count of current total blocks is less than truncated version. Actually it cannot be reached. */
                                } else {
                                    uint64_t resultFor = true;
                                    // for (uint64_t j = (countNewTotalBlock - countTotalBlockTillLastExtentEnd - 1 + 1); /* Bound of first block to truncate. A -1 is to convert count to bound. A +1 is to move bound of last kept block to bound of first to truncate. */
                                    //     j < metaFile.countExtentBlock[i]; j++) {  /* i is current extent. */ 
                                    //     if (storage->tableBlock->remove(metaFile.indexExtentStartBlock[i] + j) == false) {
                                    //         resultFor = false;
                                    //         break;
                                    //     }
                                    // }
                                    for (int j = metaFile.tuple[i].countExtentBlock - 1; 
                                        j >= (int)(countNewTotalBlock - countTotalBlockTillLastExtentEnd - 1 + 1); j--) { /* i is current extent. */ /* Bound of first block to truncate. A -1 is to convert count to bound. A +1 is to move bound of last kept block to bound of first to truncate. */
                                        if (storage->tableBlock->remove(metaFile.tuple[i].indexExtentStartBlock + j) == false) {
                                            resultFor = false;
                                            break;
                                        }
                                    }
                                    if (resultFor == false) {
                                        result = false;
                                    } else {
                                        resultFor = true;
                                        // for (uint64_t j = i + 1; j < metaFile.count; j++) { /* Remove rest extents. */
                                        //     for (uint64_t k = 0; k < metaFile.countExtentBlock[j]; k++) {
                                        //         if (storage->tableBlock->remove(metaFile.indexExtentStartBlock[j] + k) == false) {
                                        //             resultFor = false;
                                        //             break;
                                        //         }
                                        //     }
                                        // }
                                        for (uint64_t j = i + 1; j < metaFile.count; j++) { /* Remove rest extents. */
                                            for (int k = metaFile.tuple[j].countExtentBlock - 1; k >= 0; k--) {
                                                if (storage->tableBlock->remove(metaFile.tuple[j].indexExtentStartBlock + k) == false) {
                                                    resultFor = false;
                                                    break;
                                                }
                                            }
                                        }
                                        if (resultFor == false) {
                                            result = false; /* Fail due to block remove error. */
                                        } else {
                                            metaFile.size = size; /* Size is the acutal size. */
                                            metaFile.count = i + 1; /* i is the last extent containing last block. */
                                            if (storage->tableFileMeta->put(indexFileMeta, &metaFile) == false) { /* Update meta. */
                                                result = false; /* Fail due to update file meta error. */
                                            } else {
                                                result = true; /* Succeed. */
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Remove directory (Unused).
   @param   path    Path of directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::rmdir(const char *path) 
{
    Debug::debugTitle("FileSystem::rmdir");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexDirectoryMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexDirectoryMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == false) { /* If not directory meta. */
                        result = false;
                    } else {
                        Debug::debugItem("Stage 3. Remove meta from directory.");
                        char *parent = (char *)malloc(strlen(path) + 1);
                        char *name = (char *)malloc(strlen(path) + 1);
                        getNameFromPath(path, name);
                        getParentDirectory(path, parent);
                        if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                            result = false;
                        } else {
                            DirectoryMeta metaDirectory;
                            if (storage->tableDirectoryMeta->get(indexDirectoryMeta, &metaDirectory) == false) {
                                result = false; /* Fail due to get file meta error. */
                            } else {
                                if (metaDirectory.count != 0) { /* Directory is not empty. */
                                    result = false; /* Fail due to directory is not empty. */
                                } else {
                                    Debug::debugItem("Stage 3. Remove directory meta.");
                                    if (storage->tableDirectoryMeta->remove(indexDirectoryMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                            }
                        }
			            free(parent);
			            free(name);
                    }
                }
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return true;
        }
    }
}

/* Remove file or empty directory. 
   @param   path    Path of file or directory.
   @return          If operation succeeds then return true, otherwise return false. */
bool FileSystem::remove(const char *path, FileMeta *metaFile) 
{
#ifdef TRANSACTION_2PC
    return remove2pc(path, metaFile);
#endif
#ifdef TRANSACTION_CD
    return removecd(path, metaFile);
#endif
}

bool FileSystem::removecd(const char *path, FileMeta *metaFile)
{
    Debug::debugTitle("FileSystem::remove");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        // uint64_t DistributedTxID;
        uint64_t LocalTxID = 0;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    char *parent = (char *)malloc(strlen(path) + 1);
                    char *name = (char *)malloc(strlen(path) + 1);
                    DirectoryMeta parentMeta;
	                uint64_t parentHashAddress, parentMetaAddress;
	                uint16_t parentNodeID;
                    getParentDirectory(path, parent);
                    getNameFromPath(path, name);
                    Debug::debugItem("Stage 2. Get meta.");
                    if(isDirectory)
                    {
                        DirectoryMeta metaDirectory;
                        if (storage->tableDirectoryMeta->get(indexMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            metaFile->count = MAX_FILE_EXTENT_COUNT;
                            if (metaDirectory.count != 0) { /* Directory is not empty. */
                                result = false; /* Fail due to directory is not empty. */
                            } else {
                                // DistributedTxID = TxDistributedBegin();
                                LocalTxID = TxLocalBegin();
                            	if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                            		Debug::notifyError("Remove Meta From Directory failed.");
                                    // TxDistributedPrepare(DistributedTxID, false);
                            		result = false;
                            	} else {
                            		/* Remove meta from directory */
                            		DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
			                        uint64_t indexModifiedNames = 0;
			                        for (uint64_t i = 0; i < parentMeta.count; i++) {
			                            if (strcmp(parentMeta.tuple[i].names, name) == 0) { /* If found selected name. */
			                            } else {
			                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, parentMeta.tuple[i].names); /* Copy original name to modified meta. */
			                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = parentMeta.tuple[i].isDirectories; /* Add directory state. */
			                                indexModifiedNames++;
			                            }
			                        }
			                        metaModifiedDirectory.count = indexModifiedNames;
                                    /* Apply updated data to local log. */
                                    TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                                    /* Receive remote prepare with (OK) */
                                    // TxDistributedPrepare(DistributedTxID, true);
                                    /* Start phase 2, commit it. */
                                    updateRemoteMeta(parentNodeID, &metaModifiedDirectory, parentMetaAddress, parentHashAddress);
                                    /* Only allocate momery, write to log first. */
                            		Debug::debugItem("Stage 3. Remove directory meta.");
	                                if (storage->tableDirectoryMeta->remove(indexMeta) == false) {
	                                    result = false; /* Fail due to remove error. */
	                                } else {
	                                    if (storage->hashtable->del(&hashUnique) == false) {
	                                        result = false; /* Fail due to hash table del. No roll back. */
	                                    } else {
	                                        result = true;
	                                    }
	                                }
                            	}
                                
                            }
                        }   
                    } else {
                        if (storage->tableFileMeta->get(indexMeta, metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            // DistributedTxID = TxDistributedBegin();
                            LocalTxID = TxLocalBegin();
                        	if (readDirectoryMeta(parent, &parentMeta, &parentHashAddress, &parentMetaAddress, &parentNodeID) == false) {
                        		Debug::notifyError("Remove Meta From Directory failed.");
                                // TxDistributedPrepare(DistributedTxID, false);
                        		result = false;
                        	} else {
                        		/* Remove meta from directory */
                        		DirectoryMeta metaModifiedDirectory; /* Buffer to save modified directory. */
		                        uint64_t indexModifiedNames = 0;
		                        for (uint64_t i = 0; i < parentMeta.count; i++) {
		                            if (strcmp(parentMeta.tuple[i].names, name) == 0) { /* If found selected name. */
		                            } else {
		                                strcpy(metaModifiedDirectory.tuple[indexModifiedNames].names, parentMeta.tuple[i].names); /* Copy original name to modified meta. */
		                                metaModifiedDirectory.tuple[indexModifiedNames].isDirectories = parentMeta.tuple[i].isDirectories; /* Add directory state. */
		                                indexModifiedNames++;
		                            }
		                        }
		                        metaModifiedDirectory.count = indexModifiedNames;
                                /* Apply updated data to local log. */
                                TxWriteData(LocalTxID, (uint64_t)&parentMeta, (uint64_t)sizeof(DirectoryMeta));
                                /* Receive remote prepare with (OK) */
                                // TxDistributedPrepare(DistributedTxID, true);
                                /* Start phase 2, commit it. */
                                updateRemoteMeta(parentNodeID, &metaModifiedDirectory, parentMetaAddress, parentHashAddress);
                        		/* Only allocate momery, write to log first. */
								bool resultFor = true;
	                            Debug::debugItem("Stage 3. Remove blocks.");
	                            for (uint64_t i = 0; (i < metaFile->count) && (metaFile->tuple[i].hashNode == hashNode); i++) {
	                                for (int j = (int)(metaFile->tuple[i].countExtentBlock) - 1; j >= 0; j--) {
	                                    if (storage->tableBlock->remove(metaFile->tuple[i].indexExtentStartBlock + j) == false) {
	                                        ;
	                                    }
	                                }
	                            }
	                            if (resultFor == false) {
	                                result = false; /* Fail due to block remove error. */
	                            } else {
	                                Debug::debugItem("Stage 4. Remove file meta.");
	                                if (storage->tableFileMeta->remove(indexMeta) == false) {
	                                    result = false; /* Fail due to remove error. */
	                                } else {
	                                    if (storage->hashtable->del(&hashUnique) == false) {
	                                        result = false; /* Fail due to hash table del. No roll back. */
	                                    } else {
	                                        result = true;
	                                    }
	                                }
	                            }
                        	}
                        }
                    }
		            free(parent);
		            free(name);
                }
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                // TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                // TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::remove2pc(const char *path, FileMeta *metaFile)
{
    Debug::debugTitle("FileSystem::remove");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", path);
    if (path == NULL) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUnique;
        HashTable::getUniqueHash(path, strlen(path), &hashUnique); /* Get unique hash. */
        NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash by unique hash. */
        AddressHash hashAddress = HashTable::getAddressHash(&hashUnique); /* Get address hash by unique hash. */
        uint64_t DistributedTxID;
        uint64_t LocalTxID;
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        if (checkLocal(hashNode) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNode, hashAddress); /* Lock hash item. */
            {
                uint64_t indexMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUnique, &indexMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    char *parent = (char *)malloc(strlen(path) + 1);
                    char *name = (char *)malloc(strlen(path) + 1);
                    getParentDirectory(path, parent);
                    getNameFromPath(path, name);
                    Debug::debugItem("Stage 2. Get meta.");
                    if(isDirectory)
                    {
                        DirectoryMeta metaDirectory;
                        if (storage->tableDirectoryMeta->get(indexMeta, &metaDirectory) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            metaFile->count = MAX_FILE_EXTENT_COUNT;
                            if (metaDirectory.count != 0) { /* Directory is not empty. */
                                result = false; /* Fail due to directory is not empty. */
                            } else {
                                DistributedTxID = TxDistributedBegin();
                                LocalTxID = TxLocalBegin();
                                if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                                    Debug::notifyError("Remove Meta From Directory failed.");
                                    TxDistributedPrepare(DistributedTxID, false);
                                    result = false;
                                } else {
                                    char *logData = (char *)malloc(strlen(path) + 4);
                                    sprintf(logData, "del %s", path);
                                    /* Apply updated data to local log. */
                                    TxWriteData(LocalTxID, (uint64_t)logData, (uint64_t)strlen(logData));
                                    free(logData);
                                    /* Receive remote prepare with (OK) */
                                    TxDistributedPrepare(DistributedTxID, true);
                                    /* Start phase 2, commit it. */
                                    updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                                    /* Only allocate momery, write to log first. */
                                    Debug::debugItem("Stage 3. Remove directory meta.");
                                    if (storage->tableDirectoryMeta->remove(indexMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                                
                            }
                        }   
                    } else {
                        if (storage->tableFileMeta->get(indexMeta, metaFile) == false) {
                            result = false; /* Fail due to get file meta error. */
                        } else {
                            DistributedTxID = TxDistributedBegin();
                            LocalTxID = TxLocalBegin();
                            if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                                Debug::notifyError("Remove Meta From Directory failed.");
                                TxDistributedPrepare(DistributedTxID, false);
                                result = false;
                            } else {
                                char *logData = (char *)malloc(strlen(path) + 4);
                                sprintf(logData, "del %s", path);
                                /* Apply updated data to local log. */
                                TxWriteData(LocalTxID, (uint64_t)logData, (uint64_t)strlen(logData));
                                free(logData);
                                /* Receive remote prepare with (OK) */
                                TxDistributedPrepare(DistributedTxID, true);
                                /* Start phase 2, commit it. */
                                updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
                                /* Only allocate momery, write to log first. */
                                bool resultFor = true;
                                Debug::debugItem("Stage 3. Remove blocks.");
                                for (uint64_t i = 0; (i < metaFile->count) && (metaFile->tuple[i].hashNode == hashNode); i++) {
                                    for (int j = (int)(metaFile->tuple[i].countExtentBlock) - 1; j >= 0; j--) {
                                        if (storage->tableBlock->remove(metaFile->tuple[i].indexExtentStartBlock + j) == false) {
                                            ;
                                        }
                                    }
                                }
                                if (resultFor == false) {
                                    result = false; /* Fail due to block remove error. */
                                } else {
                                    Debug::debugItem("Stage 4. Remove file meta.");
                                    if (storage->tableFileMeta->remove(indexMeta) == false) {
                                        result = false; /* Fail due to remove error. */
                                    } else {
                                        if (storage->hashtable->del(&hashUnique) == false) {
                                            result = false; /* Fail due to hash table del. No roll back. */
                                        } else {
                                            result = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    free(parent);
                    free(name);
                }
            }
            if (result == false) {
                TxLocalCommit(LocalTxID, false);
                TxDistributedCommit(DistributedTxID, false);
            } else {
                TxLocalCommit(LocalTxID, true);
                TxDistributedCommit(DistributedTxID, true);
            }
            unlockWriteHashItem(key, hashNode, hashAddress);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

bool FileSystem::blockFree(uint64_t startBlock, uint64_t countBlock)
{
    Debug::debugTitle("FileSystem::blockFree");
    Debug::debugItem("Stage 1. startBlock = %d, countBlock = %d", startBlock, countBlock);
    bool result;
    for (int j = (int)countBlock - 1; j >= 0; j--) {
        if (storage->tableBlock->remove(startBlock + j) == false) {
            result = false; /* Fail due to block remove error. */
            break;
        }
    }
    return result;
}
/* Rename file. 
   @param   pathOld     Old path.
   @param   pathNew     New path.
   @return              If operation succeeds then return true, otherwise return false. */
bool FileSystem::rename(const char *pathOld, const char *pathNew) 
{
    Debug::debugTitle("FileSystem::rename");
    Debug::debugItem("Stage 1. Entry point. Path: %s.", pathNew);
    if ((pathOld == NULL) || (pathNew == NULL)) {
        return false;                   /* Fail due to null path. */
    } else {
        UniqueHash hashUniqueOld;
        HashTable::getUniqueHash(pathOld, strlen(pathOld), &hashUniqueOld); /* Get unique hash. */
        NodeHash hashNodeOld = storage->getNodeHash(&hashUniqueOld); /* Get node hash by unique hash. */
        AddressHash hashAddressOld = HashTable::getAddressHash(&hashUniqueOld); /* Get address hash by unique hash. */
        uint64_t RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset;
        // UniqueHash hashUniqueNew;
        // HashTable::getUniqueHash(pathNew, strlen(pathNew), &hashUniqueNew); /* Get unique hash. */
        // NodeHash hashNodeNew = storage->getNodeHash(&hashUniqueNew); /* Get node hash by unique hash. */
        // AddressHash hashAddressNew = HashTable::getAddressHash(&hashUniqueNew); /* Get address hash by unique hash. */
        if (checkLocal(hashNodeOld) == true) { /* If local node. */
            bool result;
            uint64_t key = lockWriteHashItem(hashNodeOld, hashAddressOld); /* Lock hash item. */
            {
                uint64_t indexFileMeta;
                bool isDirectory;
                if (storage->hashtable->get(&hashUniqueOld, &indexFileMeta, &isDirectory) == false) { /* If path does not exist. */
                    result = false; /* Fail due to existence of path. */
                } else {
                    Debug::debugItem("Stage 2. Get meta.");
                    if (isDirectory == true) { /* If directory meta. */
                        result = false; /* Fail due to directory rename is not supported. */
                    } else {
                    	char *parent = (char *)malloc(strlen(pathOld) + 1);
                    	char *name = (char *)malloc(strlen(pathOld) + 1);
                    	getParentDirectory(pathOld, parent);
                    	getNameFromPath(pathOld, name);
                    	if (removeMetaFromDirectory(parent, name, &RemoteTxID, &srcBuffer, &desBuffer, &size, &remotekey, &offset) == false) {
                    		result = false;
                    	} else {
							FileMeta metaFile;
	                        if (storage->tableFileMeta->get(indexFileMeta, &metaFile) == false) {
	                            result = false; /* Fail due to get file meta error. */
	                        } else{
                                updateDirectoryMeta(parent, RemoteTxID, srcBuffer, desBuffer, size, remotekey, offset);
	                            Debug::debugItem("Stage 4. Remove file meta.");
	                            if (storage->tableFileMeta->remove(indexFileMeta) == false) {
	                                result = false; /* Fail due to remove error. */
	                            } else {
	                                if (storage->hashtable->del(&hashUniqueOld) == false) {
	                                    result = false; /* Fail due to hash table del. No roll back. */
	                                } else {
	                                    result = true;
	                                }
	                            }
	                        }
                    	}
                    	free(parent);
                    	free(name);
                    }
                }
            }
            unlockWriteHashItem(key, hashNodeOld, hashAddressOld);  /* Unlock hash item. */
            Debug::debugItem("Stage end.");
            return result;              /* Return specific result. */
        } else {                        /* If remote node. */
            return false;
        }
    }
}

/* Initialize root directory. 
   @param   LocalNode     Local Node ID.
*/
void FileSystem::rootInitialize(NodeHash LocalNode)
{
    this->hashLocalNode = LocalNode; /* Assign local node hash. */
    /* Add root directory. */
    UniqueHash hashUnique;
    HashTable::getUniqueHash("/", strlen("/"), &hashUnique);
    NodeHash hashNode = storage->getNodeHash(&hashUnique); /* Get node hash. */
    Debug::debugItem("root node: %d", (int)hashNode);
    if (hashNode == this->hashLocalNode) { /* Root directory is here. */
        Debug::notifyInfo("Initialize root directory.");
        DirectoryMeta metaDirectory;
        metaDirectory.count = 0;    /* Initialize count of files in root directory is 0. */
        uint64_t indexDirectoryMeta;
        if (storage->tableDirectoryMeta->create(&indexDirectoryMeta, &metaDirectory) == false) {
            fprintf(stderr, "FileSystem::FileSystem: create directory meta error.\n");
            exit(EXIT_FAILURE);             /* Exit due to parameter error. */
        } else {
            if (storage->hashtable->put("/", indexDirectoryMeta, true) == false) { /* true for directory. */
                fprintf(stderr, "FileSystem::FileSystem: hashtable put error.\n");
                exit(EXIT_FAILURE);             /* Exit due to parameter error. */
            } else {
                ;
            }
        }
    }
}

/* Constructor of file system. 
   @param   buffer              Buffer of memory.
   @param   countFile           Max count of file.
   @param   countDirectory      Max count of directory.
   @param   countBlock          Max count of blocks. 
   @param   countNode           Max count of nodes.
   @param   hashLocalNode       Local node hash. From 1 to countNode. */
FileSystem::FileSystem(char *buffer, char *bufferBlock, uint64_t countFile,
                       uint64_t countDirectory, uint64_t countBlock, 
                       uint64_t countNode, NodeHash hashLocalNode)
{
    if ((buffer == NULL) || (bufferBlock == NULL) || (countFile == 0) || (countDirectory == 0) ||
        (countBlock == 0) || (countNode == 0) || (hashLocalNode < 1) || 
        (hashLocalNode > countNode)) {
        fprintf(stderr, "FileSystem::FileSystem: parameter error.\n");
        exit(EXIT_FAILURE);             /* Exit due to parameter error. */
    } else {
        this->addressHashTable = (uint64_t)buffer;
        storage = new Storage(buffer, bufferBlock, countFile, countDirectory, countBlock, countNode); /* Initialize storage instance. */
        lock = new LockService((uint64_t)buffer);
    }
}
/* Destructor of file system. */
FileSystem::~FileSystem()
{
    delete storage;                     /* Release storage instance. */
}
