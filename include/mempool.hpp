#ifndef MEMPOOL_HEADER
#define MEMPOOL_HEADER

#include <unordered_map>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <debug.hpp>
#include "global.h"
#define SHARE_MEMORY_KEY 78

/************************************************************************************************
	+-------+-------+-----+-------+-------------+-------------+----------+------------+---------+
	| Cli_1 | Cli_2 | ... | Cli_N | SERVER_SEND | SERVER_RECV | MetaData | Data_block | LogFile |
	+-------+-------+-----+-------+-------------+-------------+----------+------------+---------+
								 /				 \
	  --------------------------/				  \---------------------------
	/																		  \
	+-----------------------+-----------------------+-----+-----------------------+
	|  Ser_1 (1, 2, ... 8)  |  Ser_2 (1, 2, ... 8)  | ... |  Ser_M (1, 2, ... 8)  | 
	+-----------------------+-----------------------+-----+-----------------------+
************************************************************************************************/
typedef unordered_map<uint32_t, int> Thread2ID;
class MemoryManager {
private:
	uint64_t ServerCount;
	uint64_t MemoryBaseAddress;
	uint64_t ClientBaseAddress;
	uint64_t ServerSendBaseAddress;
	uint64_t ServerRecvBaseAddress;
	uint64_t MetadataBaseAddress;
	uint64_t DataBaseAddress;
	uint8_t *SendPoolPointer;
	uint64_t DMFSTotalSize;
	uint64_t LocalLogAddress;
	uint64_t DistributedLogAddress;
	int shmid;
	Thread2ID th2id;
public:
	MemoryManager(uint64_t mm, uint64_t ServerCount, int DataSize);
	~MemoryManager();
	uint64_t getDmfsBaseAddress();
	uint64_t getDmfsTotalSize();
	uint64_t getMetadataBaseAddress();
	uint64_t getDataAddress();
	uint64_t getServerSendAddress(uint16_t NodeID, uint64_t *buffer);
	uint64_t getServerRecvAddress(uint16_t NodeID, uint16_t offset);
	uint64_t getClientMessageAddress(uint16_t NodeID);
	uint64_t getLocalLogAddress();
	uint64_t getDistributedLogAddress();
	void setID(int ID);
};

#endif