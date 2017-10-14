#ifndef TXMANAGER_HEADER
#define TXMANAGER_HEADER

#include <stdint.h>
#include <mutex>
#include <string.h>

using namespace std;

#define CACHELINE_SIZE (64)

#define _mm_clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))

typedef struct  {
    uint64_t TxID;
    bool begin;
    bool prepare;
    bool commit;
} __attribute__((packed)) DistributedLogEntry;

typedef struct {
    uint64_t TxID;
    bool begin;
    char logData[4086];
    bool commit;
} __attribute__((packed)) LocalLogEntry;

class TxManager {
private:
	uint64_t LocalLogAddress;
	uint64_t DistributedLogAddress;
	uint64_t LocalLogIndex;
	uint64_t DistributedLogIndex;
	mutex LocalMutex;
	mutex DisMutex;
public:
	TxManager(uint64_t LocalLogAddress, uint64_t DistributedLogAddress);
	~TxManager();
	void FlushData(uint64_t address, uint64_t size);
	uint64_t TxLocalBegin();
	void TxWriteData(uint64_t TxID, uint64_t address, uint64_t size);
	uint64_t getTxWriteDataAddress(uint64_t txID);
	void TxLocalCommit(uint64_t TxID, bool action);
	uint64_t TxDistributedBegin();
	void TxDistributedPrepare(uint64_t TxID, bool action);
	void TxDistributedCommit(uint64_t TxID, bool action);
};

#endif