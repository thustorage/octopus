#include "TxManager.hpp"

using namespace std;

TxManager::TxManager(uint64_t _LocalLogAddress, uint64_t _DistributedLogAddress) 
: LocalLogAddress(_LocalLogAddress), DistributedLogAddress(_DistributedLogAddress){
	LocalLogIndex = 0;
	DistributedLogIndex = 0;
}

TxManager::~TxManager(){}

void TxManager::FlushData(uint64_t address, uint64_t size) {
	uint32_t i;
	size = size + ((unsigned long)(address) & (CACHELINE_SIZE - 1));
	for (i = 0; i < size; i += CACHELINE_SIZE) {
		_mm_clflush((void *)(address + i));
	}
}

uint64_t TxManager::TxLocalBegin() {
	lock_guard<mutex> lck (LocalMutex);
	LocalLogEntry *log = (LocalLogEntry *)LocalLogAddress;
	log[LocalLogIndex].TxID = LocalLogIndex;
	log[LocalLogIndex].begin = true;
	FlushData((uint64_t)&log[LocalLogIndex], CACHELINE_SIZE);
	LocalLogIndex += 1;
	return (LocalLogIndex - 1);
}

void TxManager::TxWriteData(uint64_t TxID, uint64_t buffer, uint64_t size) {
	LocalLogEntry *log = (LocalLogEntry *)LocalLogAddress;
	memcpy((void *)log[TxID].logData, (void *)buffer, size);
	FlushData((uint64_t)log[TxID].logData, size);
}

uint64_t TxManager::getTxWriteDataAddress(uint64_t TxID) {
	LocalLogEntry *log = (LocalLogEntry *)LocalLogAddress;
	return (uint64_t)log[TxID].logData;
}

void TxManager::TxLocalCommit(uint64_t TxID, bool action) {
	LocalLogEntry *log = (LocalLogEntry *)LocalLogAddress;
	log[TxID].commit = action;
	FlushData((uint64_t)&log[TxID].commit, CACHELINE_SIZE);
}

uint64_t TxManager::TxDistributedBegin() {
	lock_guard<mutex> lck (DisMutex);
	DistributedLogEntry *log = (DistributedLogEntry *)DistributedLogAddress;
	log[DistributedLogIndex].TxID = DistributedLogIndex;
	log[DistributedLogIndex].begin = true;
	FlushData((uint64_t)&log[DistributedLogIndex], CACHELINE_SIZE);
	DistributedLogIndex += 1;
	return (DistributedLogIndex - 1);
}

void TxManager::TxDistributedPrepare(uint64_t TxID, bool action) {
	DistributedLogEntry *log = (DistributedLogEntry *)DistributedLogAddress;
	log[TxID].prepare = action;
	FlushData((uint64_t)&log[TxID].prepare, CACHELINE_SIZE);
}

void TxManager::TxDistributedCommit(uint64_t TxID, bool action) {
	DistributedLogEntry *log = (DistributedLogEntry *)DistributedLogAddress;
	log[TxID].commit = action;
	FlushData((uint64_t)&log[TxID].commit, CACHELINE_SIZE);
}