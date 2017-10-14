#ifndef LOCK_HEADER
#define LOCK_HEADER

#include <stdint.h>
#include <unistd.h>

class LockService {
private:
	uint16_t WriteID;
	uint16_t ReadID;
	uint64_t MetaDataBaseAddress;
public:
	LockService(uint64_t MetaDataBaseAddress);
	~LockService();
	uint64_t WriteLock(uint16_t NodeID, uint64_t Address);
	bool WriteUnlock(uint64_t key, uint16_t NodeID, uint64_t Address);
	uint64_t ReadLock(uint16_t NodeID, uint64_t Address);
	bool ReadUnlock(uint64_t key, uint16_t NodeID, uint64_t Address);
};

#endif