#include "lock.h"
#include "RPCServer.hpp"
extern RPCServer *server;
LockService::LockService(uint64_t _MetaDataBaseAddress)
: MetaDataBaseAddress(_MetaDataBaseAddress){}

LockService::~LockService(){}

uint64_t LockService::WriteLock(uint16_t NodeID, uint64_t Address) {
    int workerid = server->getIDbyTID();
    uint16_t ID = __sync_fetch_and_add(&WriteID, 1ULL);
    uint64_t key = (uint64_t)NodeID;
    uint64_t LockAddress = MetaDataBaseAddress + Address;
    key = key << 16;
    key += ID;
    key = key << 32;
    while (true) {
        if (__sync_bool_compare_and_swap((uint64_t *)LockAddress, 0ULL, key))
            break;
        //if (workerid == 0) {
            server->RequestPoller(workerid);
        //}
    }
        return key;
}

bool LockService::WriteUnlock(uint64_t key, uint16_t NodeID, uint64_t Address) {
    uint64_t LockAddress = MetaDataBaseAddress + Address;
    uint32_t *p = (uint32_t *)(LockAddress + 4);
    *p = 0;
    return true;
}

uint64_t LockService::ReadLock(uint16_t NodeID, uint64_t Address) {
    uint64_t LockAddress = MetaDataBaseAddress + Address;
    uint64_t preNumber = __sync_fetch_and_add((uint64_t*)LockAddress, 1ULL);
    preNumber = preNumber >> 32;
    if (preNumber == 0) {
        return 1;
    } else {
        while (true) {
            if (__sync_bool_compare_and_swap((uint32_t*)(LockAddress + 4), 0, 0))
                break;
            usleep(1);
        }
    }
    return 1;
}

bool LockService::ReadUnlock(uint64_t key, uint16_t NodeID, uint64_t Address) {
    uint64_t LockAddress = MetaDataBaseAddress + Address;
    __sync_fetch_and_sub((uint64_t *)LockAddress, 1);
    return true;
}
