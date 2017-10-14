/** Redundance check. **/
#ifndef GLOBAL_HEADER
#define GLOBAL_HEADER
#include <stdint.h>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <sys/syscall.h>
#include <unistd.h>
#include "common.hpp"

using namespace std;

static inline uint32_t gettid() {
    return (uint32_t)syscall(SYS_gettid);
}

#define CLIENT_MESSAGE_SIZE 4096
#define MAX_CLIENT_NUMBER   1024
#define SERVER_MASSAGE_SIZE CLIENT_MESSAGE_SIZE
#define SERVER_MASSAGE_NUM 8
#define METADATA_SIZE (1024 * 1024 * 1024)
#define LOCALLOGSIZE (40 * 1024 * 1024)
#define DISTRIBUTEDLOGSIZE (1024 * 1024)

// #define TRANSACTION_2PC 1
#define TRANSACTION_CD 1

/** Enumerators and structures. **/
typedef enum {                          /* Message enumerator. */
    MESSAGE_ADDMETATODIRECTORY,
    MESSAGE_REMOVEMETAFROMDIRECTORY,
    MESSAGE_MKNODWITHMETA,
    MESSAGE_MKNOD,
    MESSAGE_GETATTR,
    MESSAGE_ACCESS,
    MESSAGE_MKDIR,
    MESSAGE_READDIR,
    MESSAGE_EXTENTREAD,
    MESSAGE_EXTENTWRITE,
    MESSAGE_UPDATEMETA,
    MESSAGE_EXTENTREADEND,
    MESSAGE_TRUNCATE,
    MESSAGE_RMDIR,
    MESSAGE_REMOVE,
    MESSAGE_FREEBLOCK,
    MESSAGE_RENAME,
    MESSAGE_RESPONSE,
    MESSAGE_DISCONNECT,
    MESSAGE_TEST,
    MESSAGE_RAWWRITE,
    MESSAGE_RAWREAD,
    MESSAGE_DOCOMMIT,
    MESSAGE_READDIRECTORYMETA,
    MESSAGE_INVALID
} Message;

typedef struct {                        /* Extra information structure. */
    uint16_t sourceNodeID;              /* Source node ID. */
    uint64_t taskID;                    /* Task ID. */
    uint64_t sizeReceiveBuffer;         /* Size of receive buffer. */
} ExtraInformation;

typedef struct : ExtraInformation {     /* General send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
} GeneralSendBuffer;
typedef struct : ExtraInformation {
	Message message;
	uint64_t startBlock;
	uint64_t countBlock;
} BlockFreeSendBuffer;

typedef struct : ExtraInformation {     /* General receive buffer structure. */
	Message message;                    /* Message type. */
    bool result;                        /* Result. */
} GeneralReceiveBuffer;

typedef struct : ExtraInformation {     /* addMetaToDirectory send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    char name[MAX_FILE_NAME_LENGTH];    /* Name to add. */
    bool isDirectory;                   /* Is directory or not. */
} AddMetaToDirectorySendBuffer;

typedef struct : ExtraInformation {     /* removeMetaFromDirectory send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    char name[MAX_FILE_NAME_LENGTH];    /* Name to add. */
} RemoveMetaFromDirectorySendBuffer;

typedef struct : ExtraInformation {     /* extentRead send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    uint64_t size;                      /* Size to read. */
    uint64_t offset;                    /* Offset to read. */
} ExtentReadSendBuffer;

typedef struct : ExtraInformation {     /* extentWrite send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    uint64_t size;                      /* Size to read. */
    uint64_t offset;                    /* Offset to read. */
} ExtentWriteSendBuffer;

typedef struct : ExtraInformation {     /* updateMeta send buffer structure. */
    Message message;                    /* Message type. */
    uint64_t offset;
    uint64_t key;                       /* Key to unlock. */
} UpdateMetaSendBuffer;

typedef struct : ExtraInformation {     /* extentReadEnd send buffer structure. */
    Message message;                    /* Message type. */
    uint64_t offset;
    uint64_t key;                       /* Key to unlock. */
} ExtentReadEndSendBuffer;

typedef struct : ExtraInformation {     /* mknodWithMeta send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    FileMeta metaFile;                  /* File meta. */
} MakeNodeWithMetaSendBuffer;

typedef struct : ExtraInformation {     /* truncate send buffer structure. */
    Message message;                    /* Message type. */
    char path[MAX_PATH_LENGTH];         /* Path. */
    uint64_t size;                      /* Size to kept after truncation. */
} TruncateSendBuffer;

typedef struct : ExtraInformation {     /* rename send buffer structure. */
    Message message;                    /* Message type. */
    char pathOld[MAX_PATH_LENGTH];      /* Old path. */
    char pathNew[MAX_PATH_LENGTH];      /* New path. */
} RenameSendBuffer;

typedef struct : ExtraInformation {     /* getattr receive buffer structure. */
    Message message;                    /* Message type. */
    bool result;                        /* Result. */
    FileMeta attribute;             	/* Attribute. */
} GetAttributeReceiveBuffer;

typedef struct : ExtraInformation {     /* readdir receive buffer structure. */
    Message message;                    /* Message type. */
    bool result;                        /* Result. */
    nrfsfilelist list;                  /* List. */
} ReadDirectoryReceiveBuffer;

typedef struct : ExtraInformation {     /* extentRead receive buffer structure. */
    Message message;                    /* Message type. */
    bool result;                        /* Result. */
	uint64_t offset;
    uint64_t key;                       /* Key to unlock. */
    file_pos_info fpi;                  /* File position information. */
} ExtentReadReceiveBuffer;

typedef struct : ExtraInformation {     /* extentWrite receive buffer structure. */
    Message message;                    /* Message type. */
    bool result;                        /* Result. */
	uint64_t offset;
    //FileMeta metaFile;                  /* File meta. */
    uint64_t key;                       /* Key to unlock. */
    file_pos_info fpi;                  /* File position information. */
} ExtentWriteReceiveBuffer;

typedef struct : GeneralReceiveBuffer {
    uint64_t TxID;
    uint64_t srcBuffer;
    uint64_t desBuffer;
    uint64_t size;
    uint64_t key;
    uint64_t offset;
} UpdataDirectoryMetaReceiveBuffer;

typedef struct : UpdataDirectoryMetaReceiveBuffer {
    char path[MAX_PATH_LENGTH];
} DoRemoteCommitSendBuffer;

typedef struct : GeneralReceiveBuffer {
    uint64_t hashAddress;
    uint64_t metaAddress;
    uint16_t parentNodeID;
    DirectoryMeta meta;
} ReadDirectoryMetaReceiveBuffer;

/* A global queue manager. */
template <typename T>
class Queue {
private:
    std::vector<T> queue;
    std::mutex m;
    std::condition_variable cond;
    uint8_t offset = 0;
public:
    Queue(){}
    ~Queue(){}
    T pop() {
        std::unique_lock<std::mutex> mlock(m);
        while (queue.empty()) {
            cond.wait(mlock);
        }
        auto item = queue.front();
        queue.erase(queue.begin());
        return item;
    }
    T PopPolling() {
        while (offset == 0);
        auto item = queue.front();
        queue.erase(queue.begin());
         __sync_fetch_and_sub(&offset, 1);
         return item;
    }
    void push(T item) {
        std::unique_lock<std::mutex> mlock(m);
        queue.push_back(item);
        mlock.unlock();
        cond.notify_one();
    }
    void PushPolling(T item) {
        queue.push_back(item);
        __sync_fetch_and_add(&offset, 1);
    }
};

/** Redundance check. **/
#endif
