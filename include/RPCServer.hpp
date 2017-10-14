#ifndef RPCSERVER_HREADER
#define RPCSERVER_HREADER
#include <thread>
#include <unordered_map>
#include <vector>
#include "RdmaSocket.hpp"
#include "Configuration.hpp"
#include "RPCClient.hpp"
#include "mempool.hpp"
#include "global.h"
#include "filesystem.hpp"
#include "TxManager.hpp"

using namespace std;

typedef unordered_map<uint32_t, int> Thread2ID;

typedef struct {
	uint64_t send;
	uint16_t NodeID;
	uint16_t offset;
} RPCTask;

class RPCServer {
private:
	thread *wk;
	Configuration *conf;
	RdmaSocket *socket;
	MemoryManager *mem;
	uint64_t mm;
	TxManager *tx;
	RPCClient *client;
	int ServerCount;
	FileSystem *fs;
	int cqSize;
	Thread2ID th2id;
	vector<RPCTask*> tasks;
	bool UnlockWait;
	void Worker(int id);
	void ProcessRequest(GeneralSendBuffer *send, uint16_t NodeID, uint16_t offset);
	void ProcessQueueRequest();
public:
	RPCServer(int cqSize);
	RdmaSocket* getRdmaSocketInstance();
	MemoryManager* getMemoryManagerInstance();
	RPCClient* getRPCClientInstance();
	TxManager* getTxManagerInstance();
	uint64_t ContractReceiveBuffer(GeneralSendBuffer *send, GeneralReceiveBuffer *recv);
	void RequestPoller(int id);
	int getIDbyTID();
	~RPCServer();
};

#endif