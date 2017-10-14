#ifndef RPCCLINET_HREADER
#define RPCCLINET_HREADER
#include <thread>
#include "RdmaSocket.hpp"
#include "Configuration.hpp"
#include "mempool.hpp"
#include "global.h"

using namespace std;
class RPCClient {
private:
	Configuration *conf;
	RdmaSocket *socket;
	MemoryManager *mem;
	bool isServer;
	uint32_t taskID;
public:
	uint64_t mm;
	RPCClient(Configuration *conf, RdmaSocket *socket, MemoryManager *mem, uint64_t mm);
	RPCClient();
	~RPCClient();
	RdmaSocket* getRdmaSocketInstance();
	Configuration* getConfInstance();
	bool RdmaCall(uint16_t DesNodeID, char *bufferSend, uint64_t lengthSend, char *bufferReceive, uint64_t lengthReceive);
	uint64_t ContractSendBuffer(GeneralSendBuffer *send);
};

#endif