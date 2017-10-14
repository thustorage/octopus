#include "RPCServer.hpp"
// __thread struct  timeval startt, endd;
RPCServer::RPCServer(int _cqSize) :cqSize(_cqSize) {
	mm = 0;
	UnlockWait = false;
	conf = new Configuration();
	mem = new MemoryManager(mm, conf->getServerCount(), 2);
	mm = mem->getDmfsBaseAddress();
	Debug::notifyInfo("DmfsBaseAddress = %lx, DmfsTotalSize = %lx",
		mem->getDmfsBaseAddress(), mem->getDmfsTotalSize());
	ServerCount = conf->getServerCount();
	socket = new RdmaSocket(cqSize, mm, mem->getDmfsTotalSize(), conf, true, 0);
	client = new RPCClient(conf, socket, mem, (uint64_t)mm);
	tx = new TxManager(mem->getLocalLogAddress(), mem->getDistributedLogAddress());
	socket->RdmaListen();
	fs = new FileSystem((char *)mem->getMetadataBaseAddress(),
              (char *)mem->getDataAddress(),
              1024 * 20,/* Constructor of file system. */
              1024 * 30,
              2000,
              conf->getServerCount(),    
              socket->getNodeID());
	fs->rootInitialize(socket->getNodeID());
	wk = new thread[cqSize]();
	for (int i = 0; i < cqSize; i++)
		wk[i] = thread(&RPCServer::Worker, this, i);
}
RPCServer::~RPCServer() {
	Debug::notifyInfo("Stop RPCServer.");
	delete conf;
	for (int i = 0; i < cqSize; i++) {
		wk[i].detach();
	}
	delete mem;
	delete wk;
	delete socket;
	delete tx;
	Debug::notifyInfo("RPCServer is closed successfully.");
}

RdmaSocket* RPCServer::getRdmaSocketInstance() {
	return socket;
}

MemoryManager* RPCServer::getMemoryManagerInstance() {
	return mem;
}

RPCClient* RPCServer::getRPCClientInstance() {
	return client;
}

TxManager* RPCServer::getTxManagerInstance() {
	return tx;
}

void RPCServer::Worker(int id) {
	uint32_t tid = gettid();
	// gettimeofday(&startt, NULL);
	Debug::notifyInfo("Worker %d, tid = %d", id, tid);
	th2id[tid] = id;
	mem->setID(id);
	while (true) {
		RequestPoller(id);
	}
}

void RPCServer::RequestPoller(int id) {
	struct ibv_wc wc[1];
	uint16_t NodeID;
	uint16_t offset;
	int ret = 0, count = 0;
	uint64_t bufferRecv;
	// unsigned long diff;
	ret = socket->PollOnce(id, 1, wc);
	if (ret <= 0) {
		/*gettimeofday(&endd, NULL);
		diff = 1000000 * (endd.tv_sec - startt.tv_sec) + endd.tv_usec - startt.tv_usec;
		if (diff > 2000000) {
			printf("ID = %d, Polling, ret = %d\n", id, ret);
			diff = 0;
			gettimeofday(&startt, NULL);
			uint64_t bufferRecv = mem->getClientMessageAddress(2);
			ExtentWriteSendBuffer *send = (ExtentWriteSendBuffer *)bufferRecv;
			socket->RdmaReceive(2, mm + 2 * 4096, 0);
			socket->RdmaReceive(2, mm + 2 * 4096, 0);
			Debug::debugItem("Path = %s, size = %x, offset = %x", send->path, send->size, send->offset);
		}*/
		return;
	} else if (wc[0].opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
		NodeID = wc[0].imm_data >> 20;
		if (NodeID == 0XFFF) {
			/* Unlock request, process it directly. */
			// uint64_t hashAddress = wc[0].imm_data & 0x000FFFFF;
			// fs->unlockWriteHashItem(0, 0, hashAddress);
			return;
		}
		NodeID = (uint16_t)(wc[0].imm_data << 16 >> 16);
		offset = (uint16_t)(wc[0].imm_data >> 16);
		Debug::debugItem("NodeID = %d, offset = %d", NodeID, offset);
		count += 1;
		if (NodeID > 0 && NodeID <= ServerCount) {
			/* Recv Message From Other Server. */
			bufferRecv = mem->getServerRecvAddress(NodeID, offset);
		} else if (NodeID > ServerCount) {
			/* Recv Message From Client. */
			bufferRecv = mem->getClientMessageAddress(NodeID);
		}
		GeneralSendBuffer *send = (GeneralSendBuffer*)bufferRecv;
		switch (send->message) {
			case MESSAGE_TEST: {

			}
			default: {
				// if (id == 0 && UnlockWait == false && (send->message == MESSAGE_ADDMETATODIRECTORY || send->message == MESSAGE_REMOVEMETAFROMDIRECTORY)) {
				// 	/* 
				// 	* When process addmeta or remove meta, lock will be added without release,
				// 	* So we will wait until update meta arrives.
				// 	*/
				// 	UnlockWait = true;
				// 	ProcessRequest(send, NodeID, offset);
				// 	return;
				// } else if (id == 0 && send->message == MESSAGE_DOCOMMIT) {
				// 	UnlockWait = false;
				// 	ProcessRequest(send, NodeID, offset);
				// 	printf("a\n");
				// 	ProcessQueueRequest();
				// 	printf("b\n");
				// 	return;
				// }else if (id == 0 && UnlockWait == true) {
				// 	/* Just push the requests into the queue and return. */
				// 	RPCTask *task = (RPCTask *)malloc(sizeof(RPCTask));
				// 	task->send = (uint64_t)send;
				// 	task->NodeID = NodeID;
				// 	task->offset = offset;
				// 	tasks.push_back(task);
				// 	// printf("process docommit end.");
				// 	return;
				// }
				ProcessRequest(send, NodeID, offset);
				// printf("id = %d,end\n", id);
			}
		}
		
	}
}

void RPCServer::ProcessQueueRequest() {
	for (auto task = tasks.begin(); task != tasks.end(); ) {
		// printf("1\n");
		ProcessRequest((GeneralSendBuffer *)(*task)->send, (*task)->NodeID, (*task)->offset);
		free(*task);
		task = tasks.erase(task);
	}
	// printf("2\n");
}

void RPCServer::ProcessRequest(GeneralSendBuffer *send, uint16_t NodeID, uint16_t offset) {
	char receiveBuffer[CLIENT_MESSAGE_SIZE];
	uint64_t bufferRecv = (uint64_t)send;
	GeneralReceiveBuffer *recv = (GeneralReceiveBuffer*)receiveBuffer;
	recv->taskID = send->taskID;
	recv->message = MESSAGE_RESPONSE;
	uint64_t size = send->sizeReceiveBuffer;
	if (send->message == MESSAGE_DISCONNECT) {
        //rdma->disconnect(send->sourceNodeID);
        return;
    } else if (send->message == MESSAGE_TEST) {
    	;
    } else if (send->message == MESSAGE_UPDATEMETA) {
    	/* Write unlock. */
    	// UpdateMetaSendBuffer *bufferSend = (UpdateMetaSendBuffer *)send;
    	// fs->unlockWriteHashItem(bufferSend->key, NodeID, bufferSend->offset);
    	return;
    } else if (send->message == MESSAGE_EXTENTREADEND) {
    	/* Read unlock */
    	// ExtentReadEndSendBuffer *bufferSend = (ExtentReadEndSendBuffer *)send;
    	// fs->unlockReadHashItem(bufferSend->key, NodeID, bufferSend->offset);
    	return;
	} else {
    	fs->parseMessage((char*)send, receiveBuffer);
    	// fs->recursivereaddir("/", 0);
	Debug::debugItem("Contract Receive Buffer, size = %d.", size);
	size -= ContractReceiveBuffer(send, recv);
    	if (send->message == MESSAGE_RAWREAD) {
    		ExtentReadSendBuffer *bufferSend = (ExtentReadSendBuffer *)send;
    		uint64_t *value = (uint64_t *)mem->getDataAddress();
    		// printf("rawread size = %d\n", (int)bufferSend->size);
    		*value = 1;
    		socket->RdmaWrite(NodeID, mem->getDataAddress(), 2 * 4096, bufferSend->size, -1, 1);
    	} else if (send->message == MESSAGE_RAWWRITE) {
    		ExtentWriteSendBuffer *bufferSend = (ExtentWriteSendBuffer *)send;
    		// printf("rawwrite size = %d\n", (int)bufferSend->size);
    		uint64_t *value = (uint64_t *)mem->getDataAddress();
    		*value = 0;
    		socket->RdmaRead(NodeID, mem->getDataAddress(), 2 * 4096, bufferSend->size, 1); // FIX ME.
    		while (*value == 0);
    	}
	Debug::debugItem("Copy Reply Data, size = %d.", size);
    	memcpy((void *)send, receiveBuffer, size);
	Debug::debugItem("Select Buffer.");
    	if (NodeID > 0 && NodeID <= ServerCount) {
			/* Recv Message From Other Server. */
			bufferRecv = bufferRecv - mm;
		} else if (NodeID > ServerCount) {
			/* Recv Message From Client. */
			bufferRecv = 0;
		} 
		Debug::debugItem("send = %lx, recv = %lx", send, bufferRecv);
    		socket->_RdmaBatchWrite(NodeID, (uint64_t)send, bufferRecv, size, 0, 1);
		// socket->_RdmaBatchReceive(NodeID, mm, 0, 2);
		socket->RdmaReceive(NodeID, mm + NodeID * 4096, 0);
		// printf("process end\n");
    }
}

int RPCServer::getIDbyTID() {
	uint32_t tid = gettid();
	return th2id[tid];
}
uint64_t RPCServer::ContractReceiveBuffer(GeneralSendBuffer *send, GeneralReceiveBuffer *recv) {
	uint64_t length;
	switch (send->message) {
		case MESSAGE_GETATTR: {
			GetAttributeReceiveBuffer *bufferRecv = 
			(GetAttributeReceiveBuffer *)recv;
			if (bufferRecv->attribute.count >= 0 && bufferRecv->attribute.count < MAX_FILE_EXTENT_COUNT)
				length = (MAX_FILE_EXTENT_COUNT - bufferRecv->attribute.count) * sizeof(FileMetaTuple);
			else 
				length = sizeof(FileMetaTuple) * MAX_FILE_EXTENT_COUNT;
			break;
		}
		case MESSAGE_READDIR: {
			ReadDirectoryReceiveBuffer *bufferRecv = 
			(ReadDirectoryReceiveBuffer *)recv;
			if (bufferRecv->list.count >= 0 && bufferRecv->list.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->list.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		case MESSAGE_EXTENTREAD: {
			ExtentReadReceiveBuffer *bufferRecv = 
			(ExtentReadReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_EXTENTWRITE: {
			ExtentWriteReceiveBuffer *bufferRecv = 
			(ExtentWriteReceiveBuffer *)recv;
			if (bufferRecv->fpi.len >= 0 && bufferRecv->fpi.len <= MAX_MESSAGE_BLOCK_COUNT)
				length = (MAX_MESSAGE_BLOCK_COUNT - bufferRecv->fpi.len) * sizeof(file_pos_tuple);
			else 
				length = MAX_MESSAGE_BLOCK_COUNT * sizeof(file_pos_tuple);
			break;
		}
		case MESSAGE_READDIRECTORYMETA: {
			ReadDirectoryMetaReceiveBuffer *bufferRecv = 
			(ReadDirectoryMetaReceiveBuffer *)recv;
			if (bufferRecv->meta.count >= 0 && bufferRecv->meta.count <= MAX_DIRECTORY_COUNT)
				length = (MAX_DIRECTORY_COUNT - bufferRecv->meta.count) * sizeof(DirectoryMetaTuple);
			else 
				length = MAX_DIRECTORY_COUNT * sizeof(DirectoryMetaTuple);
			break;
		}
		default: {
			length = 0;
			break;
		}
	}	
	// printf("contract length = %d", (int)length);
	return length;
}
