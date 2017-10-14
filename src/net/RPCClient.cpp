#include "RPCClient.hpp"
#include "TxManager.hpp"

RPCClient::RPCClient(Configuration *_conf, RdmaSocket *_socket, MemoryManager *_mem, uint64_t _mm)
:conf(_conf), socket(_socket), mem(_mem), mm(_mm) {
	isServer = true;
	taskID  = 1;
}

RPCClient::RPCClient() {
	isServer = false;
	taskID = 1;
	mm = (uint64_t)malloc(sizeof(char) * (1024 * 4 + 1024 * 1024 * 4));
	conf = new Configuration();
	socket = new RdmaSocket(1, mm, (1024 * 4 + 1024 * 1024 * 4), conf, false, 0);
	socket->RdmaConnect();
}

RPCClient::~RPCClient() {
	Debug::notifyInfo("Stop RPCClient.");
	if (!isServer) {
		delete conf;
		delete socket;
		free((void *)mm);
	}
	Debug::notifyInfo("RPCClient is closed successfully.");
}

RdmaSocket* RPCClient::getRdmaSocketInstance() {
	return socket;
}

Configuration* RPCClient::getConfInstance() {
	return conf;
}

bool RPCClient::RdmaCall(uint16_t DesNodeID, char *bufferSend, uint64_t lengthSend, char *bufferReceive, uint64_t lengthReceive) {
	uint32_t ID = __sync_fetch_and_add( &taskID, 1 ), temp;
	uint64_t sendBuffer, receiveBuffer, remoteRecvBuffer;
	uint16_t offset = 0;
	uint32_t imm = (uint32_t)socket->getNodeID();
	// struct  timeval startt, endd;
	// unsigned long diff, tempCount = 0;
	GeneralSendBuffer *send = (GeneralSendBuffer*)bufferSend;
	lengthReceive -= ContractSendBuffer(send);
	send->taskID = ID;
	send->sourceNodeID = socket->getNodeID();
	send->sizeReceiveBuffer = lengthReceive;
	if (isServer) {
		offset = mem->getServerSendAddress(DesNodeID, &sendBuffer);
		// printf("offset = %d\n", offset);
		receiveBuffer = mem->getServerRecvAddress(socket->getNodeID(), offset);
		remoteRecvBuffer = receiveBuffer - mm;
	} else {
		sendBuffer = mm;
		receiveBuffer = mm;
		remoteRecvBuffer = (socket->getNodeID() - conf->getServerCount() - 1) * CLIENT_MESSAGE_SIZE;
	}
	GeneralReceiveBuffer *recv = (GeneralReceiveBuffer*)receiveBuffer;
	if (isServer)
		recv->message = MESSAGE_INVALID;
	memcpy((void *)sendBuffer, (void *)bufferSend, lengthSend);
	_mm_clflush(recv);
	asm volatile ("sfence\n" : : );
	temp = (uint32_t)offset;
	imm = imm + (temp << 16);
	Debug::debugItem("sendBuffer = %lx, receiveBuffer = %lx, remoteRecvBuffer = %lx, ReceiveSize = %d", 
		sendBuffer, receiveBuffer, remoteRecvBuffer, lengthReceive);
	if (send->message == MESSAGE_DISCONNECT
		|| send->message == MESSAGE_UPDATEMETA
		|| send->message == MESSAGE_EXTENTREADEND) {
		// socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1);
		// socket->PollCompletion(DesNodeID, 1, &wc);
		return true;
	}
	socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1);
	if (isServer) {
		while (recv->message == MESSAGE_INVALID || recv->message != MESSAGE_RESPONSE)
			;
	} else {
		// gettimeofday(&startt,NULL);
		while (recv->message != MESSAGE_RESPONSE) {
			;
			/* gettimeofday(&endd,NULL);
			diff = 1000000 * (endd.tv_sec - startt.tv_sec) + endd.tv_usec - startt.tv_usec;
			if (diff > 1000000) {
				Debug::debugItem("Send the Fucking Message Again.");
				ExtentWriteSendBuffer *tempsend = (ExtentWriteSendBuffer *)sendBuffer;
				tempsend->offset = (uint64_t)tempCount;
				tempCount += 1;
				socket->_RdmaBatchWrite(DesNodeID, sendBuffer, remoteRecvBuffer, lengthSend, imm, 1);
				gettimeofday(&startt,NULL);
				diff = 0;
			}*/
		}
	}
	memcpy((void*)bufferReceive, (void *)receiveBuffer, lengthReceive);
	return true;
}

uint64_t RPCClient::ContractSendBuffer(GeneralSendBuffer *send) {
	uint64_t length = 0;
	switch (send->message) {
		case MESSAGE_MKNODWITHMETA: {
			MakeNodeWithMetaSendBuffer *bufferSend = 
	                (MakeNodeWithMetaSendBuffer *)send;
        	    	length = (MAX_FILE_EXTENT_COUNT - bufferSend->metaFile.size) * sizeof(FileMetaTuple);
			length = 0;
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
