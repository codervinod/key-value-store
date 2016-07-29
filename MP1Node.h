/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    GOSSIP_MESSAGE
};

typedef struct JoinReqMesg {
    char addr[6];
    long heartbeat;
}JoinReqMesg;

typedef struct GossipMesg {
    long number_of_entry;
    MemberListEntry entry_list[1];
}GossipMesg;

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

typedef union MessageData {
   JoinReqMesg join_req_mesg;
   GossipMesg gossip_mesg;
}MessageData;

typedef struct Mp1Message {
    MessageHdr hdr;
    MessageData data;
}Mp1Message;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];
    int timestamp;
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
private:
    void handleJoinReq(Member *memberNode, JoinReqMesg *mesg_data);
    void handleJoinRep(Member *memberNode);
    void handleGossipMesg(Member *memberNode, GossipMesg *gossip_mesg);

    void sendJoinRep(Address *addr);
    void sendGossipMesg(Address *addr);

    MemberListEntry *getMemberListEntryForId(int id);
    void scanMembershipListForFailures();

    int getTimeStamp() { return timestamp;}
    vector<int> *_failedSet;

};

#endif /* _MP1NODE_H_ */
