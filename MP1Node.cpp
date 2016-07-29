/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    /* initialize random seed: */
    srand (time(NULL));

	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->timestamp = 0;

}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {

}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	_failedSet = new vector<int>();
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        Mp1Message *message = (Mp1Message *)malloc(sizeof(Mp1Message));
        message->hdr.msgType = JOINREQ;
        memcpy(message->data.join_req_mesg.addr, &memberNode->addr.addr,
            sizeof(memberNode->addr.addr));
        message->data.join_req_mesg.heartbeat = memberNode->heartbeat;
        size_t msgsize = sizeof(Mp1Message);
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)message, msgsize);
        free(message);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   if(_failedSet) {
        delete _failedSet;
    }
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    Member *memberNode = (Member*)env;
    Mp1Message *msg = (Mp1Message*)data;
    switch(msg->hdr.msgType) {
        case JOINREQ:
            handleJoinReq(memberNode, &(msg->data.join_req_mesg));
            break;
        case JOINREP:
            handleJoinRep(memberNode);
            break;
        case GOSSIP_MESSAGE:
            handleGossipMesg(memberNode, &(msg->data.gossip_mesg));
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    Member *node = getMemberNode();
    node->heartbeat = node->heartbeat + 1;
    ++this->timestamp;
    scanMembershipListForFailures();

    int id = 0;
    short port;
    memcpy(&id, &node->addr.addr[0], sizeof(int));
    memcpy(&port, &node->addr.addr[4], sizeof(short));

    MemberListEntry *member_entry = getMemberListEntryForId(id);
    if(member_entry)
    {
        member_entry->heartbeat = node->heartbeat;
        member_entry->timestamp = getTimeStamp();
    }

	// Select 4 random address from membership list
	// Send gossip message to them
    if(!node->memberList.empty())
    {
        int fanout = 4;
        fanout = (node->memberList.size() < fanout)?node->memberList.size()
                :fanout;

        for(int i=0;i<fanout;++i)
        {
            int randomeEntryId = rand() % node->memberList.size();
            MemberListEntry &entry = node->memberList[randomeEntryId];

            vector<int>::iterator itr = find (_failedSet->begin(),
                _failedSet->end(), entry.id);
            if (itr != _failedSet->end())
            {
                fanout+=1;
                continue;
            }

            Address addr;
            memcpy(&addr.addr[0], &entry.id, sizeof(int));
            memcpy(&addr.addr[4], &entry.port, sizeof(short));
            sendGossipMesg(&addr);
        }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::handleJoinReq(Member *memberNode, JoinReqMesg *mesg_data)
{
    Address addr;
    memcpy(&addr.addr, &mesg_data->addr, sizeof(addr.addr));

    int id;
    short port;
    long heartbeat = mesg_data->heartbeat;
    long timestamp = getTimeStamp();

    memcpy(&id, &addr.addr[0], sizeof(int));
    memcpy(&port, &addr.addr[4], sizeof(short));

    MemberListEntry entry = MemberListEntry(id, port,
            heartbeat, timestamp);

    if(memberNode->memberList.empty())
    {
        int id;
        short port;
        memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
        memcpy(&port, &memberNode->addr.addr[4], sizeof(short));

        //Add my own entry here
        MemberListEntry own_entry = MemberListEntry(id, port,
            memberNode->heartbeat, timestamp);
        memberNode->memberList.push_back(own_entry);
    }

    memberNode->memberList.push_back(entry);

    log->logNodeAdd(&memberNode->addr, &addr);
    sendJoinRep(&addr);
}

void MP1Node::sendJoinRep(Address *addr)
{
    Mp1Message *smessage = (Mp1Message *)malloc(sizeof(Mp1Message));
    smessage->hdr.msgType = JOINREP;
    size_t msgsize = sizeof(Mp1Message);
    // send JOINREP message to introducer member
    emulNet->ENsend(&memberNode->addr, addr, (char *)smessage, msgsize);
    free(smessage);
}

void MP1Node::handleJoinRep(Member *memberNode)
{
    memberNode->inGroup = true;
}

void MP1Node::sendGossipMesg(Address *addr)
{

    int num_members = getMemberNode()->memberList.size();
    if(num_members <= 0)
    {
        return;
    }
    size_t size_of_mesg = sizeof(Mp1Message) +
        (num_members-1)*sizeof(MemberListEntry);
    Mp1Message *smessage = (Mp1Message *)malloc(size_of_mesg);
    smessage->hdr.msgType = GOSSIP_MESSAGE;

    GossipMesg *gossip = &smessage->data.gossip_mesg;
    gossip->number_of_entry = getMemberNode()->memberList.size();

    int i = 0;
    for( auto &entry : getMemberNode()->memberList)
    {
        vector<int>::iterator itr = find (_failedSet->begin(),
        _failedSet->end(), entry.id);
        if (itr != _failedSet->end())
        {
            --gossip->number_of_entry;
            continue;
        }
        memcpy(&(gossip->entry_list[i]), &entry, sizeof(MemberListEntry));
        ++i;
    }
    // send Gossip message
    emulNet->ENsend(&memberNode->addr, addr, (char *)smessage, size_of_mesg);
    free(smessage);
}

void MP1Node::scanMembershipListForFailures()
{
    for( vector<MemberListEntry>::iterator itr= getMemberNode()->memberList.begin();
        itr != getMemberNode()->memberList.end(); )
    {
        MemberListEntry &entry = *itr;
        long diff = getTimeStamp() - entry.timestamp;
        vector<int>::iterator itr2 = find (_failedSet->begin(), _failedSet->end(),
                                            entry.id);


        if (diff > (TREMOVE*10))
        {
            itr = getMemberNode()->memberList.erase(itr);
            if(itr2 != _failedSet->end())
            {
                _failedSet->erase(itr2);
            }
            continue;
        }else if(diff > TFAIL)
        {
            if (itr2 == _failedSet->end())
            {
                Address addr;
                memcpy(&addr.addr[0], &entry.id, sizeof(int));
                memcpy(&addr.addr[4], &entry.port, sizeof(short));
                log->logNodeRemove(&getMemberNode()->addr, &addr);
               _failedSet->push_back(entry.id);
            }
        }
        ++itr;
    }
}

void MP1Node::handleGossipMesg(Member *memberNode, GossipMesg *gossip_mesg)
{
    for(int i = 0; i < gossip_mesg->number_of_entry; ++i)
    {
        MemberListEntry *new_entry = &gossip_mesg->entry_list[i];
        MemberListEntry *old_entry = getMemberListEntryForId(new_entry->id);
        vector<int>::iterator itr = find (_failedSet->begin(),
                    _failedSet->end(), new_entry->id);
        if(itr != _failedSet->end())
        {
            continue;
        }
        if(old_entry)
        {
            //Compare heatbeat counters
            if (old_entry->heartbeat < new_entry->heartbeat)
            {
                old_entry->heartbeat = new_entry->heartbeat;
                old_entry->timestamp = getTimeStamp();
            }
        }
        else
        {
            new_entry->heartbeat = getMemberNode()->heartbeat;
            new_entry->timestamp = getTimeStamp();
            Address addr;
            memcpy(&addr.addr[0], &new_entry->id, sizeof(int));
            memcpy(&addr.addr[4], &new_entry->port, sizeof(short));
            log->logNodeAdd(&memberNode->addr, &addr);
            getMemberNode()->memberList.push_back(*new_entry);
        }
    }
}

MemberListEntry *MP1Node::getMemberListEntryForId(int id)
{
    for( auto &&entry : getMemberNode()->memberList)
    {
        if( id == entry.id)
        {
            return &entry;
        }
    }
    return NULL;
}
