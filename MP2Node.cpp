/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	quorum_store = new map<int,vector<Mp2Message>>();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	if(quorum_store) {
	    delete quorum_store;
	}
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	vector<Node> oldRing;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	oldRing = ring;
	ring = curMemList;


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    stabilizationProtocol(oldRing, ring);

}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    Mp2Message message(CREATE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    message.data.value = value;
    int i = 0;
    for (auto &&node: replicas)
    {

        message.data.replica = (enum ReplicaType)i++;
        emulNet->ENsend(&memberNode->addr,
                        &node.nodeAddress, (char *)&message,
                        sizeof(Mp2Message));
    }

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    vector<Node> replicas = findNodes(key);

    Mp2Message message(READ);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    int i = 0;
    for (auto &&node: replicas)
    {

        message.data.replica = (enum ReplicaType)i++;
        emulNet->ENsend(&memberNode->addr,
                        &node.nodeAddress, (char *)&message,
                        sizeof(Mp2Message));
    }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);

    Mp2Message message(UPDATE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    message.data.value = value;
    int i = 0;
    for (auto &&node: replicas)
    {

        message.data.replica = (enum ReplicaType)i++;
        emulNet->ENsend(&memberNode->addr,
                        &node.nodeAddress, (char *)&message,
                        sizeof(Mp2Message));
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);

    Mp2Message message(DELETE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    int i = 0;
    for (auto &&node: replicas)
    {

        message.data.replica = (enum ReplicaType)i++;
        emulNet->ENsend(&memberNode->addr,
                        &node.nodeAddress, (char *)&message,
                        sizeof(Mp2Message));
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

void MP2Node::serverReplicateData(size_t hash_code,
                                    Address &to_replicate_addr,
                                    ReplicaType replica)
{
    map<string, string>::iterator itr =  ht->hashTable.begin();
    while(itr != ht->hashTable.end())
    {
        string key = itr->first;
        string value = itr->second;
        size_t curr_hash_code = hashFunction(key);
        if(hash_code == curr_hash_code)
        {
            Mp2Message message(CREATE);
            message.data.transID = -1;
            message.data.senderAddr = memberNode->addr;
            message.data.key = key;
            message.data.value = value;
            message.data.replica = replica;
            emulNet->ENsend(&memberNode->addr,
                            &to_replicate_addr, (char *)&message,
                            sizeof(Mp2Message));
        }
        ++itr;
    }
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();


		/*
		 * Handle the message types here
		 */
		Mp2Message message;
		memcpy(&message, data, size);
		switch(message.msgType)
		{
		    case CREATE:
		        {
		            Mp2Message reply_message = message;
		            reply_message.msgType = REPLY;
		            reply_message.resp4msgType = CREATE;

		            reply_message.data.success = createKeyValue(message.data.key,
                                                    message.data.value,
                                                    message.data.replica);
                    if(message.data.transID != -1)
                    {
                        if(reply_message.data.success)
                        {
                            log->logCreateSuccess(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key,
                                        message.data.value);
                        }
                        else
                        {
                            log->logCreateFail(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key,
                                        message.data.value);
                        }
                        emulNet->ENsend(&memberNode->addr,
                        &message.data.senderAddr, (char *)&reply_message,
                        sizeof(Mp2Message));
                    }
		        }
		        break;
		    case READ:
		        {
		            Mp2Message reply_message = message;
		            reply_message.msgType = READREPLY;
		            reply_message.resp4msgType = READ;
		            message.data.value = readKey(message.data.key);
		            if(message.data.transID != -1)
                    {
                        if(message.data.value != "")
                        {
                            log->logReadSuccess(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key,
                                        message.data.value);
                        }
                        else
                        {
                            log->logReadFail(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key);
                        }
                        emulNet->ENsend(&memberNode->addr,
                            &message.data.senderAddr, (char *)&reply_message,
                            sizeof(Mp2Message));
                    }

		        }
		        break;
		    case UPDATE:
                {
                    Mp2Message reply_message = message;
		            reply_message.msgType = REPLY;
		            reply_message.resp4msgType = UPDATE;
                    reply_message.data.success = updateKeyValue(message.data.key,
                                        message.data.value,
                                        message.data.replica);
                    if(message.data.transID != -1)
                    {
                        if(reply_message.data.success)
                        {
                            log->logUpdateSuccess(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key,
                                        message.data.value);
                        }
                        else
                        {
                            log->logUpdateFail(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key,
                                        message.data.value);
                        }
                        emulNet->ENsend(&memberNode->addr,
                            &message.data.senderAddr, (char *)&reply_message,
                            sizeof(Mp2Message));
                    }

                }
		        break;
		    case DELETE:
                {
		            Mp2Message reply_message = message;
		            reply_message.msgType = REPLY;
		            reply_message.resp4msgType = DELETE;
                    reply_message.data.success = deletekey(message.data.key);
                    if(message.data.transID != -1)
                    {
                        if(reply_message.data.success)
                        {
                            log->logDeleteSuccess(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key);
                        }
                        else
                        {
                            log->logDeleteFail(&memberNode->addr, false,
                                        message.data.transID,
                                        message.data.key);
                        }
                        emulNet->ENsend(&memberNode->addr,
                            &message.data.senderAddr, (char *)&reply_message,
                            sizeof(Mp2Message));
                     }

                }
		        break;
		    case REPLY:
		    case READREPLY:
		        {
		            map<int,vector<Mp2Message>>::iterator itr =
		                quorum_store->find(message.data.transID);
		            if(itr != quorum_store->end())
		            {
		                itr->second.push_back(message);
		                check_quorum(message.data.transID);
		            }
		            else{
		                vector<Mp2Message> mp2mesgvct;
		                mp2mesgvct.push_back(message);
                        quorum_store->insert(make_pair(message.data.transID,
                                                mp2mesgvct));
		            }

		        }
		        break;
		    case REPLICATE:
                {
                    serverReplicateData(message.data.hash_code,
                                    message.data.to_replicate_addr,
                                    message.data.replica);
                }
                break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node> &oldRing,
                                    vector<Node> &newRing) {
	/*
	 * Implement this
	 */

	// Only run at master
	if(memberNode->addr == newRing[0].nodeAddress)
	{
	    return;
	}

	int i=0,j=0;
    while( i<oldRing.size() && j<newRing.size())
    {
        Node &old_node = oldRing[i];
        Node &new_node = newRing[j];
        if(old_node.getHashCode() != new_node.getHashCode() )
        {
            // node i has failed
            // handle the case here
            // copy previous 2 nodes unique data to next one
            // copy next node this node data to 3rd the node
            // N-2, N-1, Failed(N), N+1, N+2, N+3
            // copy N-2 to N+1
            // copy N-1 to N+2
            // copy N data in N+1 to N+3

            Node prev_node_first/* N-1 */, prev_node_second/* N-2 */;
            Node next_node /* N+1 */,
                    next_second_node /* N+2 */,
                    next_third_node/* N+3
             */;
            if(i>0)
                prev_node_first = oldRing[i-1];
            else if(i==0)
                prev_node_first = oldRing[oldRing.size()-1];

            if(i>1)
                prev_node_second = oldRing[i-2];
            else if(i==1)
                prev_node_second = oldRing[oldRing.size()-1];
            else if(i==0)
                prev_node_second = oldRing[oldRing.size()-2];

            next_node = oldRing[(i+1)%(oldRing.size()-1)];
            next_second_node = oldRing[(i+2)%(oldRing.size()-1)];
            next_third_node = oldRing[(i+3)%(oldRing.size()-1)];
            //Send Hash code of data to replicate and to address

            // copy N-2 to N+1
            replicateDataWithHash(prev_node_second.getHashCode(),
                                  prev_node_second.nodeAddress,
                                  next_node.nodeAddress, TERTIARY);
            // copy N-1 to N+2
            replicateDataWithHash(prev_node_first.getHashCode(),
                                  prev_node_first.nodeAddress,
                                  next_second_node.nodeAddress, SECONDARY);

            // copy N data in N+1 to N+3
            replicateDataWithHash(old_node.getHashCode(),
                                  next_node.nodeAddress,
                                  next_third_node.nodeAddress,
                                  PRIMARY);

            ++i;
            continue;
        }
        ++i;
        ++j;
    }
}

void MP2Node::replicateDataWithHash(size_t hash_code, Address &from_node,
                                        Address &to_node,ReplicaType replica)
{
    Mp2Message message(REPLICATE);
    message.data.hash_code = hash_code;
    message.data.to_replicate_addr = to_node;
    message.data.replica = replica;
    emulNet->ENsend(&memberNode->addr,
                    &from_node, (char *)&message,
                    sizeof(Mp2Message));
}

void MP2Node::check_quorum(int transID) {
    map<int,vector<Mp2Message>>::iterator itr =
		                quorum_store->find(transID);
    if(itr != quorum_store->end())
    {
        vector<Mp2Message> replies = itr->second;
        bool quorum_reached = false;
        int quorum_result = 0;
        if(replies.size() == 3)
        {
            quorum_reached = true;
        }else{
            quorum_reached = (replies[0].data.success ==
                                replies[1].data.success);
        }

        if(quorum_reached)
        {
            for(auto &&mesg: replies)
            {
                if(mesg.data.success) {
                    quorum_result+=1;
                }else{
                    quorum_result-=1;
                }
            }
            switch(replies[0].resp4msgType)
            {
            case CREATE:
                {
                    if(quorum_result>0)
                    {
                        log->logCreateSuccess(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key,
                                    replies[0].data.value);
                    }
                    else
                    {
                        log->logCreateFail(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key,
                                    replies[0].data.value);
                    }
                }
                break;
            case READ:
                {
                    if(quorum_result>0)
                    {
                        log->logReadSuccess(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key,
                                    replies[0].data.value);
                    }
                    else
                    {
                        log->logReadFail(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key);
                    }
                }
                break;
            case UPDATE:
                {
                    if(quorum_result>0)
                    {
                        log->logUpdateSuccess(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key,
                                    replies[0].data.value);
                    }
                    else
                    {
                        log->logUpdateFail(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key,
                                    replies[0].data.value);
                    }
                }
                break;
            case DELETE:
                {
                    if(quorum_result>0)
                    {
                        log->logDeleteSuccess(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key);
                    }
                    else
                    {
                        log->logDeleteFail(&memberNode->addr, true,
                                    replies[0].data.transID,
                                    replies[0].data.key);
                    }
                }
                break;
            }
            quorum_store->erase(itr);
        }

    }
}

