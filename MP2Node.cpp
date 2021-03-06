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
	quorum_store = new map<int,QuorumReplies>();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
	delete quorum_store;
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
	ring = curMemList;


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    stabilizationProtocol();

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

    Mp2Message message(CREATE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    message.data.value = value;
    sendMessage(message);
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

    Mp2Message message(READ);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    sendMessage(message);
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

    Mp2Message message(UPDATE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    message.data.value = value;
    sendMessage(message);
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

    Mp2Message message(DELETE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    sendMessage(message);
}

void MP2Node::sendMessage(Mp2Message &message)
{
    vector<Node> replicas = findNodes(message.data.key);
    QuorumReplies qrs;
    int  i = 0;
    for (auto &&node: replicas)
    {

        message.data.replica = (enum ReplicaType)i++;
        message.data.recvAddr = node.nodeAddress;
        emulNet->ENsend(&memberNode->addr,
                        &node.nodeAddress, (char *)&message,
                        sizeof(Mp2Message));
        qrs.messages[i-1] = message;
    }
    quorum_store->insert(std::pair<int,QuorumReplies>(message.data.transID, qrs));
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
		#if 1
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
		        break;
		    case READ:
		        {
		            Mp2Message reply_message = message;
		            reply_message.msgType = READREPLY;
		            reply_message.resp4msgType = READ;
		            reply_message.data.value = readKey(message.data.key);
                    if(reply_message.data.value != "")
                    {
                        reply_message.data.success = true;
                        log->logReadSuccess(&memberNode->addr, false,
                                    message.data.transID,
                                    message.data.key,
                                    reply_message.data.value);
                    }
                    else
                    {
                        reply_message.data.success = false;
                        log->logReadFail(&memberNode->addr, false,
                                    message.data.transID,
                                    message.data.key);
                    }
                    int sent = emulNet->ENsend(&memberNode->addr,
                        &message.data.senderAddr, (char *)&reply_message,
                        sizeof(Mp2Message));
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
		        break;
		    case DELETE:
                {
                    Mp2Message reply_message = message;
                    reply_message.msgType = REPLY;
                    reply_message.resp4msgType = DELETE;
                    reply_message.data.success = deletekey(message.data.key);
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
		        break;
		    case REPLY:
		    case READREPLY:
		        {
		            map<int,QuorumReplies>::iterator itr = quorum_store->find(message.data.transID);
                    if(itr != quorum_store->end())
                    {
                        QuorumReplies &qrs = itr->second;
                        message.got_reply = true;
                        qrs.reply_messages[(int)message.data.replica] = message;
                        check_quorum(message.data.transID);
                    }
		        }
		        break;
		}
		#endif
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
    markFailedNodeMessageFailed();
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
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
    //iterator on all keys in my hash table
    //move the keys to another nodes where key belongs
    for (auto e = ht->hashTable.begin(); e != ht->hashTable.end();++e) {
      auto replicas = findNodes (e->first);
      auto &key = e->first;
      auto &value = e->second;
      auto inReplicas = find_if(replicas.begin(),
            replicas.end(),
            [&](Node &n){return n.nodeAddress == memberNode->addr;});
      if (inReplicas != replicas.end()) {

        // The key belongs to this node
        switch (inReplicas - replicas.begin()) {
        case 0:
          {
            bool firstSucessorFail = !isNodeAlive(replicas[1].nodeAddress);
            bool secondSuccessorFail = !isNodeAlive(replicas[2].nodeAddress);
            // Keep key as primary
            ReplicaType replType = PRIMARY;
            if (firstSucessorFail) {
              // Send to new first successor
              sendReplicationMessage(&replicas[1].nodeAddress, key, value, SECONDARY);
            }
            if (secondSuccessorFail) {
              // Send to new second successor
              sendReplicationMessage(&replicas[2].nodeAddress, key, value, TERTIARY);
            }
            break;
          }
        case 1:
          {
            bool firstPredecessorFail = !isNodeAlive(replicas[0].nodeAddress);
            bool firstSucessorFail = !isNodeAlive(replicas[2].nodeAddress);
            // Keep key as secondary
            ReplicaType replType = SECONDARY;
            if (firstPredecessorFail) {
              // Send to new first predecessor
              sendReplicationMessage(&replicas[0].nodeAddress, key, value, PRIMARY);
            }
            if (firstSucessorFail) {
              // Send to new first successor
              sendReplicationMessage(&replicas[2].nodeAddress, key, value, TERTIARY);
            }
            break;
          }
        case 2:
          {
            bool secondPredecessorFail = !isNodeAlive(replicas[0].nodeAddress);
            bool firstPredecessorFail = !isNodeAlive(replicas[1].nodeAddress);
            // Keep key as tertiary
            ReplicaType replType = TERTIARY;
            if (secondPredecessorFail) {
              // Send to new second predecessor
              sendReplicationMessage(&replicas[0].nodeAddress, key, value, PRIMARY);
            }
            if (firstPredecessorFail) {
              // Send to new first predecessor
              sendReplicationMessage(&replicas[1].nodeAddress, key, value, SECONDARY);
            }
            break;
          }
        }



      } else {
        // The key doesn't belong any longer to this node
        sendReplicationMessage(&replicas[0].nodeAddress, key, value, PRIMARY);
        sendReplicationMessage(&replicas[1].nodeAddress, key, value, SECONDARY);
        sendReplicationMessage(&replicas[2].nodeAddress, key, value, TERTIARY);
        ht->hashTable.erase(e);
      }
    }

}

void MP2Node::sendReplicationMessage(Address *addr,
        string key, string value, ReplicaType replica) {
    //Send replication message

    Mp2Message message(CREATE);
    message.data.transID = ++g_transID;
    message.data.senderAddr = memberNode->addr;
    message.data.key = key;
    message.data.value = value;

    message.data.replica = replica;
    message.data.recvAddr = *addr;
    emulNet->ENsend(&memberNode->addr,
                    addr, (char *)&message,
                    sizeof(Mp2Message));
}

void MP2Node::check_quorum(int transID) {
    map<int,QuorumReplies>::iterator itr = quorum_store->find(transID);
    if(itr != quorum_store->end())
    {
        QuorumReplies &qrs = itr->second;
        int num_replies = 0;
        if(qrs.quorum_reached)
            return;

        for(int i=0;i<3;++i)
        {
            if(qrs.reply_messages[i].got_reply)
            {
                ++num_replies;
            }
        }
        bool quorum_reached = (num_replies >=2)?true:false;


        if(quorum_reached)
        {
            int quorum_result = 0;
            Mp2Message *reply = NULL;

            for(int i=0;i<3;++i)
            {
                if(!qrs.reply_messages[i].got_reply)
                    continue;
                reply = &qrs.reply_messages[i];
                if(qrs.reply_messages[i].data.success) {
                    quorum_result+=1;
                }else{
                    quorum_result-=1;
                }
            }
            if(quorum_result != 0)
            {
                qrs.quorum_reached = true;
            }else{
                return;
            }
            switch(reply->resp4msgType)
            {
            case CREATE:
                {
                    if(quorum_result>0)
                    {
                        log->logCreateSuccess(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key,
                                    reply->data.value);
                    }
                    else
                    {
                        log->logCreateFail(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key,
                                    reply->data.value);
                    }
                }
                break;
            case READ:
                {
                    if(quorum_result>0)
                    {
                        log->logReadSuccess(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key,
                                    reply->data.value);
                    }
                    else
                    {
                        log->logReadFail(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key);
                    }
                }
                break;
            case UPDATE:
                {
                    if(quorum_result>0)
                    {
                        log->logUpdateSuccess(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key,
                                    reply->data.value);
                    }
                    else
                    {
                        log->logUpdateFail(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key,
                                    reply->data.value);
                    }
                }
                break;
            case DELETE:
                {
                    if(quorum_result>0)
                    {
                        log->logDeleteSuccess(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key);
                    }
                    else
                    {
                        log->logDeleteFail(&memberNode->addr, true,
                                    reply->data.transID,
                                    reply->data.key);
                    }
                }
                break;
            }
        }

    }
}

bool MP2Node::isNodeAlive(Address &adr)
{
    auto memberList = getMembershipList();
    for(auto &&node: memberList)
    {
        if(node.nodeAddress == adr)
            return true;
    }
    return false;
}

void MP2Node::markFailedNodeMessageFailed()
{
    for(map<int,QuorumReplies>::iterator itr=quorum_store->begin();
        itr != quorum_store->end(); ++itr){
        int transID = itr->first;
        QuorumReplies &qrs = itr->second;

        for(int i=0;i<3;++i)
        {
            if(!qrs.reply_messages[i].got_reply &&
                !isNodeAlive(qrs.messages[i].data.recvAddr))
            {
                Mp2Message &message = qrs.messages[i];
                Mp2Message reply_message = message;
                reply_message.msgType = REPLY;
                reply_message.got_reply = true;
                reply_message.resp4msgType = message.msgType;
                reply_message.data.success = false;
                qrs.reply_messages[i] = reply_message;

                log->logReadFail(&message.data.recvAddr, false,
                                    transID,
                                    message.data.key);
            }
        }
        if(!qrs.quorum_reached)
            check_quorum(transID);
    }

}

