/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

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
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif     
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
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

	// Check MessageType
	static char s[1024];
	MessageHdr *msg;
	
	if (size <1 ) {
		return false;
	}

	// Get MsgType
	msg = (MessageHdr *) malloc(sizeof(MessageHdr));
	memcpy(&msg, &data, sizeof(MessageHdr));	
	
	switch (msg->msgType) {
		case JOINREQ : 
			ProcessJoinReq(env,data,size);
			break;
		case JOINREP:			
			ProcessJoinRep(env,data,size);					
			break;
		case PUSH :	
			cout << "[" << memberNode->addr.getAddress() << "] RCV PUSH MESSAGE\n" ;
			ProcessPush(env,data,size);		
			break;			
		default :			
			break;
	}

	free(msg);
	return true;
}

/* Receive Gossip-Membership Message and process it */
void MP1Node::ProcessPush(void *env, char *data, int size)
{
	// Get Membership List
	size_t msgsize = size - sizeof(MessageHdr);
	int numberOfMemberListEntry = msgsize/sizeof(memberNode->memberList);
	MemberListEntry *mEOut = new MemberListEntry[numberOfMemberListEntry];
	memcpy(mEOut, &data[0]+sizeof(MessageHdr), msgsize);

	// Process each membership from message
	for (int index=0; index < numberOfMemberListEntry; index++) {	
		int id = mEOut[index].getid();
		long heartbeat = mEOut[index].getheartbeat();

		//Iterate over our Membership List
		bool found = false;
		for (int index=0; index < memberNode->memberList.size(); index++) {
			if (memberNode->memberList[index].getid() == id) {
				found = true;

				// Check heartbeat and update it if value is more recent
				if (heartbeat > memberNode->memberList[index].getheartbeat()) {
					// update heartbeat and timestamp
					memberNode->memberList[index].setheartbeat(heartbeat);
					memberNode->memberList[index].settimestamp((long)time(NULL));
				}
				break;
			}
		}

		// Check if not found -> new node
		if (!found) {
			mEOut[index].settimestamp((long)time(NULL));
			memberNode->memberList.push_back(mEOut[index]);
			log->logNodeAdd(&memberNode->addr, new Address(to_string(id) + ":" + to_string(mEOut[index].getport())));

			// Send to GOssip Node
			sendPushMsg(&mEOut[index]);
		}
	}

	

	free(mEOut);
}
/*
Receive a list of nodes already joined
*/
void MP1Node::ProcessJoinRep(void *env, char *data, int size)
{
	// Get ListMember from message
	size_t msgsize = size - sizeof(MessageHdr);
	int numberOfMemberListEntry = msgsize/sizeof(memberNode->memberList);

	// Add to own listmember
	MemberListEntry *mEOut = new MemberListEntry[numberOfMemberListEntry];
	memcpy(mEOut, &data[0]+sizeof(MessageHdr), msgsize);

	// Copy to internal Vector
	for (int index=0; index < numberOfMemberListEntry; index++) {
		mEOut[index].settimestamp((long)time(NULL));
		mEOut[index].setheartbeat(1);	// first heartbeat to send		
		memberNode->memberList.push_back(mEOut[index]);

		// log Other Node
		log->logNodeAdd(&memberNode->addr, new Address(to_string(mEOut[index].getid()) + ":" + to_string(mEOut[index].getport())));
	}	

	// Set node to group
	this->memberNode->inGroup = true;
		
	// Log add own node
	log->logNodeAdd(&memberNode->addr, &memberNode->addr);

	// Send PUSH message with updated Membership List
	for (int index=0; index < memberNode->memberList.size(); index++) {
		sendPushMsg(&memberNode->memberList[index]);
	}
	
	
	free(mEOut);
}

//  Send PUSH message with updated Membership List
void MP1Node::sendPushMsg(MemberListEntry *me) {
	int numberToPush = GOSSIPK;
	int indexGossip = 0;
	MessageHdr *msg;
	int sizeNode = memberNode->memberList.size();

	//Nothing to send, only one node in Membership List (itslef)
	if (sizeNode == 1) {
		return;
	} else if (sizeNode == 2) {
		numberToPush = 1;
	}

	while (indexGossip < numberToPush) {
		
		// Get Randomly a Member
		int dice_roll = getRandomVectorPosition();
		MemberListEntry nodeToSend = memberNode->memberList[dice_roll];

		// Send Push Message to node
		size_t size_vector = sizeof(MemberListEntry);
		size_t msgsize = sizeof(MessageHdr) + size_vector;

		msg = (MessageHdr *) malloc(msgsize * sizeof(char));
		msg->msgType = PUSH;	
      
	    // Send via EmulNet
	    memcpy((char *)(msg+1), (char *)me, size_vector);
	    emulNet->ENsend(&memberNode->addr, new Address(to_string(nodeToSend.getid()) + ":" + to_string(nodeToSend.getport())), (char *)msg, msgsize);

		// Increase Gossip Number
		indexGossip++;

		free(msg);

	}
}

// return a random position of the Membership List
int MP1Node::getRandomVectorPosition() {
	int size = memberNode->memberList.size();
	std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<> distr(0, size); // define the range
    int dice_roll = distr(eng);
	return dice_roll;
}
/*
Send a List of joined nodes from introducer
*/
void MP1Node::ProcessJoinReq(void *env, char *data, int size)
{
	static char s[1024];
	MemberListEntry mE;
	char addr[6];
	int id = 0;
	short port;
	MessageHdr *msg;
	Address joinaddr;

	// Get Node Addr
	memset(&addr, 40, sizeof(addr));
	memcpy(&addr,  &data[0]+sizeof(MessageHdr), sizeof(addr));
	memcpy(&id, &addr[0], sizeof(int));
	memcpy(&port, &addr[4], sizeof(short));
	sprintf(s, "Introducer receive Message JOINREQ from (%d:%d)", id, port);			

	// Adding this node to node list members
	mE = MemberListEntry(id,port);
	memberNode->memberList.push_back(mE);

	// Send back the node list members to the sender 
	size_t size_vector = memberNode->memberList.size()*sizeof(memberNode->memberList);
	size_t msgsize = sizeof(MessageHdr) + size_vector;

	msg = (MessageHdr *) malloc(msgsize * sizeof(char));
	msg->msgType = JOINREP;	
      
    // Send via EmulNet
    //memcpy((char *)(msg+1), &memberNode->memberList, size_vector);
    MemberListEntry *mEOut = new MemberListEntry[memberNode->memberList.size()];
    std::copy(memberNode->memberList.begin(), memberNode->memberList.end(), mEOut);
    memcpy((char *)(msg+1), (char *)mEOut, size_vector);

    emulNet->ENsend(&memberNode->addr, new Address(to_string(id) + ":" + to_string(port)), (char *)msg, msgsize);	

    free(mEOut);
    free(msg);
	log->LOG(&memberNode->addr, s);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	// Check TFail

	// Check TCleanup

	// Propagate the membership list
	
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
