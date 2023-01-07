/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

#include <set>
#include <random>

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

    this->id = *(int*)(&memberNode->addr.addr);
	this->port = *(short*)(&memberNode->addr.addr[4]);
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

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        msg = new MessageHdr;

        msg->msgType = JOINREQ;
        msg->sourceAddress = Address(memberNode->addr);

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        delete msg;
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

   return 0;
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

    // Parse out message header
    MessageHdr *msg = ((MessageHdr*) data);

    // Take action depending on message type.
    // JOINREQ: Add node to (fixed-length) membership list, respond with JOINREP containing membership list
    // JOINREP: Set own membership list to the passed one
    // TABLEUPDATE: Extract membership list from message.  Merge into existing membership list, choosing
    //     members whose heartbeat is newer than the existing heartbeat.

    if (JOINREQ == msg->msgType) {

        handleJoinReq(msg);

    } else if (JOINREP == msg->msgType) {

        handleJoinRep(msg);
        
    } else if (PING == msg->msgType) {
        
        handlePing(msg);

    } else {
        log->LOG(&memberNode->addr, "Invalid message type");
    }

    memberNode->nnb = memberNode->memberList.size();

    return true;
}

void MP1Node::handleJoinReq(MessageHdr *data) {
    addNewMember(data);

    MessageHdr *response = prepareJoinRepMessage();

    size_t messageSize = sizeof(MessageHdr) + (sizeof(MemberListEntry) * response->memberCount);

    emulNet->ENsend(&memberNode->addr, &data->sourceAddress, std::string((char *) response, messageSize));

    free(response);
}

void MP1Node::handlePing(MessageHdr *data) {
    MemberListEntry* members = reinterpret_cast<MemberListEntry*>(data + 1);

    for (unsigned int i = 0; i < data->memberCount; i++) {

        // don't put this node in its own membership list
        if (members[i].getid() == id && members[i].getport() == port) {
            continue;
        }

        updateMember(members[i]);
    }

    std::cout << "[" + memberNode->addr.getAddress() + "] received PING from [" << data->sourceAddress.getAddress() << "] with " << data->memberCount << " members in list" << std::endl;
}

bool MP1Node::hasNodeFailed(MemberListEntry& entry) {
    return (entry.gettimestamp() + TREMOVE <= par->getcurrtime());
}

bool MP1Node::isNodeSuspicious(MemberListEntry& entry) {
    return (entry.gettimestamp() + TFAIL <= par->getcurrtime());
}

void MP1Node::updateMember(MemberListEntry &entry) {
    int index = indexOfMember(entry);

    if (index == -1) {
        if (hasNodeFailed(entry) || isNodeSuspicious(entry)) {
            return;
        } 

        addNewMember(entry);
    } else {
        if (hasNodeFailed(entry)) {
            memberNode->memberList.erase(memberNode->memberList.begin() + index);
            Address removedAddress = getAddress(entry);
            log->logNodeRemove(&memberNode->addr, &removedAddress);
        } else {
            MemberListEntry& oldEntry = memberNode->memberList[index];

            if (oldEntry.getheartbeat() < entry.getheartbeat()) {
                oldEntry.setheartbeat(entry.getheartbeat());
                oldEntry.timestamp = par->getcurrtime();
            }
        }
    }
}

int MP1Node::indexOfMember(int id, short port) {
    for (unsigned int i = 0; i < memberNode->memberList.size(); i++) {
        if (id == memberNode->memberList[i].getid() && port == memberNode->memberList[i].getport()) {
            return i;
        }
    }

    return -1;
}

int MP1Node::indexOfMember(const Address &address) {
    int id = *(int *) (&address.addr);
    short port = *(short *) (&address.addr[4]);

    return indexOfMember(id, port);
}

int MP1Node::indexOfMember(MemberListEntry &entry) {
    return indexOfMember(entry.getid(), entry.getport());
}

void MP1Node::addNewMember(MessageHdr *data) {
    int id = *(int *) (&data->sourceAddress.addr);
    short port = *(short *) (&data->sourceAddress.addr[4]);

    if ((id == this->id && port == this->port) || (indexOfMember(id, port) > -1)) {
        return;
    }

    log->logNodeAdd(&memberNode->addr, &data->sourceAddress);

    memberNode->memberList.emplace_back(id, port, 1, par->getcurrtime());
}

void MP1Node::serializeMembersToMessage(MessageHdr *message) {
    MemberListEntry* array = reinterpret_cast<MemberListEntry*>(message + 1);

    size_t memberCount = memberNode->memberList.size();

    message->memberCount = memberCount + 1; // include one for self entry

    // put this member node in the list
    array[0] = MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime());

    for (unsigned int i = 0; i < memberCount; i++) {
        const MemberListEntry& member = memberNode->memberList.at(i);

        array[i + 1] = MemberListEntry(member);
    }
}

size_t getExtraSizeForMessage(const std::vector<MemberListEntry>& list) {
    return sizeof(MemberListEntry) * (list.size() + 1);
}

MessageHdr* MP1Node::prepareJoinRepMessage() {
    size_t extraSize = getExtraSizeForMessage(memberNode->memberList);

    MessageHdr *message = prepareMessage(JOINREP, extraSize);

    serializeMembersToMessage(message);

    return message;
}

MessageHdr* MP1Node::preparePingMessage() {
    size_t extraSize = getExtraSizeForMessage(memberNode->memberList);

    MessageHdr *message = prepareMessage(PING, extraSize);

    serializeMembersToMessage(message);

    return message;
}

void MP1Node::handleJoinRep(MessageHdr *data) {
    MemberListEntry* members = reinterpret_cast<MemberListEntry*>(data + 1);

    // Add all nodes in its membership list
    for (unsigned int i = 0; i < data->memberCount; i++) {
        addNewMember(members[i]);
    }

    memberNode->inGroup = true;
}

void MP1Node::addNewMember(const MemberListEntry &newMember) {
    MemberListEntry memberToAdd(newMember);

    Address address = getAddress(memberToAdd);

    if (address == memberNode->addr || indexOfMember(address) > -1) {
        return;
    }

    log->logNodeAdd(&memberNode->addr, &address);

    memberNode->memberList.push_back(std::move(memberToAdd));
}

Address MP1Node::getAddress(const MemberListEntry& entry) {
    return getAddress(entry.id, entry.port);
}

Address MP1Node::getAddress(int id, short port) {
    Address address(std::to_string(id) + ":" + std::to_string(port));

    return address;
}

MessageHdr* MP1Node::prepareMessage(MsgTypes type, size_t additionalSize) {
    MessageHdr *message = (MessageHdr*) malloc(sizeof(MessageHdr) + additionalSize);

    message->msgType = type;
    message->sourceAddress = Address(memberNode->addr);

    return message;
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

    std::vector<MemberListEntry> entriesToKeep;

    for (MemberListEntry& entry : memberNode->memberList) {
        if (!hasNodeFailed(entry)) {
            entriesToKeep.emplace_back(entry);
        } else {
            Address removedAddress(std::to_string(entry.getid()) + ":" + std::to_string(entry.getport()));
            log->logNodeRemove(&memberNode->addr, &removedAddress);
            memberNode->nnb--;
        }
    }

    memberNode->memberList.clear();
    memberNode->memberList.assign(entriesToKeep.begin(), entriesToKeep.end());

    std::set<MemberListEntry*> entriesToPing;

    unsigned int gossipTargetCount = min(memberNode->nnb, FANOUT);

    if (gossipTargetCount == 0) {
        return;
    }

    auto rng = std::default_random_engine(std::random_device{}());
    auto dist = std::uniform_int_distribution<>(0, memberNode->memberList.size() - 1);

    while (entriesToPing.size() < gossipTargetCount) {
        int index = dist(rng);
        entriesToPing.insert(&memberNode->memberList[index]);
    }

    for (MemberListEntry* entry : entriesToPing) {
        Address targetAddress(std::to_string(entry->getid()) + ":" + std::to_string(entry->getport()));
        MessageHdr *msg = preparePingMessage();

        size_t messageSize = sizeof(MessageHdr) + (sizeof(MemberListEntry) * msg->memberCount);

        emulNet->ENsend(&memberNode->addr, &targetAddress, std::string((char*) msg, messageSize));

        delete msg;
    }

    memberNode->heartbeat++;

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

    joinaddr.init();
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
