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
/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
	PING,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
struct MessageHdr {
	enum MsgTypes msgType;
	Address sourceAddress;
	size_t memberCount = 0;
};

static const int FANOUT = 2;

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
	int id;
	short port;
	char NULLADDR[6];
	void handleJoinReq(MessageHdr *data);
	void handleJoinRep(MessageHdr *data);
	void handlePing(MessageHdr *data);
	
	MessageHdr* prepareMessage(MsgTypes type, size_t additionalSize);
	MessageHdr* prepareJoinRepMessage();
	MessageHdr* preparePingMessage();
	void serializeMembersToMessage(MessageHdr *message);

	void addNewMember(MessageHdr *data);
	void addNewMember(const MemberListEntry& entry);

	void updateMember(const Address&);
	void updateMember(MemberListEntry&);

	// Return index of member in memberList.  -1 if not found.
	int indexOfMember(const Address&);
	int indexOfMember(MemberListEntry&);
	int indexOfMember(int id, short port);

	Address getAddress(const MemberListEntry&);
	Address getAddress(int id, short port);

	bool hasNodeFailed(MemberListEntry&);
	bool isNodeSuspicious(MemberListEntry& entry);

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
};

#endif /* _MP1NODE_H_ */
