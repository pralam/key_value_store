/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) 
{
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	//map<int,string> logmap;
	//map<int,vector<bool>> result_map;
	//vector<bool> result_vector;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
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
void MP2Node::updateRing() 
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	 /*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	vector<Node> curMemList;
	bool haschanged = false;
	curMemList = getMembershipList();
	//cout << ring.size() ; 

	// If first time, assign currMemList to ring and find original has and have replicas
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(this->ring.size() == 0)
	{
		sort(curMemList.begin(), curMemList.end());
		this->ring = curMemList;
		//oldMemList = curMemList;
		//storeReplica(1);
	}
	else
	{
        sort(curMemList.begin(), curMemList.end());
		if(curMemList.size() != ring.size())
        {
         	//cout << "the change is " << curMemList[i].nodeHashCode << '\n';
         	haschanged = true;

        }
        else
        {
         	for ( int i = 0; i<curMemList.size() ; i++)
         	{
         	//cout << "comparing" << curMemList[i].nodeHashCode << " and " << ring[i].nodeHashCode <<'\n';
         		if(curMemList[i].nodeHashCode != ring[i].nodeHashCode)
         		{
         			haschanged = true;
         		//cout << "the change is " << curMemList[i].nodeHashCode << '\n';
         			break;
         		}
         	}
       	}

    }

 	if(haschanged)
 	{
 		sort(curMemList.begin(), curMemList.end());
		stabilizationProtocol(curMemList);
		this->ring = curMemList;
	}
 	else
 	{
 		this->ring = curMemList;
 	}
 	/*cout<<"-------------------------------";
 	for(int i = 0; i < ring.size(); i++)
    {
        //cout<<ring.at(i).getAddress()<<endl;
        Address addr=ring.at(i).nodeAddress;
        cout<<addr.getAddress()<<endl;
 	}*/
}

//This function maintains has and have replicas of all the nodes. 
void MP2Node::storeReplica(ReplicaType type,string key)
{
	bool success;
	bool alive;
	vector<Node> replicas;
	replicas = findNodes(key);
	
	if(type==PRIMARY)
	{
    	alive = isElementAlive(this->hasReplicas, replicas.at(1));
       	 if (!alive)
        	this->hasReplicas.emplace_back(replicas.at(1));
        alive = isElementAlive(this->hasReplicas, replicas.at(2));
        if (!alive)
       		this->hasReplicas.emplace_back(replicas.at(2));
    }
    if(type==SECONDARY)
    {

		alive = isElementAlive(this->haveReplicas, replicas.at(0));
		if (!alive)
			this->haveReplicas.emplace_back(replicas.at(0));
	}
	if(type==TERTIARY)
	{
		alive = isElementAlive(this->haveReplicas, replicas.at(0));
		if (!alive)
			this->haveReplicas.emplace_back(replicas.at(0));
		alive = isElementAlive(this->haveReplicas, replicas.at(1));
		if (!alive)
			this->haveReplicas.emplace_back(replicas.at(1));
	 }
	 replicas.clear();
}
//This function checks whether node is alive in the list 
bool MP2Node::isElementAlive(vector<Node> List, Node node)
{
    for(vector<Node>::iterator it = List.begin() ; it != List.end() ; ++it)
    {
        if ((*it).nodeHashCode == node.getHashCode())
			return true;
    }
    return false;
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

void MP2Node::clientCreate(string key, string value)
 {
	/*
	 * Implement this
	 */
	vector<Node> replica = findNodes(key);
    int transaction_id = par->getcurrtime()+g_transID;
    Address fromAddr = ((this->getMemberNode())->addr);
   // cout<<fromAddr.getAddress()<<endl;
    MessageType msg_type = MessageType::CREATE;
    // Send message to first replica
    Node node_replica = replica.at(0);
    ReplicaType replica_type = ReplicaType::PRIMARY;

    Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
	string string_logmessage = logmessage->toString();
	this->logmap[transaction_id]=string_logmessage;
	/*for (map<int,string>::iterator it=logmap.begin(); it!=logmap.end(); it++)
    {
    	cout <<"id="<<it->first<<"message"<<it->second<<endl;
    }
    cout<<"end"<<endl;*/
    send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);

  // Send message to second replica
    node_replica = replica.at(1);
    replica_type = ReplicaType::SECONDARY;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);
   // Send message to third replica 
 	node_replica = replica.at(2);
    replica_type = ReplicaType::TERTIARY;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);
 	g_transID++;
   	//(this->log)->logCreateSuccess(&(fromAddr), prime_replica.getAddress(), transID, key, value);
   	//(this->log)->logCreateSuccess(&(fromAddr), third_replica.getAddress(), transID, key, value);
}
void MP2Node::send_message(Node replica, int transaction_id, Address fromAddr, MessageType msg_type, string key, string value, ReplicaType replica_type)
{
    	Message* message_replica = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
	    string msg_replica_string = message_replica->toString();
		//cout<<msg_replica_string<<endl;
		emulNet->ENsend(&(fromAddr), &replica.nodeAddress, (char*)(msg_replica_string.c_str()), msg_replica_string.size());
    	//(this->log)->LOG(&(fromAddr), "CREATE message to... The message is: ");
    	//(this->log)->LOG(&(fromAddr), msg_replica_string.c_str());
}
void MP2Node::send_message(Node replica, int transaction_id, Address fromAddr, MessageType msg_type, string key)
{
    	Message* message_replica = new Message(transaction_id, fromAddr, msg_type, key);
		string msg_replica_string = message_replica->toString();
		//cout<<msg_replica_string<<endl;
		emulNet->ENsend(&(fromAddr), &replica.nodeAddress, (char*)(msg_replica_string.c_str()), msg_replica_string.size());
    	//(this->log)->LOG(&(fromAddr), "pralams msg to... The message is: ");
    	//(this->log)->LOG(&(fromAddr), msg_replica_string.c_str());
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
void MP2Node::clientRead(string key)
{
	/*
	 * Implement this
	 */
	vector<Node> replica = findNodes(key);
    int transaction_id = par->getcurrtime()+g_transID;
    Address fromAddr = ((this->getMemberNode())->addr);
    MessageType msg_type = MessageType::READ;
    // Send message to first replica
    Node node_replica = replica.at(0);
    ReplicaType replica_type = ReplicaType::PRIMARY;
    Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key);
	string string_logmessage = logmessage->toString();
	this->logmap[transaction_id]=string_logmessage;
	//cout<<"sent msg with type "<<msg_type<<"with address "<<&node_replica.nodeAddress<<endl;
    send_message(node_replica,transaction_id,fromAddr,msg_type,key);
  // Send message to second replica
    node_replica = replica.at(1);
    replica_type = ReplicaType::SECONDARY;
    //cout<<"sent msg with type"<<msg_type<<"with address "<<&node_replica.nodeAddress<<endl;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key);
   // Send message to third replica 
 	node_replica = replica.at(2);
    replica_type = ReplicaType::TERTIARY;
    //cout<<"sent msg with type"<<msg_type<<"with address "<<&node_replica.nodeAddress<<endl;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key);
 	g_transID++;
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
void MP2Node::clientUpdate(string key, string value)
{
	/*
	 * Implement this
	 */
	vector<Node> replica = findNodes(key);
    int transaction_id = par->getcurrtime()+g_transID;
    Address fromAddr = ((this->getMemberNode())->addr);
   	MessageType msg_type = MessageType::UPDATE;
    // Send message to first replica
    Node node_replica = replica.at(0);
    ReplicaType replica_type = ReplicaType::PRIMARY;
    Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
	string string_logmessage = logmessage->toString();
	this->logmap[transaction_id]=string_logmessage;

    send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);
  // Send message to second replica
    node_replica = replica.at(1);
    replica_type = ReplicaType::SECONDARY;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);
   // Send message to third replica 
 	node_replica = replica.at(2);
    replica_type = ReplicaType::TERTIARY;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key,value,replica_type);
 	g_transID++;
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
void MP2Node::clientDelete(string key)
{
	/*
	 * Implement this
	 */
	vector<Node> replica = findNodes(key);
    int transaction_id = par->getcurrtime()+g_transID;
    Address fromAddr = ((this->getMemberNode())->addr);
    MessageType msg_type = MessageType::DELETE;
  // Send message to first replica
    Node node_replica = replica.at(0);
    ReplicaType replica_type = ReplicaType::PRIMARY;
    Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key);
	string string_logmessage = logmessage->toString();
	this->logmap[transaction_id]=string_logmessage;
	//cout<<"message sent with type"<<msg_type<<"with address"<<node_replica.nodeAddress<<endl;
    send_message(node_replica,transaction_id,fromAddr,msg_type,key);
  // Send message to second replica
    node_replica = replica.at(1);
    replica_type = ReplicaType::SECONDARY;
    //cout<<"message sent with type"<<msg_type<<"with address"<<node_replica.getAddress()<<endl;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key);
  // Send message to third replica 
 	node_replica = replica.at(2);
    replica_type = ReplicaType::TERTIARY;
    //cout<<"message sent with type"<<msg_type<<"with address"<<node_replica.getAddress()<<endl;
 	send_message(node_replica,transaction_id,fromAddr,msg_type,key);
 	g_transID++;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int transaction_id)
{
	//cout<<"inside create key value"<<endl;
	int timestamp=par->getcurrtime();
	Entry* entry=new Entry(value,timestamp,replica);
	string val=entry->convertToString();
	bool result=ht->create(key, val);
	//
	Entry* ent=new Entry(val);
	val=ent->value;
	//cout<<val<<endl;
	//cout<<"extracted value is"<<val<<endl;
	 //cout<<result;
	int trans_id=transaction_id;
	Address from_addr = ((this->getMemberNode())->addr);
	bool isCoordinator=false;
	//cout<<"printed log"<<endl;
	(this->log)->logCreateSuccess(&(from_addr),isCoordinator , trans_id, key, val);
	//cout<<"exited from create key value"<<endl;
	return result;

	// Insert key, value, replicaType into the hash table
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int transaction_id) 
{
	//cout<<"inside readkey"<<endl;
	bool isCoordinator=false;
	string returnedvalue=ht->read(key);
	string val;
	//cout<<returnedvalue<<endl;
	if(returnedvalue!="")
	{
		Entry* ent=new Entry(returnedvalue);
		val=ent->value;
	}
	//cout<<"returnedvalue="<<returnedvalue<<endl;
	Address from_addr = ((this->getMemberNode())->addr);
	if(returnedvalue=="")
	{
		//cout<<"replica printed success log"<<endl;
		(this->log)->logReadFail(&(from_addr),isCoordinator , transaction_id, key);	
	}
	else
	{
		//cout<<"replica printed failed log"<<endl;
		(this->log)->logReadSuccess(&(from_addr),isCoordinator , transaction_id, key,val);		
	}
	return returnedvalue;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int transaction_id) 
{
	
	// Update key in local hash table and return true or false
	int timestamp=par->getcurrtime();
	Entry* entry=new Entry(value,timestamp,replica);
	string val=entry->convertToString();
	bool result=ht->update(key, val);
	 //cout<<result;
	int trans_id=transaction_id;
	Address from_addr = ((this->getMemberNode())->addr);
	bool isCoordinator=false;
	if(result==1)
	{

		(this->log)->logUpdateSuccess(&(from_addr),isCoordinator , trans_id, key, value);
	}
	else
	{
		(this->log)->logUpdateFail(&(from_addr),isCoordinator , trans_id, key, value);	
	}
	
	return result;

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int transaction_id) 
{
	bool result=ht->deleteKey(key);
	int trans_id=transaction_id;
	Address from_addr = ((this->getMemberNode())->addr);
	bool isCoordinator=false;
	if(result==1)
	{
		(this->log)->logDeleteSuccess(&(from_addr),isCoordinator,trans_id,key);
	}
	else
	{
		(this->log)->logDeleteFail(&(from_addr),isCoordinator,trans_id,key);
	}
	return result;

}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() 
{
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;
	while ( !memberNode->mp2q.empty() )
	{
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		//cout<<message<<endl;
		Message *msg=new Message(message);
		MessageType msgtype=msg->type;
		//cout<<msg->type<<endl;
		int transaction_id=msg->transID;
		
		//cout<<"msg type="<<msgtype<<endl;
		//cout<<"key="<<key<<endl;
		//cout<<"value="<<value<<endl;
		//cout<<"replicatype="<<replica_type<<endl;
		if(msgtype==UPDATE)
		{
			//cout<<"in delete"<<endl;
			string key=msg->key;	
			string value=msg->value;
			//cout<<"value in update="<<value<<endl;
			ReplicaType replica_type=msg->replica;
			bool r=updateKeyValue(key,value,replica_type,transaction_id);
			Address from_addr = ((this->getMemberNode())->addr);
			Address to_addr=msg->fromAddr;
			MessageType type = MessageType::REPLY;
			Message *msg1=new Message(transaction_id, from_addr,type, r);
			string msg_string = msg1->toString();
			emulNet->ENsend(&(from_addr), &(to_addr), (char*)(msg_string.c_str()), msg_string.size());
		}
		if(msgtype==DELETE)
		{
			//cout<<"in delete"<<endl;
			string key=msg->key;
			bool r=deletekey(key,transaction_id);
			Address from_addr = ((this->getMemberNode())->addr);
			Address to_addr=msg->fromAddr;
			MessageType type = MessageType::REPLY;
			Message *msg1=new Message(transaction_id, from_addr,type, r);
			string msg_string = msg1->toString();
			emulNet->ENsend(&(from_addr), &(to_addr), (char*)(msg_string.c_str()), msg_string.size());
		}
		if(msgtype==CREATE)	
		{
			//cout<<"inside create"<<endl;

			string value=msg->value;
			string key=msg->key;
			ReplicaType replica_type=msg->replica;
			storeReplica(replica_type,key);
			MessageType type = MessageType::REPLY;
			Address to_addr=msg->fromAddr;
			Address from_addr = ((this->getMemberNode())->addr);
			bool r=createKeyValue(key, value,replica_type,transaction_id);
			//construct a reply message
			Message* msg=new Message(transaction_id, from_addr, type, r);
			string msg_string = msg->toString();
			emulNet->ENsend(&(from_addr), &(to_addr), (char*)(msg_string.c_str()), msg_string.size());
			//cout<<"exited from create"<<endl;
		}
		if(msgtype==READ)
		{
			Address from_addr = ((this->getMemberNode())->addr);
			string key=msg->key;
			string value=readKey(key, transaction_id);
			Address to_addr=msg->fromAddr;
			Message* readmsg=new Message(transaction_id, from_addr, value);
			string readmsg_string = readmsg->toString();
			emulNet->ENsend(&(from_addr), &(to_addr), (char*)(readmsg_string.c_str()), readmsg_string.size());
					
		}

		if(msgtype==REPLY)
		{
			//cout<<"in reply msg"<<endl;	
			bool r=msg->success ;
			
			if (result_map.find(transaction_id) != result_map.end()) 
			{
  				// found
  				vector<bool> temp;
  				temp=this->result_map[transaction_id];
  				temp.push_back(r);
  				//result_map.insert(transaction_id,result_vector);
  				result_map[transaction_id]=temp;
  				temp.clear();
  				// not found
				//cout<<"inside if"<<endl;
			} 
			else 
			{
				result_vector.push_back(r);
				//result_map.insert(transaction_id, result_vector);
				result_map[transaction_id]=result_vector;
				result_vector.clear();
			}
			
				
		}
		if(msgtype==READREPLY)
		{
			string r1=msg->value;
			//cout<<"r1="<<r1;
			//Entry *entry=new Entry(r1);
			//string r=entry->value;
			//cout<<r<<endl;
			if (result_readmap.find(transaction_id) != result_readmap.end()) 
			{
  				// found
  				vector<string> temp;
  				temp=this->result_readmap[transaction_id];
  				temp.push_back(r1);
  				//result_map.insert(transaction_id,result_vector);
  				result_readmap[transaction_id]=temp;
  				temp.clear();
  				// not found
				//cout<<"inside if"<<endl;
			} 
			else 
			{
				result_readvector.push_back(r1);
				//result_map.insert(transaction_id, result_vector);
				result_readmap[transaction_id]=result_readvector;
				result_readvector.clear();
			}
		}


	} //end-while

	//function to check quorum for create update and delete
	for (map<int,vector<bool>>::iterator it=result_map.begin(); it!=result_map.end(); it++)
    {
    	//cout<<"inside create quorum"<<endl;

    	int trans_id=it->first;
    	string newmsg1=logmap[trans_id];	
    	Message* msg1=new Message(newmsg1);
    	Address from_addr=msg1->fromAddr;
    	string key=msg1->key;
    	string value=msg1->value;
    	int mycount=0;
    	vector<bool> new1;
    	MessageType msgtype=msg1->type;
   		//cout<<"msg type="<<msgtype<<endl;
   		//cout <<"For msg "<<it->first<<"->";
		new1=this->result_map[trans_id];
		mycount = std::count (new1.begin(), new1.end(), 1);
		//cout<<it->first<<"-->"<<mycount<<endl;
		bool isCoordinator=true;
		if(msgtype==CREATE)
		{
			if(mycount>1)
			{
				//cout<<"printed log"<<endl;
				(this->log)->logCreateSuccess(&(from_addr),isCoordinator , trans_id, key, value);
			}
			else
			{
				(this->log)->logCreateFail(&(from_addr),isCoordinator , trans_id, key, value);	
			}
		}
		if(msgtype==DELETE)
		{
			
			if(mycount>1)
			{
				//cout<<"printed log"<<endl;
				(this->log)->logDeleteSuccess(&(from_addr),isCoordinator,trans_id,key);
			}
			else
			{
				(this->log)->logDeleteFail(&(from_addr),isCoordinator,trans_id,key);
			}	
		}
		if(msgtype==UPDATE)
		{
			if(mycount>1)
			{
				//cout<<"printed log"<<endl;
				(this->log)->logUpdateSuccess(&(from_addr),isCoordinator , trans_id, key, value);
			}
			else
			{
				(this->log)->logUpdateFail(&(from_addr),isCoordinator , trans_id, key, value);		
			}
		}
		//cout<<"exited from create quorum"<<endl;

	}
	result_map.clear();
	//function to check read quorum
	for (map<int,vector<string>>::iterator it=result_readmap.begin(); it!=result_readmap.end(); it++)
    {
    	//cout<<"inside read quorum"<<endl;
    	int trans_id=it->first;
    	//vector<string>tempval;
    	//tempval=it->second;
    	//string val=val_process(tempval);
    	string newmsg1=logmap[trans_id];	
    	Message* msg1=new Message(newmsg1);
    	//Address from_addr=msg1->fromAddr;
    	Address from_addr=this->getMemberNode()->addr;
    	string key=msg1->key;
    	string value;
    	string val;
    	int mycount=0;
    	int nullcount;
    	vector<string> new1;
    	MessageType msgtype=msg1->type;
    	new1=this->result_readmap[trans_id];
		for (vector <string>::iterator it2=new1.begin(); it2!=new1.end(); it2++)
    	{
			if((*it2)!="")
			{
				mycount++;
				value = (*it2);
				
			}
			    	
    	}
    	//cout<<value<<endl;
    	if(value!="")
    	{
    		Entry* ent=new Entry(value);
			val=ent->value;
		}
		//cout<<"val="<<val<<endl;
    	//cout<<"above count is "<<mycount<<endl;
		//cout<<it->first<<"-->"<<mycount<<endl;
		bool isCoordinator=true;
		//if(msgtype==READREPLY)
		//{
			if(mycount>1)
			{
				//cout<<"coordinator printed success log"<<endl;
				(this->log)->logReadSuccess(&(from_addr),isCoordinator , trans_id, key,val);		
			}
			else
			{
				//cout<<"coordinator printed failed log"<<endl;
				(this->log)->logReadFail(&(from_addr),isCoordinator , trans_id, key);	
			}

	}
	result_readmap.clear();


	
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
vector<Node> MP2Node::findNodes(string key)
 {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) 
		{
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else
		{
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
void MP2Node::stabilizationProtocol(vector<Node> MembershipList) 
{
	//cout<<"Inside stabilizationProtocol"<<endl;
	/*
	 * Implement this
	 */
    size_t position;
    vector<Node> failed;
    bool hasReplica = false;
    bool haveReplica = false;
    Node hasReplicaFailed;
    Node haveReplicaFailed;
    vector<Node> neighbours;
    string key, value;
    Message *msg;
    Node updatedPrimaryNode;
    //current nodes hash code
    position = (Node(memberNode->addr)).getHashCode();

    // Detect failed Node in Ring with the help of Membership List
    for(vector<Node>::iterator it = ring.begin();it < ring.end(); ++it)
    {
        if(!isElementAlive(MembershipList, *it))
        failed.emplace_back(Node((*it).nodeAddress));

    }
    // Check if failed replica node is present in hasReplicas
    for(vector<Node>::iterator it = failed.begin();it < failed.end(); ++it)
    {
        if(isElementAlive(hasReplicas, *it))
        {
            hasReplica = true;
            hasReplicaFailed = *it;
            break;
        }    
    }

    // Check if failed replica node is present in haveReplicas
    for(vector<Node>::iterator it = failed.begin();it < failed.end(); ++it)
    {
        if(isElementAlive(haveReplicasOf, *it))
        {
            haveReplica = true;
            haveReplicaFailed = *it;
            break;
        }    
    }
    // Stabilize if my SECONDARY or TERTIARY failed
    if(hasReplica==true)
    {
        // find neighbours and position at which node is deleted
        //hasMyKeys = myNeighbours(MembershipList, position);
        
    	for(int i=0; i<MembershipList.size(); i++)
    	{
        	if(position == MembershipList.at(i).getHashCode())
        	{
            	neighbours.emplace_back(MembershipList.at(i % MembershipList.size()));
            	neighbours.emplace_back(MembershipList.at((i+1) % MembershipList.size()));
            	neighbours.emplace_back(MembershipList.at((i+2) % MembershipList.size()));
        	}
    	}
        
        int deletenode = 0;
    	for(vector<Node>::iterator it = hasReplicas.begin();it < hasReplicas.end(); it++)
    	{
        	if((*it).nodeHashCode!=hasReplicaFailed.getHashCode())
            	deletenode++;
        	else
            	break;
    	}
        
        // Update hasReplicas by new neighbour
        hasReplicas.erase(hasReplicas.begin()+deletenode);
		hasReplicas.emplace_back(neighbours.at(2));
      
        for(map<string, string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
        {
            key = it->first;
            value = it->second;
            Entry* ent = new Entry(value);
            value=ent->value;

            if(ent->replica == PRIMARY)
            {
                // Genereate new transactionID 
                int transaction_id = par->getcurrtime()+g_transID;
    			Address fromAddr = ((this->getMemberNode())->addr);
    			MessageType msg_type = MessageType::CREATE;
				ReplicaType replica_type = ReplicaType::TERTIARY;
				Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
				string string_logmessage = logmessage->toString();
				this->logmap[transaction_id]=string_logmessage;
                this->emulNet->ENsend(&(memberNode->addr), &(neighbours.at(2).nodeAddress), string_logmessage);
               
            }
        }
        hasReplica = false;
    }
    neighbours.clear();

    // Stabilize If predecessors fail
    if(haveReplica==true)
    {
        // find neighbours
    	for(int i=0; i<MembershipList.size(); i++)
    	{
        	if(position == MembershipList.at(i).getHashCode())
        	{
            	neighbours.emplace_back(MembershipList.at(i % MembershipList.size()));
            	neighbours.emplace_back(MembershipList.at((i+1) % MembershipList.size()));
            	neighbours.emplace_back(MembershipList.at((i+2) % MembershipList.size()));
        	}
    	}
        // Stabilize when my immediate predecessor failed
        if(haveReplicas.at(0).nodeHashCode == haveReplicaFailed.getHashCode())
        {
            for(map<string, string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
            {
                key = it->first;
                value = it->second;
                //e_log = new Entry(value);
                Entry* ent = new Entry(value);
            	value=ent->value;
                if(ent->replica == SECONDARY)
                {
                	int transaction_id = par->getcurrtime()+g_transID;
    				Address fromAddr = ((this->getMemberNode())->addr);
    				MessageType msg_type = MessageType::CREATE;
					ReplicaType replica_type = ReplicaType::TERTIARY;
					Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
					string string_logmessage = logmessage->toString();
					this->emulNet->ENsend(&(memberNode->addr), &(neighbours.at(2).nodeAddress), string_logmessage);
					this->logmap[transaction_id]=string_logmessage;
                    
                }
            }
        }
        neighbours.clear();
         // Stabilize if PRIMARY failed
        if(haveReplicas.at(1).nodeHashCode == haveReplicaFailed.getHashCode())
        {
            // find neighbour from the ring and update it as PRIMARY node
            //hasMyKeys = myNeighbours(ring, haveReplicaFailed.getHashCode());
            for(int i=0; i<ring.size(); i++)
    		{
        		if( haveReplicaFailed.getHashCode()== ring.at(i).getHashCode())
        		{
            		neighbours.emplace_back(ring.at(i % ring.size()));
            		neighbours.emplace_back(ring.at((i+1) % ring.size()));
            		neighbours.emplace_back(ring.at((i+2) % ring.size()));
        		}
    		}
            updatedPrimaryNode = neighbours.at(1);        

            int deletenode = 0;
    		for(vector<Node>::iterator it = haveReplicas.begin();it < haveReplicas.end(); it++)
    		{
        		if((*it).nodeHashCode!=haveReplicaFailed.getHashCode())
            		deletenode++;
        		else
            		break;
    		}
            neighbours.clear();
           
            for(int i=0; i<ring.size(); i++)
    		{
        		if( Node(memberNode->addr).getHashCode()== MembershipList.at(i).getHashCode())
        		{
            		neighbours.emplace_back(MembershipList.at(i % MembershipList.size()));
            		neighbours.emplace_back(MembershipList.at((i+1) % MembershipList.size()));
            		neighbours.emplace_back(MembershipList.at((i+2) % MembershipList.size()));
        		}
    		}
            haveReplicas.erase(haveReplicas.begin()+deletenode);
            haveReplicas.emplace_back(neighbours.at(1));
            
            for(map<string, string>::iterator it = ht->hashTable.begin(); it != ht->hashTable.end(); ++it)
            {
                key = it->first;
                value = it->second;
                Entry* ent = new Entry(value);
                value=ent->value;
                if(ent->replica == TERTIARY)
                {
                    // Generate new transaction ID
                    g_transID++;
                    int transaction_id = par->getcurrtime()+g_transID;
    				Address fromAddr = ((this->getMemberNode())->addr);
    				MessageType msg_type = MessageType::CREATE;
					ReplicaType replica_type = ReplicaType::TERTIARY;
					Message* logmessage = new Message(transaction_id, fromAddr, msg_type, key, value, replica_type);
					string string_logmessage = logmessage->toString();
					this->emulNet->ENsend(&(memberNode->addr), &(neighbours.at(1).nodeAddress), string_logmessage);
					this->logmap[transaction_id]=string_logmessage; 

                }           
            }
        }
        neighbours.clear();
        haveReplica = false;

    }
}

