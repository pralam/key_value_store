/**********************
*
* Progam Name: MP1. Membership Protocol
* 
* Code authors: <your name here>
*
* Current file: mp1_node.c
* About this file: Member Node Implementation
* 
***********************/

#include "mp1_node.h"
#include "emulnet.h"
#include "MPtemplate.h"
#include "log.h"


/*
 *
 * Routines for introducer and current time.
 *
 */

char NULLADDR[] = {0,0,0,0,0,0};
int isnulladdr( address *addr){
    return (memcmp(addr, NULLADDR, 6)==0?1:0);
}
void printlist(memberlist *mlist,int count)
{
	int i=0;
	memberlist *current = NULL;
	for(i=0;i<count;i++)
	{
		current = mlist + i;	
		printf("\naddress = %d.0.0.0 ", current->addr);
		printf(" heartbeat -> %d",current->heartbeat);
		printf(" loacaltime->%d",current->localtime);
	}
}
void process_sendgossip(member *node)
{
	
	int i=0;
	int random=0;	
	messagehdr *msg;
	for(i=0;i<node->count;i++)
	{
		if(memcmp(&node->addr,&node->mlist[i].addr,sizeof(address))==0)
		{
			node->mlist[i].heartbeat +=1;
			node->mlist[i].localtime=getcurrtime();
		}
		
	}
	
	//--------------------------------------------------------------------------
	int currenttime=getcurrtime();
	int delindicator[node->count];
	int tdiff=0,flag=0,tfail=30,tcleanup=30,count=0;
	for(i=0;i<node->count;i++)
	{
		delindicator[i]=0;
	}
	for(i=0;i<node->count;i++)
	{
		tdiff=(currenttime-node->mlist[i].localtime);
		if(tdiff>tfail && node->mlist[i].suspect!=1)
		{	
			node->mlist[i].suspect = 1;
		}
		else if(tdiff>(tfail+tcleanup))
		{
			count+=1;
			delindicator[i]=1;
		}
		else
		{
			flag=1;
		}
		
			
	}
	if(count>0)
	{
		deletenode(node,count,delindicator);
	}
	//-----------------------------------------------------

	random = rand() % (node->count);
	while(memcmp(&node->addr,&node->mlist[random].addr,sizeof(address))==0)
	{
		random = rand() % (node->count);			
	}
	
	size_t msgsize = sizeof(messagehdr) + sizeof(address)+(node->count*sizeof(memberlist));
    	msg=malloc(msgsize);	
	msg->msgtype=DUMMYLASTMSGTYPE;
	
	memcpy((msg+1),&node->addr,sizeof(address));
	memcpy(((char *)msg)+sizeof(messagehdr)+sizeof(address),node->mlist,(node->count*sizeof(memberlist)));
			
	//getchar();
	MPp2psend(&node->addr,&node->mlist[random].addr,(char *)msg,msgsize);
	free(msg);
	
    
    return;
	
}


void Process_receivegossip(void *env, char *data, int size)
{
	
	int i=0;
	int countnodes=0;
	int tsize=0;
	int j=0;
	member *node = (member *) env;
	
	tsize=(size-sizeof(address));
	countnodes=tsize/sizeof(memberlist);
	int currenttime=0;
	int tdiff=0;
	int tfail=30;
	int tcleanup=30;
	int count=0;
	
	memberlist *temp=(memberlist *)(data + sizeof(address));
	int flag = 0;
	for(i=0;i<countnodes;i++)
	{
		if(temp[i].suspect!=1)
		{
			for(j=0;j<node->count;j++)
			{
			
				if(memcmp(&node->mlist[j].addr,&temp[i].addr,sizeof(address))==0)
				{
					flag = 1;
					if(memcmp(&node->addr,&node->mlist[j].addr,sizeof(address))==0)
					{
						node->mlist[j].heartbeat+=1;
						node->mlist[j].localtime=getcurrtime();
					}
					else
					{
						if(temp[i].heartbeat > node->mlist[j].heartbeat)
						{
							node->mlist[j].heartbeat=temp[i].heartbeat;
							if(node->mlist[j].suspect==1)
							{
								node->mlist[j].suspect=0;
							}
							node->mlist[j].localtime=getcurrtime();
						}	
					}
				
				}					
			}
				if(flag==0) recalculate(temp,node,i);	
				flag = 0;
		}
	}

	currenttime=getcurrtime();
	int delindicator[node->count];
	flag=0;
	for(i=0;i<node->count;i++)
	{
		delindicator[i]=0;
	}
	for(i=0;i<node->count;i++)
	{
		tdiff=(currenttime-node->mlist[i].localtime);
		if(tdiff>tfail && node->mlist[i].suspect!=1)
		{	
			node->mlist[i].suspect = 1;
		}
		else if(tdiff>(tfail+tcleanup))
		{
			count+=1;
			delindicator[i]=1;
		}
		else
		{
			flag=1;
		}
		
			
	}
	if(count>0)
	{
		deletenode(node,count,delindicator);
	}
	
}
void deletenode(member *node,int count, int delindicator[])
{
	//printf("In delete node");
	int i=0;
	int j=0;	
	//printf("\ncount=%d",count);
	memberlist *templist =(struct memberlist *)malloc(((node->count)-count)*(sizeof(memberlist)));
	for(i=0;i<node->count;i++)
	{
		if(delindicator[i]==0)
		{
			memcpy(&templist[j],&node->mlist[i],sizeof(memberlist));
			j++;
		}	
	}
	
	for(i=0;i<node->count;i++)
	{
		if(delindicator[i]==1)
		{
			logNodeRemove(&node->addr,&node->mlist[i]);
			//printf("The deletednode is%d",node->mlist[i].addr);
		}
	}

	//printf("count=%d",node->count);
	node->count=((node->count)-count);
	free(node->mlist);
	node->mlist = templist;	
}
void recalculate(memberlist *temp, member *node,int i)
{
	int flag=0;
	int j=0;
	for(j=0;j<node->count;j++)
	{	
		if(memcmp(&node->mlist[j].addr,&temp[i].addr,sizeof(address))==0)
		{
			flag=1;
			break;
		}
	}
	if (flag==0)
	{
		node->count+=1;
		node->mlist= (struct memberlist *)realloc(node->mlist,(node->count*(sizeof(memberlist))));
		memcpy(&node->mlist[(node->count)-1].addr,&temp[i].addr,sizeof(address));
		node->mlist[(node->count)-1].heartbeat=temp[i].heartbeat;
		node->mlist[(node->count)-1].localtime=getcurrtime();	
		logNodeAdd(&node->addr,&temp[i].addr);		
	}	
			
}

/* 
Return the address of the introducer member. 
*/
address getjoinaddr(void)
{
    address joinaddr;

    memset(&joinaddr, 0, sizeof(address));
    *(int *)(&joinaddr.addr)=1;
    *(short *)(&joinaddr.addr[4])=0;

    return joinaddr;
}

/*
 *
 * Message Processing routines.
 *
 */

/* 
Received a JOINREQ (joinrequest) message.
*/
void Process_joinreq(void *env, char *data, int size)
{
	member *node = (member *) env;
	messagehdr *msg;
	address *addr=(address *) data;

	
	node->count +=1;
	node->mlist= (struct memberlist *)realloc(node->mlist,(node->count*(sizeof(memberlist))));
	memcpy(&node->mlist[(node->count)-1].addr,addr,sizeof(address));
	node->mlist[(node->count)-1].heartbeat=0;
	node->mlist[(node->count)-1].localtime=getcurrtime();
	logNodeAdd(&node->addr,addr);
		
	size_t msgsize = sizeof(messagehdr) + sizeof(address)+(node->count*sizeof(memberlist));
    	msg=malloc(msgsize);
	msg->msgtype=JOINREP;
	
	memcpy((msg+1),addr,sizeof(address));
	memcpy(((char *)msg)+sizeof(messagehdr)+sizeof(address),node->mlist,(node->count*sizeof(memberlist)));
	//getchar();
	MPp2psend(&node->addr,addr,(char *)msg,msgsize);
	
     	free(msg);
	
    	
    return;
}

 
//Received a JOINREP (joinreply) message. 

void Process_joinrep(void *env, char *data, int size)
{
	
	int i=0;
	int countnodes=0;
	int tsize=0;
	member *node = (member *) env;
	
	node->ingroup=1;

	tsize=(size-sizeof(address));
	countnodes=tsize/sizeof(memberlist);
	
	node->count=countnodes;

	memberlist *temp=(memberlist *)(data + sizeof(address));
	node->mlist = malloc(countnodes * sizeof(memberlist));
	memcpy(node->mlist,temp,(countnodes * sizeof(memberlist)));
	
	
	
	for(i=0;i<countnodes;i++)
	{
		logNodeAdd(&node->addr,&temp[i].addr);
		
		if(memcmp(&node->addr,&node->mlist[i].addr,sizeof(address))==0)
		{
			node->mlist[i].heartbeat =1;
			node->mlist[i].localtime=getcurrtime();
			
		}
		
	}
	
    return;
	
}
/* 
		
Array of Message handlers. 
*/
void ( ( * MsgHandler [20] ) STDCLLBKARGS )={
/* Message processing operations at the P2P layer. */
    Process_joinreq, 
    Process_joinrep,
    Process_receivegossip
};

/* 
Called from nodeloop() on each received packet dequeue()-ed from node->inmsgq. 
Parse the packet, extract information and process. 
env is member *node, data is 'messagehdr'. 
*/
int recv_callback(void *env, char *data, int size){

    member *node = (member *) env;
    messagehdr *msghdr = (messagehdr *)data;
    char *pktdata = (char *)(msghdr+1);

    if(size < sizeof(messagehdr)){
#ifdef DEBUG
        LOG(&((member *)env)->addr, "Faulty packet received - ignoring");
#endif
        return -1;
    }

#ifdef DEBUGLOG
    LOG(&((member *)env)->addr, "Received msg type %d with %d B payload", msghdr->msgtype, size - sizeof(messagehdr));
#endif

    if((node->ingroup && msghdr->msgtype >= 0 && msghdr->msgtype <= DUMMYLASTMSGTYPE)
        || (!node->ingroup && msghdr->msgtype==JOINREP))            
            /* if not yet in group, accept only JOINREPs */
        MsgHandler[msghdr->msgtype](env, pktdata, size-sizeof(messagehdr));
    /* else ignore (garbled message) */
    free(data);

    return 0;

}

/*
 *
 * Initialization and cleanup routines.
 *
 */

/* 
Find out who I am, and start up. 
*/
int init_thisnode(member *thisnode, address *joinaddr){
    
    if(MPinit(&thisnode->addr, PORTNUM, (char *)joinaddr)== NULL){ /* Calls ENInit */
#ifdef DEBUGLOG
        LOG(&thisnode->addr, "MPInit failed");
#endif
        exit(1);
    }
#ifdef DEBUGLOG
    else LOG(&thisnode->addr, "MPInit succeeded. Hello.");
#endif

    thisnode->bfailed=0;
    thisnode->inited=1;
    thisnode->ingroup=0;
    /* node is up! */

    return 0;
}


/* 
Clean up this node. 
*/
int finishup_thisnode(member *node){
free(node->mlist);

	
    return 0;
}


/* 
 *
 * Main code for a node 
 *
 */

/* 
Introduce self to group. 
*/
int introduceselftogroup(member *node, address *joinaddr){
    
    messagehdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if(memcmp(&node->addr, joinaddr, 4*sizeof(char)) == 0){
        /* I am the group booter (first process to join the group). Boot up the group. */
#ifdef DEBUGLOG
        LOG(&node->addr, "Starting up group...");
#endif
	node->count=1;
	memberlist *mlist =(struct memberlist *)(malloc(node->count*(sizeof(memberlist))));
	memcpy(&mlist[(node->count)-1].addr,&node->addr,sizeof(address));	
	mlist->heartbeat=1;
	mlist->localtime=getcurrtime();
	logNodeAdd(&node->addr,&node->addr);
        node->ingroup = 1;
	
	node->mlist=mlist;
	
	
    }
    else{
        size_t msgsize = sizeof(messagehdr) + sizeof(address);
        msg=malloc(msgsize);

    /* create JOINREQ message: format of data is {struct address myaddr} */
        msg->msgtype=JOINREQ;
        memcpy((char *)(msg+1), &node->addr, sizeof(address));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        LOG(&node->addr, s);
#endif

    /* send JOINREQ message to introducer member. */
        MPp2psend(&node->addr, joinaddr, (char *)msg, msgsize);
        
        free(msg);
    }

    return 1;

}

/* 
Called from nodeloop(). 
*/
void checkmsgs(member *node){
    void *data;
    int size;

    /* Dequeue waiting messages from node->inmsgq and process them. */
	
    while((data = dequeue(&node->inmsgq, &size)) != NULL) {
        recv_callback((void *)node, data, size); 
    }
    return;
}


/* 
Executed periodically for each member. 
Performs necessary periodic operations. 
Called by nodeloop(). 
*/
void nodeloopops(member *node){
	process_sendgossip(node);	
    return;
}

/* 
Executed periodically at each member. Called from app.c.
*/
void nodeloop(member *node){
    if (node->bfailed) return;

    checkmsgs(node);

    /* Wait until you're in the group... */
    if(!node->ingroup) return;

    /* ...then jump in and share your responsibilites! */
    nodeloopops(node);
    
    return;
}

/* 
All initialization routines for a member. Called by app.c. 
*/
void nodestart(member *node, char *servaddrstr, short servport){

    address joinaddr=getjoinaddr();

    /* Self booting routines */
    if(init_thisnode(node, &joinaddr) == -1){

#ifdef DEBUGLOG
        LOG(&node->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if(!introduceselftogroup(node, &joinaddr)){
        finishup_thisnode(node);
#ifdef DEBUGLOG
        LOG(&node->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/* 
Enqueue a message (buff) onto the queue env. 
*/
int enqueue_wrppr(void *env, char *buff, int size){    return enqueue((queue *)env, buff, size);}

/* 
Called by a member to receive messages currently waiting for it. 
*/
int recvloop(member *node){
    if (node->bfailed) return -1;
    else return MPrecv(&(node->addr), enqueue_wrppr, NULL, 1, &node->inmsgq); 
    /* Fourth parameter specifies number of times to 'loop'. */
}

