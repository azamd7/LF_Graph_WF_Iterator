#include<iostream>
#include<float.h>
#include<stdint.h>
#include<stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <vector>
#include <ctime>        // std::time
#include <random>
#include <algorithm>
#include <iterator>
#include <math.h>
#include <time.h>
#include <fstream>
#include <iomanip>
#include <sys/time.h>
#include <atomic>
#include<list>
#include<queue>
#include<stack>
#include<atomic>
using namespace std;

ofstream coutt("getpath.txt");
// freopen("error.txt", "w", stderr );
  
inline int is_marked_ref(long i){
  return (int) (i & 0x1L);
}

inline long unset_mark(long i){
  i &= ~0x1L;
  return i;
}

inline long set_mark(long i){
  i |= 0x1L;
  return i;
}

inline long get_unmarked_ref(long w){
  return w & ~0x1L;
}

inline long get_marked_ref(long w){
  return w | 0x1L;
}


class VNode;  // so that ENode can use it

// ENode structure
class ENode{
  public:
	  int val; // data
	  atomic<VNode*>pointv; // pointer to its vertex
	  atomic<ENode*>enext; // pointer to the next ENode
};


// VNode structure
class VNode{
	public:
    int val; // data
    atomic<VNode*>vnext; // pointer to the next VNode
    atomic<ENode*>enext; // pointer to the EHead
};

// Vertex's Report Structure
class VertexReport{
  public:
    ENode *enode;
    VNode *vnode; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
    int action;                              //1-insert 2-Delete 3-Block
    VertexReport * nextReport;
};

// Edge's Report Structure
class EdgeReport{
  public:
    ENode *enode;
    VNode *source; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
    int action;                              //1-insert 2-Delete 3-Block
    EdgeReport *nextReport;
};


// Structure for Collected graph's Edge Node 
class Snap_Enode{
  public:
    Snap_Enode *enext;
    ENode *enode;
    Snap_Enode(ENode *arg1, Snap_Enode *arg2)
    {
        this->enext = arg2; // arg2 will be a common sentinel E_Node
        this->enode = arg1;
    }
};

// the common sentinel snap ENode
Snap_Enode *end_snap_Enode = new Snap_Enode(NULL, NULL); 

// Structure for Collected graph's Vertex Node 
class Snap_Vnode{ 
  public:
    VNode *vnode;
    Snap_Vnode* vnext;
    Snap_Enode *ehead;   //head of edge linked list

    Snap_Vnode(VNode *arg1, Snap_Vnode *arg2){
        this->vnode = arg1;
        this->vnext = arg2;
        Snap_Enode *start_snap_Enode = new Snap_Enode(vnode->enext, end_snap_Enode);
        this->ehead = start_snap_Enode;
    }
};

// the common sentinel snap VNode
Snap_Vnode *end_snap_Vnode = new Snap_Vnode(NULL, NULL); 

//class GRAPH;
class graphList
{
 public:
  VNode *Head, *Tail;

  graphList()
  {
    Head = new VNode();
    Tail = new VNode();
    this->Head->val = INT_MIN;
    this->Tail->val = INT_MAX;
    this->Head->enext = NULL;
    this->Tail->enext = NULL;
    this->Head->vnext = Tail;
    this->Tail->vnext = NULL;
  }

  // creation of new ENode
  ENode* createE(int key){
    ENode * newe = new ENode();
    newe ->val = key;
    newe ->pointv.store(NULL);
    newe ->enext.store(NULL);
    return newe;
  }

  // creation of new VNode
  VNode* createV(int key, int n){
    ENode *EHead = createE(INT_MIN); //create Edge Head
    ENode *ETail = createE(INT_MAX); // create Edge Tail
    EHead ->enext.store(ETail); 
    VNode * newv = new VNode();
    newv ->val = key;
    newv ->vnext.store(NULL);
    newv ->enext.store(EHead);
    EHead ->pointv.store(newv);
    ETail ->pointv.store(newv); // # check 1(old2019 had TAIL)
  return newv;
  }

  // will update later
  void ReportInsertVertex(VNode* node)
  {        
    //SC = (dereference) PSC
    //if (SC.IsActive())
              // if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
              //     addReport(Report(newNode, INSERt),tid)
  }

  // will update later
  void ReportDeleteVertex(VNode* node)
  {        
    //SC = (dereference) PSC
    //if (SC.IsActive())
              // if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
              //     addReport(Report(newNode, INSERt),tid)
  }


  // Find pred and curr for VNode(key)     
  void locateVPlus(VNode *startV, VNode ** n1, VNode ** n2, int key){
    VNode *succv, *currv, *predv;
    retry:
    while(true){
      predv = startV;
      currv = predv->vnext.load();
      while(true){
      succv = currv->vnext.load();
      while(currv->vnext.load() != NULL && is_marked_ref((long) succv) && currv->val < key ){ 
        ReportDeleteVertex(currv);
        if(!predv->vnext.compare_exchange_strong(currv, (VNode *)get_unmarked_ref((long)succv), memory_order_seq_cst))
          goto retry;
        currv = (VNode *)get_unmarked_ref((long)succv);
        succv = currv->vnext.load(); 
      }
      if(currv->val >= key){
        (*n1) = predv;
        (*n2) = currv;
        return;
      }
      predv = currv;
      currv = succv;
      }
    }  
  } 


  // add a new vertex in the vertex-list
  bool AddVertex(int key, int n)
  {
    VNode *predv, *currv;
    while(true)
    {
      locateVPlus(Head, &predv, &currv, key); // find the location, <pred, curr>
      if(currv->val == key)
      {
        ReportInsertVertex(currv);
        return false; // key already present
      }       
      else{
        VNode *newv = createV(key, n); // create a new vertex node
        newv->vnext.store(currv);  
        if(atomic_compare_exchange_strong(&predv->vnext, &currv, newv)) {// added in the vertex-list
            return true;
        }
      } 
    }
  }

  // Deletes the vertex from the vertex-list
  bool RemoveVertex(int key){
    VNode *predv, *currv, *succv;
    while(true){
      locateVPlus(Head, &predv, &currv, key);
      if(currv->val != key)
        return false; // key is not present

      succv = currv->vnext.load(); 
      if(!is_marked_ref((long) succv)){
        if(atomic_compare_exchange_strong(&currv->vnext, &succv, (VNode*)get_marked_ref((long)succv))) // logical deletion
        { 
          ReportDeleteVertex(currv);
          if(atomic_compare_exchange_strong(&predv->vnext, &currv, succv)) // physical deletion
            break;
        }
      }	
    } 
    return true;
  }

  // Contains VNode   
  bool ContainsV(int key){
    VNode *currv = Head;
    while(currv->vnext.load() && currv->val < key){
      currv =  (VNode*)get_unmarked_ref((long)currv->vnext.load());
    }
    VNode *succv = currv->vnext.load();
	  if((currv->vnext.load()) && currv->val == key && !is_marked_ref((long) succv)){
	    ReportInsertVertex(currv);
      return true;
	  }   
	  else {
      if(is_marked_ref((long)succv))
        ReportDeleteVertex(currv);
	    return false;
    }   
  }

  
      
};

int main()
{
  //abc

  return 0;
}