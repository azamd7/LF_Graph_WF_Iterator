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


// ENode structure
class ENode{
  public:
	  int val; // data
	  atomic<struct VNode *>pointv; // pointer to its vertex
	  atomic<struct ENode *>enext; // pointer to the next ENode
};


// VNode structure
class VNode{
	public:
    int val; // data
    atomic<struct VNode *>vnext; // pointer to the next VNode
    atomic<struct ENode *>enext; // pointer to the EHead
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



int main()
{


  return 0;
}