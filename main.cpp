#include <iostream>
#include <float.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <vector>
#include <ctime> // std::time
#include <random>
#include <algorithm>
#include <iterator>
#include <math.h>
#include <time.h>
#include <fstream>
#include <iomanip>
#include <sys/time.h>
#include <atomic>
#include <list>
#include <queue>
#include <stack>
#include <atomic>
using namespace std;

ofstream coutt("getpath.txt");
// freopen("error.txt", "w", stderr );

inline int is_marked_ref(long i)
{
  return (int)(i & 0x1L);
}

inline long unset_mark(long i)
{
  i &= ~0x1L;
  return i;
}

inline long set_mark(long i)
{
  i |= 0x1L;
  return i;
}

inline long get_unmarked_ref(long w)
{
  return w & ~0x1L;
}

inline long get_marked_ref(long w)
{
  return w | 0x1L;
}

class VNode; // so that ENode can use it

// ENode structure
class ENode
{
public:
  int val;                // data
  atomic<VNode *> pointv; // pointer to its vertex
  atomic<ENode *> enext;  // pointer to the next ENode
};

// VNode structure
class VNode
{
public:
  int val;               // data
  atomic<VNode *> vnext; // pointer to the next VNode
  atomic<ENode *> enext; // pointer to the EHead
};

// Vertex's Report Structure
class VertexReport
{
public:
  ENode *enode;
  VNode *vnode; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
  int action;   // 1-insert 2-Delete 3-Block
  VertexReport *nextReport;
};

// Edge's Report Structure
class EdgeReport
{
public:
  ENode *enode;
  VNode *source; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
  int action;    // 1-insert 2-Delete 3-Block
  EdgeReport *nextReport;
};

// Structure for Collected graph's Edge Node
class Snap_Enode
{
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
class Snap_Vnode
{
public:
  VNode *vnode;
  Snap_Vnode *vnext;
  Snap_Enode *ehead; // head of edge linked list

  Snap_Vnode(VNode *arg1, Snap_Vnode *arg2)
  {
    this->vnode = arg1;
    this->vnext = arg2;
    Snap_Enode *start_snap_Enode = new Snap_Enode(vnode->enext, end_snap_Enode);
    this->ehead = start_snap_Enode;
  }
};

// the common sentinel snap VNode
Snap_Vnode *end_snap_Vnode = new Snap_Vnode(NULL, NULL);

// class GRAPH;
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
  ENode *createE(int key)
  {
    ENode *newe = new ENode();
    newe->val = key;
    newe->pointv.store(NULL);
    newe->enext.store(NULL);
    return newe;
  }

  // creation of new VNode
  VNode *createV(int key, int n)
  {
    ENode *EHead = createE(INT_MIN); // create Edge Head
    ENode *ETail = createE(INT_MAX); // create Edge Tail
    EHead->enext.store(ETail);
    VNode *newv = new VNode();
    newv->val = key;
    newv->vnext.store(NULL);
    newv->enext.store(EHead);
    EHead->pointv.store(newv);
    ETail->pointv.store(newv); // # check 1(old2019 had TAIL)
    return newv;
  }

  // will update later
  void ReportInsertVertex(VNode *node)
  {
    // SC = (dereference) PSC
    // if (SC.IsActive())
    //  if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
    //      addReport(Report(newNode, INSERt),tid)
  }

  // will update later
  void ReportDeleteVertex(VNode *node)
  {
    // SC = (dereference) PSC
    // if (SC.IsActive())
    //  if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
    //      addReport(Report(newNode, INSERt),tid)
  }

  // will update later
  void ReportInsertEdge(ENode *node)
  {
    // SC = (dereference) PSC
    // if (SC.IsActive())
    //  if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
    //      addReport(Report(newNode, INSERt),tid)
  }

  // will update later
  void ReportDeleteEdge(ENode *node)
  {
    // SC = (dereference) PSC
    // if (SC.IsActive())
    //  if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
    //      addReport(Report(newNode, INSERt),tid)
  }

  // (locV)Find pred and curr for VNode(key)
  // ********************DOUBTS
  //                      - isn't the first while loop in infinte loop if we reach end of list before
  //                         key becomes bigger?
  void locateV(VNode *startV, VNode **n1, VNode **n2, int key)
  {
    VNode *succv, *currv, *predv;
    retry:
    while (true)
    {
      predv = startV;
      currv = predv->vnext.load();
      while (true)
      {
        succv = currv->vnext.load();
        while (currv->vnext.load() != NULL && is_marked_ref((long)succv) && currv->val < key)
        {
          ReportDeleteVertex(currv);      // format for report not yet decided
          if (!atomic_compare_exchange_strong(&predv->vnext, &currv, (VNode *)get_unmarked_ref((long)succv)))
            goto retry;
          currv = (VNode *)get_unmarked_ref((long)succv);
          succv = currv->vnext.load();
        }
        if (currv->val >= key)
        {
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
    while (true)
    {
      locateV(Head, &predv, &currv, key); // find the location, <pred, curr>
      if (currv->val == key)
      {
        ReportInsertVertex(currv);          // format for report not yet decided
        return false; // key already present
      }
      else
      {
        VNode *newv = createV(key, n); // create a new vertex node
        newv->vnext.store(currv);
        if (atomic_compare_exchange_strong(&predv->vnext, &currv, newv))
        { // added in the vertex-list
          ReportInsertVertex(newv);         // format for report not yet decided
          return true;
        }
      }
    }
  }

  // Deletes the vertex from the vertex-list
  bool RemoveVertex(int key)
  {
    VNode *predv, *currv, *succv;
    while (true)
    {
      locateV(Head, &predv, &currv, key);
      if (currv->val != key)
        return false; // key is not present

      succv = currv->vnext.load();
      if (!is_marked_ref((long)succv))
      {
        if (atomic_compare_exchange_strong(&currv->vnext, &succv, (VNode *)get_marked_ref((long)succv))) // logical deletion
        {
          ReportDeleteVertex(currv);            // format for report not yet decided
          if (atomic_compare_exchange_strong(&predv->vnext, &currv, succv)) // physical deletion
            break;
        }
      }
    }
    return true;
  }

  // ContainsV+ : Used to verify whether both the vertices are present
  // ********************DOUBTS
  //                      - in old 2019, the first condition in the first two 'if condns' seems wrong
  //                      - check the first 2 if condns, i am assuming that locateV sends NULL also 
  bool ConVPlus(VNode ** n1, VNode ** n2, int key1, int key2)
  {
    VNode *curr1, *pred1, *curr2, *pred2;
    if(key1 < key2)
    {
      locateV(Head, &pred1, &curr1, key1); //first look for key1 
      if((!curr1) || curr1->val != key1)
        return false; // key1 is not present in the vertex-list
        
      locateV(curr1, &pred2, &curr2, key2); // looking for key2 only if key1 present
      if((!curr2) || curr2->val != key2)
        return false; // key2 is not present in the vertex-list
    }
    else
    {
      locateV(Head, &pred2, &curr2, key2); //first look for key2 
      if((!curr2) || curr2->val != key2)
	      return false; // key2 is not present in the vertex-list
	                
	    locateV(curr2, &pred1, &curr1, key1); // looking for key1 only if key2 present
      if((!curr1) || curr1->val != key1)
	      return false; // key1 is not present in the vertex-list

    }
     (*n1) = curr1; 
     (*n2) = curr2; 
    return true;    
  }


  // (locC)Find pred and curr for VNode(key), used for contains edge     
  void locateC(VNode *startV, VNode ** n1, VNode ** n2, int key)
  {
       VNode  *currv, *predv;
        predv = startV;
        currv = startV->vnext.load();
        while(currv && currv->val < key)
        {
          predv = currv;
          currv = (VNode*)get_unmarked_ref((long)currv->vnext.load());
        }
     
          (*n1) = predv;
          (*n2) = currv;
          return ;
        
  } 

  // ContainsC+ : Does the same thing as conVPlus, except the fact that it uses LocC which doesn't help like LocE       
  bool ConCPlus(VNode ** n1, VNode ** n2, int key1, int key2)
  {
    VNode *curr1, *pred1, *curr2, *pred2;
    if(key1 < key2)
    {
      locateC(Head, &pred1, &curr1, key1); //first look for key1 
      if((!curr1) || curr1->val != key1)
	      return false; // key1 is not present in the vertex-list
	        
	    locateC(curr1, &pred2, &curr2, key2); // looking for key2 only if key1 present
      if((!curr2) || curr2->val != key2)
	      return false; // key2 is not present in the vertex-list
    }
    else
    {
      locateC(Head, &pred2, &curr2, key2); //first look for key2 
      if((!curr2) || curr2->val != key2)
	      return false; // key2 is not present in the vertex-list
	                
	    locateC(curr2, &pred1, &curr1, key1); // looking for key1 only if key2 present
      if((!curr1) || curr1->val != key1)
	      return false; // key1 is not present in the vertex-list
    }
    (*n1) = curr1; 
    (*n2) = curr2; 
    return true;    
  }

  // Contains VNode
  bool ContainsV(int key)
  {
    VNode *currv = Head;
    while (currv->vnext.load() && currv->val < key)
    {
      currv = (VNode *)get_unmarked_ref((long)currv->vnext.load());
    }
    VNode *succv = currv->vnext.load();
    if ((currv->vnext.load()) && currv->val == key && !is_marked_ref((long)succv))
    {
      ReportInsertVertex(currv);          // format for report not yet decided
      return true;
    }
    else
    {
      if (is_marked_ref((long)succv))
        ReportDeleteVertex(currv);        // format for report not yet decided
      return false;
    }
  }

  //Contains ENode       
  // returns 1 if vertex not present, 2 if edge already present and 3 if vertex/edge not present
  // ********************DOUBTS
  //                      - in old 2019, in the first while loop their condn is diff and wrong maybe
  int ContainsE(int key1, int key2)
  {
    ENode *curre, *prede;
    VNode *u,*v;
    bool flag = ConCPlus(&u, &v, key1, key2);
    
    if(flag == false){             
      return 1;   // either of the vertex is not present
    }   
    
    curre = u->enext.load(); 
    
    while(curre && curre->val < key2)
    {
      curre =  (ENode*)get_unmarked_ref((long)curre->enext.load());
    }
	  if((curre) && curre->val == key2 && !is_marked_ref((long) curre->enext.load()) 
      && !is_marked_ref((long) u->vnext.load()) && !is_marked_ref((long) v->vnext.load()))
    {
      ReportInsertEdge(curre);        // format for report not yet decided
      return 2;
	  }
	  else 
    {
	    if(is_marked_ref((long)u))
      {
        ReportDeleteVertex(u);    // format for report not yet decided
      }
      else if(is_marked_ref((long)v))
      {
        ReportDeleteVertex(v);    // format for report not yet decided
      }
      else if(is_marked_ref((long)curre->enext.load()))
      {
        ReportDeleteEdge(curre);    // format for report not yet decided
      }
      return 3;
    }   
  }    

  // Deletes an edge from the edge-list if present
  // returns 1 if vertex not present, 2 if edge not present and 3 if edge removed
  // ********************DOUBTS
  //                      - diff in logic old 2019 vs pratik, last triple 'if' 
  int RemoveE(int key1, int key2)
  {
    ENode* prede, *curre, *succe;
    VNode *u,*v;
    bool flag = ConVPlus(&u, &v, key1, key2);
    if(flag == false){             
      return 1; // either of the vertex is not present
    }   
               
    while(true)
    {
      if(is_marked_ref((long) u->vnext.load()))
      {
        ReportDeleteVertex(u);    // format for report not yet decided
        return 1;
      }
      else if(is_marked_ref((long) v->vnext.load()))
      {
        ReportDeleteVertex(v);    // format for report not yet decided
        return 1;
      }
      locateE(&u, &prede, &curre, key2);
      if(curre->val != key2){
        ReportDeleteEdge(curre);    // format for report not yet decided
        return 2; // edge not present
      } 
      succe = curre->enext.load();
	    if(!is_marked_ref((long) succe))
      {
	      if(atomic_compare_exchange_strong(&curre->enext, &succe, (ENode*)get_marked_ref((long)succe))) //  logical deletion
		    { 
          ReportDeleteEdge(curre);
		      if(!atomic_compare_exchange_strong(&prede->enext, &curre, succe)) // physical deletion
			      break;  
        }
	    }
	  }
	  return 3;      
  } 


  // (locE) Find pred and curr for ENode(key) in the edge-list 
  // ********************DOUBTS
  //                      - Why two REPORTDELETE inside the third while loop
  void locateE(VNode **source_of_edge, ENode **n1, ENode **n2, int key)
  {
    ENode *succe, *curre, *prede;
    VNode *tv;
    retry: 
    while(true)
    {
      prede = (*source_of_edge)->enext.load();
      curre = prede->enext.load();
      while(true)
      {
        succe = curre->enext.load();
        tv = curre->pointv.load();
        /*helping: delete one or more enodes whose vertex was marked*/
        retry2: 
        // checking whether the destination vertex is marked (the next edge shouldn't be marked) 
        while(tv && tv->vnext.load() && curre->enext.load() != NULL && 
              is_marked_ref((long)tv->vnext.load()) && !is_marked_ref((long) succe) && curre->val < key )
        { 
          ReportDeleteEdge(curre);        // format for report not yet decided
          if(!atomic_compare_exchange_strong(&curre->enext, &succe, (ENode*)get_marked_ref((long)succe)))
            goto retry;
          ReportDeleteEdge(curre);        // format for report not yet decided
          if(!atomic_compare_exchange_strong(&prede->enext, &curre, succe))
            goto retry;
          curre = (ENode*)get_unmarked_ref((long)succe);
          succe = curre->enext.load();
          tv = curre->pointv.load();
        } 
        
        /*helping: delete one or more enodes which are marked*/
        while(curre->enext.load() != NULL && is_marked_ref((long) succe) && 
              !is_marked_ref((long)tv->vnext.load()) &&  curre->val < key )
        { 
          if(!atomic_compare_exchange_strong(&prede->enext, &curre, (ENode*)get_unmarked_ref((long)succe)))
          goto retry;
          curre = (ENode*)get_unmarked_ref((long)succe);
          succe = curre->enext.load(); 
          tv = curre->pointv.load();
        }

        if(tv && tv->vnext.load() && is_marked_ref((long) tv->vnext.load()) 
          && curre->enext.load() != NULL && curre->val < key)
          goto retry2;
        if(curre->val >= key)
        {
          (*n1) = prede;
          (*n2) = curre;
          return;
        }
        prede = curre;
        curre =(ENode*)get_unmarked_ref((long)succe);
      }
    }  
  } 



  // add a new edge in the edge-list
  // returns 1 if vertex not present, 2 if edge already present and 3 if edge added
  int AddEdge(int key1, int key2)
  {
    ENode *prede, *curre;
    VNode *u, *v;
    bool flag = ConVPlus(&u, &v, key1, key2);

    if (flag == false)
    {
      return 1; // either of the vertex is not present
    }

    while (true)
    {
      if (is_marked_ref((long)u->vnext.load()))
      {
        ReportDeleteVertex(u);        // format for report not yet decided
        return 1; // either of the vertex is not present
      }
      else if(is_marked_ref((long)v->vnext.load()))
      {
        ReportDeleteVertex(v);        // format for report not yet decided
        return 1; // either of the vertex is not present
      }
      
      locateE(&u, &prede, &curre, key2);
      if(curre->val == key2)
      {
        ReportInsertEdge(prede);  // format for report not yet decided
        return 2; // edge already present
      }       
      ENode *newe = createE(key2);// create a new edge node
      newe->enext.store(curre);  // connect newe->next to curr
      newe->pointv.store(v); // points to its vertex
      if(atomic_compare_exchange_strong(&prede->enext, &curre, newe))  // insertion
      {  
        ReportInsertEdge(newe);  // format for report not yet decided
        return 3;
      }
    } // End of while
  }



};

int main()
{
  // abc

  return 0;
}