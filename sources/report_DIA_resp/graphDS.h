#include <iostream>

#include <float.h>

#include <stdint.h>

#include <stdio.h>

#include <atomic>

#include <stdlib.h>

#include <limits.h>

using namespace std;


class Vnode; // so that Enode can use it
// Enode structure
class Enode {
    public:
        int val; // data
        Vnode * v_dest; // pointer to its vertex
        atomic<Enode *> enext; // pointer to the next Enode

        Enode(int val , Vnode * v_dest ,Enode *enext ){
            this->val = val;
            this->v_dest = v_dest;
            this->enext = enext;
        }
};


// Vnode structure
class Vnode {
    public:
        int val; // data
        atomic< Vnode *> vnext; // pointer to the next Vnode
        atomic < Enode * > ehead; // pointer to the EHead

        Vnode(int val, Vnode *vnext, Enode *ehead){
            this->val = val;
            this->vnext = vnext;
            this->ehead = ehead;
        }
};


//these values will be used to point the end of the list (vertex or edge list)
Enode * end_Enode = new Enode(INT_MAX, NULL, NULL);
Vnode * end_Vnode = new Vnode(INT_MAX, NULL, NULL);


