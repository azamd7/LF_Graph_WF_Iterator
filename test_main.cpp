
#include<stdio.h>
#include<iostream>
#include <atomic>

using namespace std;


class VNode; // so that ENode can use it
// ENode structure
class ENode {
    public:
        int val; // data
        atomic<VNode *> pointv; // pointer to its vertex
        atomic<ENode *> enext; // pointer to the next ENode
};

// VNode structure
class VNode {
    public:
        int val; // data
        atomic< int> vnext; // pointer to the next VNode
        atomic < ENode * > enext; // pointer to the EHead
};

int main(){
    ENode *enode = new ENode();
    
    return -1;
}