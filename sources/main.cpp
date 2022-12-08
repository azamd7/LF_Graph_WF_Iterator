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

#include "snapcollector.h"

#include <math.h>

#include <time.h>

#include <fstream>

#include <iomanip>

#include <sys/time.h>

#include <atomic>

#include <list>

#include <queue>

#include <stack>

using namespace std;

class GraphList {
    public:
        Vnode * head;

    GraphList() {
        head = new Vnode(INT_MIN ,end_Vnode ,NULL);
    }

    // creation of new Enode
    Enode * createE(int key , Vnode * v_dest , Enode * enext) {
        Enode * newe = new Enode(key , v_dest, enext);
        return newe;
    }

    // creation of new Vnode
    Vnode * createV(int key , Vnode * vnext) {
        Enode * EHead = createE(INT_MIN, NULL,end_Enode); // create Edge Head
        Vnode * newv = new Vnode(key, vnext , EHead);
        return newv;
    }



    // (locV)Find pred and curr for Vnode(key)
    // ********************DOUBTS
    //                      - isn't the first while loop in infinte loop if we reach end of list before
    //                         key becomes bigger?
    void locateV(Vnode * startV, Vnode ** n1, Vnode ** n2, int key,int tid) {
        Vnode * succv, * currv, * predv;
        retry:
            while (true) {
                predv = startV;
                currv = predv -> vnext.load();
                while (true) {
                    succv = currv -> vnext.load();
                    while (currv != end_Vnode  && is_marked_ref((long) succv) && currv -> val < key) {
                        reportVertex(currv , tid , 1);// 
                        if (!atomic_compare_exchange_strong( & predv -> vnext, & currv, (Vnode * ) get_unmarked_ref((long) succv)))
                            goto retry;
                        currv = (Vnode * ) get_unmarked_ref((long) succv);
                        succv = currv -> vnext.load();
                    }

                    if (currv == end_Vnode || currv -> val >= key ) {
                        ( * n1) = predv;
                        ( * n2) = currv;
                        return;
                    }
                    predv = currv;
                    currv = succv;
                }
            }
    }

    // add a new vertex in the vertex-list
    bool AddVertex(int key, int tid) { //Note : removed int v
        Vnode * predv, * currv;
        while (true) {
            locateV(head, & predv, & currv, key, tid); // find the location, <pred, curr>

            
            if (currv -> val == key) {
                reportVertex(currv , tid , 2); // 
                
                return false; // key already present
            } else {
                Vnode * newv = createV(key, currv); // create a new vertex node
                
                if (atomic_compare_exchange_strong( & predv -> vnext, & currv, newv)) { // added in the vertex-list
                    reportVertex(newv , tid , 2);// 
                    
                    return true;
                }
            }
        }
    }

    // Deletes the vertex from the vertex-list
    bool RemoveVertex(int key, int tid) {
        Vnode * predv, * currv, * succv;
        while (true) {
            locateV(head, & predv, & currv, key, tid);
            if (currv -> val != key)
                return false; // key is not present

            succv = currv -> vnext.load();
            if (!is_marked_ref((long) succv)) {
                if (atomic_compare_exchange_strong( & currv -> vnext, & succv, (Vnode * ) get_marked_ref((long) succv))) // logical deletion
                {
                    reportVertex(currv , tid , 1);// 
                    if (atomic_compare_exchange_strong( & predv -> vnext, & currv, succv)) // physical deletion
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
    bool ConVPlus(Vnode ** n1, Vnode ** n2, int key1, int key2, int tid) {
        Vnode * curr1, * pred1, * curr2, * pred2;
        if (key1 < key2) {
            locateV(head, & pred1, & curr1, key1, tid); //first look for key1 
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list

            locateV(curr1, & pred2, & curr2, key2, tid); // looking for key2 only if key1 present
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list
        } else {
            locateV(head, & pred2, & curr2, key2, tid); //first look for key2 
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list

            locateV(curr2, & pred1, & curr1, key1, tid); // looking for key1 only if key2 present
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list

        }
        ( * n1) = curr1;
        ( * n2) = curr2;
        return true;
    }

    // (locC)Find pred and curr for Vnode(key), used for contains edge     
    void locateC(Vnode * startV, Vnode ** n1, Vnode ** n2, int key) {
        Vnode * currv, * predv;
        predv = startV;
        currv = startV -> vnext.load();
        while (currv != end_Vnode && currv -> val < key) {
            predv = currv;
            currv = (Vnode * ) get_unmarked_ref((long) currv -> vnext.load());
        }

        ( * n1) = predv;
        ( * n2) = currv;
        return;

    }

    // ContainsC+ : Does the same thing as conVPlus, except the fact that it uses LocC which doesn't help like LocE       
    bool ConCPlus(Vnode ** n1, Vnode ** n2, int key1, int key2) {
        Vnode * curr1, * pred1, * curr2, * pred2;
        if (key1 < key2) {
            locateC(head, & pred1, & curr1, key1); //first look for key1 
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list

            locateC(curr1, & pred2, & curr2, key2); // looking for key2 only if key1 present
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list
        } else {
            locateC(head, & pred2, & curr2, key2); //first look for key2 
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list

            locateC(curr2, & pred1, & curr1, key1); // looking for key1 only if key2 present
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list
        }
        ( * n1) = curr1;
        ( * n2) = curr2;
        return true;
    }

    // Contains Vnode
    bool ContainsV(int key, int tid) {
        Vnode * currv = head;
        while (currv -> vnext.load() != end_Vnode && currv -> val < key) {
            currv = (Vnode * ) get_unmarked_ref((long) currv -> vnext.load());
        }
        Vnode * succv = currv -> vnext.load();
        if ( currv -> val == key && !is_marked_ref((long) succv)) {
            reportVertex(currv , tid , 2);// 
            return true;
        } else {
            if (is_marked_ref((long) succv))
                reportVertex(currv , tid , 1);// 
            return false;
        }
    }

    //Contains Enode       
    // returns 1 if vertex not present, 2 if edge already present and 3 if vertex/edge not present
    // ********************DOUBTS
    //                      - in old 2019, in the first while loop their condn is diff and wrong maybe
    int ContainsE(int key1, int key2, int tid) {
        Enode * curre, * prede;
        Vnode * u, * v;
        bool flag = ConCPlus( & u, & v, key1, key2);

        if (flag == false) {
            return 1; // either of the vertex is not present
        }

        curre = u -> ehead.load();

        while (curre != end_Enode && curre -> val < key2) {
            curre = (Enode * ) get_unmarked_ref((long) curre -> enext.load());
        }
        if ((curre) && curre -> val == key2 && !is_marked_ref((long) curre -> enext.load()) &&
            !is_marked_ref((long) u -> vnext.load()) && !is_marked_ref((long) curre->v_dest-> vnext.load())) {
            reportEdge(curre ,u , tid, 2 );// 
            return 2;
        } else {
            if (is_marked_ref((long) u)) {
                reportVertex(u , tid , 1);// 
            } else if (is_marked_ref((long) v)) {
                reportVertex(v, tid , 1);// 
            } else if (is_marked_ref((long) curre -> enext.load())) {
                reportEdge(curre , u , tid , 1);// 
            }
            return 3;
        }
    }

    // Deletes an edge from the edge-list if present
    // returns 1 if vertex not present, 2 if edge not present and 3 if edge removed
    // ********************DOUBTS
    //                      - diff in logic old 2019 vs pratik, last triple 'if' 
    int RemoveE(int key1, int key2, int tid) {
        Enode * prede, * curre, * succe;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2, tid);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid , 1);// 
                return 1;
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid, 1);// 
                return 1;
            }
            locateE( & u, & prede, & curre, key2, tid);
            
            if (curre -> val != key2) {
                reportEdge(curre , u , tid , 1);// 
                return 2; // edge not present
            }
            succe = curre -> enext.load();
            if (!is_marked_ref((long) succe)) {
                if (atomic_compare_exchange_strong( & curre -> enext, & succe, (Enode * ) get_marked_ref((long) succe))) //  logical deletion
                {
                    reportEdge(curre , u, tid , 1);
                    if (!atomic_compare_exchange_strong( & prede -> enext, & curre, succe)) // physical deletion
                        break;
                }
            }
        }
        return 3;
    }

    // (locE) Find pred and curr for Enode(key) in the edge-list 
    // ********************DOUBTS
    //                      - Why two REPORTDELETE inside the third while loop
    void locateE(Vnode ** source_of_edge, Enode ** n1, Enode ** n2, int key, int tid) {
        Enode * succe, * curre, * prede;
        Vnode * tv;
        retry:
            while (true) {

                prede = ( * source_of_edge) -> ehead.load();
                curre = prede -> enext.load();
                
                while (true) {
                    succe = curre -> enext.load();
                    tv = curre->v_dest;
                    /*helping: delete one or more enodes whose vertex was marked*/
                   
                    retry2:
                        // checking whether the destination vertex is marked (the next edge shouldn't be marked) 
                        //Note : Removed "tv" conditions
                        /*helping: delete one or more enodes which are marked*/
                                
                        while ( curre != end_Enode &&
                            (is_marked_ref((long) tv -> vnext.load()) || is_marked_ref((long) succe)) && curre -> val < key) {
                            reportEdge(curre , *source_of_edge , tid , 1);
                            //marking curr enode
                            if (!atomic_compare_exchange_strong( & curre -> enext, & succe, (Enode * ) get_marked_ref((long) succe)))
                                goto retry;
                            //physical deletion of enode if already marked
                            //Note : remove goto retry if physical deletion fails
                            atomic_compare_exchange_strong( & prede -> enext, & curre, succe);
                                
                            curre = (Enode * ) get_unmarked_ref((long) succe);
                            succe = curre -> enext.load();
                            tv = curre -> v_dest;
                        }
                    
                    
                    //Note : Commented below 3 lines : not sure of the use of these
                    //if (is_marked_ref((long) tv -> vnext.load()) &&
                    //    curre != end_Enode && curre -> val < key)
                    //    goto retry2;
                    if (curre == end_Enode || curre -> val >= key) {
                        
                        ( * n1) = prede;
                        ( * n2) = curre;
                        return;
                    }
                    prede = curre;
                    curre = (Enode * ) get_unmarked_ref((long) succe);
                }
            }
    }

    // add a new edge in the edge-list
    // returns 1 if vertex not present, 2 if edge already present and 3 if edge added
    int AddEdge(int key1, int key2, int tid) {
        Enode * prede, * curre;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2,tid);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        //cout << key1 <<endl;
        //cout << key2 << endl;
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid, 1);// 
                return 1; // either of the vertex is not present
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid , 1);// 
                return 1; // either of the vertex is not present
            }
            
            locateE( & u, & prede, & curre, key2, tid);
             
            if (curre -> val == key2) {
                reportEdge(curre , u , tid , 2);// 
                return 2; // edge already present
            }
            Enode * newe = createE(key2, v , curre); // create a new edge node
            
            if (atomic_compare_exchange_strong( & prede -> enext, & curre, newe)) // insertion
            {
                
                reportEdge(newe , u , tid , 2);// 
                return 3;
            }
        } // End of while
    }

};







void print_graph(fstream *logfile , Vnode * graph_headv){
    cout << "Graph ----------" << endl;
    Vnode * vnode = graph_headv->vnext;
    while(vnode != end_Vnode){
        string val = to_string(vnode->val);
        bool is_marked = is_marked_ref((long)vnode->vnext.load());
        if(is_marked){
            val = "!" + val;
        }
        cout << val ;

        Enode *enode = vnode->ehead.load()->enext;
        while(enode != end_Enode){
            string e_val = to_string(enode->val);
            bool e_is_marked = is_marked_ref((long)enode->enext.load());
            if (e_is_marked){
                e_val = "!" + e_val;
            }
            e_val = " -> " + e_val ;
            cout << e_val ;
            enode = enode -> enext;
            
        }
        cout << endl;
        cout << "|" <<endl;
        vnode = vnode->vnext;

    }
    cout << "Tail" << endl;
    cout << "Graph(End)-------" << endl;
}




// class GRAPH;


int main() {
    // abc
    string logFileName = "../output/logfileParr.txt";
    fstream logfile;
    logfile.open(logFileName,ios::out);

    GraphList * graph = new GraphList();
    SnapCollector * sc =  takeSnapshot(graph->head , 2);
    
    graph->AddVertex(1 , 1);
    
    graph->AddVertex(5 , 1);
    graph->AddVertex(4 , 1);
    

    graph->AddEdge(1 , 4 , 1);
    graph->AddEdge(1 , 4 , 1);
    
    graph->AddEdge(5 , 4 , 1);
    graph->RemoveVertex(4 ,1);
    graph->AddEdge(1, 5 ,1);
    graph->RemoveE(5, 4 , 1);
    graph->AddVertex(4 , 1);
    graph->AddEdge(1,6 , 1);
    graph->AddEdge(4,5 , 1);
    
    sc->blockFurtherReports();
    
    sc->reconstructUsingReports();
    
    print_graph(&logfile , graph->head);
    sc->print_snap_graph(&logfile);
    printf(graph->ContainsE(5,4,1) != 2? "False\n" : "True\n");


    return 0;
}