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

#include <random>

#include <thread>

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
    void locateV(Vnode * startV, Vnode ** n1, Vnode ** n2, int key,int tid, fstream *logfile) {
        Vnode * succv, * currv, * predv;
        retry:
            while (true) {
                predv = startV;
                currv = predv -> vnext.load();
                while (true) {
                    succv = currv -> vnext.load();
                    while (currv != end_Vnode  && is_marked_ref((long) succv) && currv -> val < key) {
                        reportVertex(currv , tid , 1, logfile);// 
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
    bool AddVertex(int key, int tid, fstream *logfile) { //Note : removed int v
        Vnode * predv, * currv;
        while (true) {
            locateV(head, & predv, & currv, key, tid, logfile); // find the location, <pred, curr>

            
            if (currv -> val == key) {
                reportVertex(currv , tid , 2, logfile); // 
                
                return false; // key already present
            } else {
                Vnode * newv = createV(key, currv); // create a new vertex node
                
                if (atomic_compare_exchange_strong( & predv -> vnext, & currv, newv)) { // added in the vertex-list
                    reportVertex(newv , tid , 2, logfile);// 
                    
                    return true;
                }
            }
        }
    }

    // Deletes the vertex from the vertex-list
    bool RemoveVertex(int key, int tid, fstream *logfile) {
        Vnode * predv, * currv, * succv;
        while (true) {
            locateV(head, & predv, & currv, key, tid, logfile);
            if (currv -> val != key)
                return false; // key is not present

            succv = currv -> vnext.load();
            if (!is_marked_ref((long) succv)) {
                if (atomic_compare_exchange_strong( & currv -> vnext, & succv, (Vnode * ) get_marked_ref((long) succv))) // logical deletion
                {
                    reportVertex(currv , tid , 1, logfile);// 
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
    bool ConVPlus(Vnode ** n1, Vnode ** n2, int key1, int key2, int tid, fstream *logfile) {
        Vnode * curr1, * pred1, * curr2, * pred2;
        if (key1 < key2) {
            locateV(head, & pred1, & curr1, key1, tid, logfile); //first look for key1 
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list

            locateV(curr1, & pred2, & curr2, key2, tid, logfile); // looking for key2 only if key1 present
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list
        } else {
            locateV(head, & pred2, & curr2, key2, tid, logfile); //first look for key2 
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list

            locateV(curr2, & pred1, & curr1, key1, tid, logfile); // looking for key1 only if key2 present
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
    bool ContainsV(int key, int tid, fstream *logfile) {
        Vnode * currv = head;
        while (currv -> vnext.load() != end_Vnode && currv -> val < key) {
            currv = (Vnode * ) get_unmarked_ref((long) currv -> vnext.load());
        }
        Vnode * succv = currv -> vnext.load();
        if ( currv -> val == key && !is_marked_ref((long) succv)) {
            reportVertex(currv , tid , 2, logfile);// 
            return true;
        } else {
            if (is_marked_ref((long) succv))
                reportVertex(currv , tid , 1, logfile);// 
            return false;
        }
    }

    //Contains Enode       
    // returns 1 if vertex not present, 2 if edge already present and 3 if vertex/edge not present
    // ********************DOUBTS
    //                      - in old 2019, in the first while loop their condn is diff and wrong maybe
    int ContainsE(int key1, int key2, int tid, fstream *logfile) {
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
            reportEdge(curre ,u , tid, 2 ,logfile);// 
            return 2;
        } else {
            if (is_marked_ref((long) u)) {
                reportVertex(u , tid , 1,logfile);// 
            } else if (is_marked_ref((long) v)) {
                reportVertex(v, tid , 1, logfile);// 
            } else if (is_marked_ref((long) curre -> enext.load())) {
                reportEdge(curre , u , tid , 1,logfile);// 
            }
            return 3;
        }
    }

    // Deletes an edge from the edge-list if present
    // returns 1 if vertex not present, 2 if edge not present and 3 if edge removed
    // ********************DOUBTS
    //                      - diff in logic old 2019 vs pratik, last triple 'if' 
    int RemoveE(int key1, int key2, int tid, fstream * logfile) {
        Enode * prede, * curre, * succe;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2, tid, logfile);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid , 1, logfile);// 
                return 1;
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid, 1, logfile);// 
                return 1;
            }
            locateE( & u, & prede, & curre, key2, tid, logfile);
            
            if (curre -> val != key2) {
                reportEdge(curre , u , tid , 1, logfile);// 
                return 2; // edge not present
            }
            succe = curre -> enext.load();
            if (!is_marked_ref((long) succe)) {
                if (atomic_compare_exchange_strong( & curre -> enext, & succe, (Enode * ) get_marked_ref((long) succe))) //  logical deletion
                {
                    reportEdge(curre , u, tid , 1, logfile);
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
    void locateE(Vnode ** source_of_edge, Enode ** n1, Enode ** n2, int key, int tid, fstream *logfile) {
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
                            reportEdge(curre , *source_of_edge , tid , 1, logfile);
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
    int AddEdge(int key1, int key2, int tid, fstream *logfile) {
        Enode * prede, * curre;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2,tid, logfile);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        //cout << key1 <<endl;
        //cout << key2 << endl;
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid, 1, logfile);// 
                return 1; // either of the vertex is not present
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid , 1, logfile);// 
                return 1; // either of the vertex is not present
            }
            
            locateE( & u, & prede, & curre, key2, tid, logfile);
             
            if (curre -> val == key2) {
                reportEdge(curre , u , tid , 2 , logfile);// 
                return 2; // edge already present
            }
            Enode * newe = createE(key2, v , curre); // create a new edge node
            
            if (atomic_compare_exchange_strong( & prede -> enext, & curre, newe)) // insertion
            {
                
                reportEdge(newe , u , tid , 2, logfile);// 
                return 3;
            }
        } // End of while
    }

};







void print_graph(fstream *logfile , Vnode * graph_headv){
    (*logfile) << "Graph ----------" << endl;
    Vnode * vnode = graph_headv->vnext;
    while(vnode != end_Vnode){
        string val = to_string(vnode->val);
        bool is_marked = is_marked_ref((long)vnode->vnext.load());
        if(is_marked){
            val = "!" + val;
        }
        (*logfile) << val ;

        Enode *enode = vnode->ehead.load()->enext;
        while(enode != end_Enode){
            string e_val = to_string(enode->val);
            bool e_is_marked = is_marked_ref((long)enode->enext.load());
            if (e_is_marked){
                e_val = "!" + e_val;
            }
            e_val = " -> " + e_val ;
            (*logfile) << e_val ;
            enode = enode -> enext;
            
        }
        (*logfile) << endl;
        (*logfile) << "|" <<endl;
        vnode = vnode->vnext;

    }
    (*logfile) << "Tail" << endl;
    (*logfile) << "Graph(End)-------" << endl;
}
/**
 * @brief paramteter that are to be passed on to the threads
 * 
 */
struct thread_args{
    GraphList *graph ;
    fstream * logfile ;
    int thread_num;
    bool debug ;  
    int max_nodes;
    bool * continue_exec; 
    int max_threads;
    
};

/**
 * @brief 
 * 
 * prob_arr will denote prob of different operations
 * 0->Add vertex
 * 1->Remove vertex
 * 2->Add edge
 * 3->Delete edge
 * 4->snapshot
 * 
 * 
 * @param t_args 
 * @return ** void* 
 */
void *thread_funct(void * t_args){

    GraphList *graph = ((struct thread_args *)t_args)->graph;
    fstream * logfile = ((struct thread_args *)t_args)->logfile;
    bool debug = ((struct thread_args *)t_args)->debug;
    int thread_num = ((struct thread_args *)t_args)->thread_num;
    int max_nodes = ((struct thread_args *)t_args)->max_nodes;
    int max_threads = ((struct thread_args *)t_args)->max_threads;
    //int prob_arr[4] = ((struct thread_args *)t_args)->prob_arr;
    bool *continue_exec = ((struct thread_args *)t_args)->continue_exec;
    
    string logFileName = "../output/logfileParr" + to_string(thread_num) +".txt";
    cout << logFileName << endl;
    fstream logfile_th;
    logfile_th.open(logFileName,ios::out);
    while(*continue_exec){
        int op_index = rand() % 5;
        op_index = 4;
        //logfile_th << "op_index" << op_index << endl;
        switch(op_index) {
        case 0://add vertex
            {
                int rand_node_id = rand() % max_nodes;    
                logfile_th << " thread id : " << thread_num << "Add vertex  : " << rand_node_id << endl;
                graph->AddVertex(rand_node_id,thread_num,&logfile_th );
            }
            break;
        case 1:
            // delete vertex
            {
                int rand_node_id = rand() % max_nodes;    
                logfile_th << " thread id : " << thread_num << "Delete vertex : " << rand_node_id << endl;
                graph->RemoveVertex(rand_node_id,thread_num,&logfile_th);
            }
            break;
        case 2:
            // add edge
            {
                int rand_source = rand() % max_nodes; 
                int rand_dest = rand() % max_nodes;
                while(rand_dest == rand_source){
                    rand_dest = rand() % max_nodes;
                }   
                logfile_th << " thread id : " << thread_num << "Add edge : " << rand_source << " " << rand_dest << endl;
                graph->AddEdge(rand_source , rand_dest , thread_num,&logfile_th);
            }
            break;
        case 3:
            //delete edge
            {   int rand_source = rand() % max_nodes; 
                int rand_dest = rand() % max_nodes;
                while(rand_dest == rand_source){
                    rand_dest = rand() % max_nodes;
                }   
                logfile_th << " thread id : " << thread_num << " Delete edge : " << rand_source << " " << rand_dest  << endl;
                graph->RemoveE(rand_source , rand_dest , thread_num,&logfile_th);
            }
            break;
        case 4:
            //snapshot
            {
                logfile_th << " thread id : " << thread_num << " Collecting snapshot"  << endl;
                //print_graph(&logfile_th , graph->head);
                SnapCollector * sc =  takeSnapshot(graph->head , thread_num, &logfile_th);
                //sc->print_snap_graph(&logfile_th);
            }
            break;
        }
    }

    return nullptr;

}


// class GRAPH;


int main() {
    // abc
    string logFileName = "../output/logfileParr.txt";
    //will be used in script
    string numberOfThreadsStr = "2";
    int   num_of_threads = stoi(numberOfThreadsStr);
    bool debug = false;


    fstream logfile;
    logfile.open(logFileName,ios::out);

    GraphList * graph = new GraphList();
    
    graph->AddVertex(1 , 1, &logfile);
    
    graph->AddVertex(5 , 1, &logfile);
    graph->AddVertex(4 , 1, &logfile);
    

    graph->AddEdge(1, 4, 1, &logfile);
    graph->AddEdge(1, 4, 1, &logfile);
    
    graph->AddEdge(5, 4, 1, &logfile);
    graph->RemoveVertex(4 ,1, &logfile);
    graph->AddEdge(1, 5, 1, &logfile);
    graph->RemoveE(5, 4, 1, &logfile);
    graph->AddVertex(4, 1, &logfile);
    graph->AddEdge(1, 6, 1, &logfile);
    graph->AddEdge(4, 5, 1, &logfile);
    //print_graph(&logfile , graph->head);



    //SnapCollector * sc =  takeSnapshot(graph->head , 2);
    
    //sc->print_snap_graph(&logfile);
    //printf(graph->ContainsE(5,4,1) != 2? "False\n" : "True\n");
    struct thread_args t_args[num_of_threads];
    pthread_t threads[num_of_threads];
    std::cout << "ASdas" << endl;
    bool *continue_exec = new bool(true);
    for( int i=0;i < num_of_threads ;i++){

        

        t_args[i].continue_exec = continue_exec;
        
        t_args[i].logfile = &logfile;
        t_args[i].graph = graph;
        t_args[i].debug = debug;
        t_args[i].thread_num = i;
        t_args[i].max_nodes = 10;
        t_args[i].max_threads = num_of_threads;
        std::cout << "Thread num : " << t_args[i].thread_num << endl;
        pthread_create(&threads[i], NULL, thread_funct, &t_args[i]);
        
    }
    sleep(10);
    std::cout << *continue_exec <<endl;
    *continue_exec = false;
    std::cout << *continue_exec <<endl;
    for(int i=0 ; i< num_of_threads;i++){
        pthread_join(threads[i], NULL);
    }

    return 0;
}