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

#include <map>

#include <random>

#include <initializer_list>

#include <unistd.h>

#include <set>




using namespace std;

void print_graph(fstream *logfile , Vnode * graph_headv);

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
    void locateV(Vnode * startV, Vnode ** n1, Vnode ** n2, int key,int tid, fstream *logfile ,bool debug) {
        Vnode * succv, * currv, * predv;
        retry:
            while (true) {
                predv = startV;
                currv = (Vnode *)get_unmarked_ref((long)predv -> vnext.load());
                while (true) {
                    //print_graph(logfile , this->head);
                    succv = currv -> vnext.load();
                    while (currv != end_Vnode  && is_marked_ref((long) succv) && currv -> val <= key) {
                        if(debug)
                            (*logfile) << "Vertex is marked : " << currv->val << "(" << currv << ")" << endl; 
                        reportVertex(currv , tid , 1, logfile,debug);// 
                        //physical deletion of vertex
                        if (!atomic_compare_exchange_strong( & predv -> vnext, & currv, (Vnode * ) get_unmarked_ref((long) succv)))
                        {
                            goto retry; 
                        }
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
    bool AddVertex(int key, int tid, fstream *logfile, bool debug) { //Note : removed int v
        Vnode * predv, * currv;
        while (true) {
            locateV(head, & predv, & currv, key, tid, logfile,debug); // find the location, <pred, curr>

            
            if (currv -> val == key) {
                if(debug)
                    (*logfile) << "Vertex already present with key : " << key << endl; 
                reportVertex(currv , tid , 2, logfile,debug); // 
                
                return false; // key already present
            } else {
                Vnode * newv = createV(key, currv); // create a new vertex node
                
                if (atomic_compare_exchange_strong( & predv -> vnext, & currv, newv)) { // added in the vertex-list
                    if(debug)
                        (*logfile) << "Vertex added with key : " << key <<"("<< newv <<")"<< endl;
                    reportVertex(newv , tid , 2, logfile ,debug);// 
                    return true;
                }
                delete newv;
            }
        }
    }

    // Deletes the vertex from the vertex-list
    bool RemoveVertex(int key, int tid, fstream *logfile,bool debug) {
        Vnode * predv, * currv, * succv;
        while (true) {
            locateV(head, & predv, & currv, key, tid, logfile,debug);
            if (currv -> val != key ){
                if(debug)
                    (*logfile) << "Vertex not found with key : " << key << endl;
                return false; // key is not present
            }

            succv = currv -> vnext.load();
            if (!is_marked_ref((long) succv)) {
                if (atomic_compare_exchange_strong( & currv -> vnext, & succv, (Vnode * ) get_marked_ref((long) succv))) // logical deletion
                {
                    reportVertex(currv , tid , 1, logfile, debug);// 
                    if (atomic_compare_exchange_strong( & predv -> vnext, & currv, succv)) // physical deletion
                    {   
                        if(debug)
                            (*logfile) << "physically deleted " << currv->val << "("<< currv <<")"<< endl; 
                        break;
                    }
                }
            }
        }
        return true;
    }

    // ContainsV+ : Used to verify whether both the vertices are present
    // ********************DOUBTS
    //                      - in old 2019, the first condition in the first two 'if condns' seems wrong
    //                      - check the first 2 if condns, i am assuming that locateV sends NULL also 
    bool ConVPlus(Vnode ** n1, Vnode ** n2, int key1, int key2, int tid, fstream *logfile,bool debug) {
        Vnode * curr1, * pred1, * curr2, * pred2;
        if (key1 < key2) {
            locateV(head, & pred1, & curr1, key1, tid, logfile,debug); //first look for key1 
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list

            locateV(head, & pred2, & curr2, key2, tid, logfile,debug); // looking for key2 only if key1 present
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list
        } else {
            locateV(head, & pred2, & curr2, key2, tid, logfile,debug); //first look for key2 
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list

            locateV(head, & pred1, & curr1, key1, tid, logfile,debug); // looking for key1 only if key2 present
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
        currv = (Vnode * ) get_unmarked_ref((long) startV-> vnext.load());;
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

            locateC(head, & pred2, & curr2, key2); // looking for key2 only if key1 present
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list
        } else {
            locateC(head, & pred2, & curr2, key2); //first look for key2 
            if ((!curr2) || curr2 -> val != key2)
                return false; // key2 is not present in the vertex-list

            locateC(head, & pred1, & curr1, key1); // looking for key1 only if key2 present
            if ((!curr1) || curr1 -> val != key1)
                return false; // key1 is not present in the vertex-list
        }
        ( * n1) = curr1;
        ( * n2) = curr2;
        return true;
    }

    // Contains Vnode
    bool ContainsV(int key, int tid, fstream *logfile ,bool debug) {
        Vnode * currv = head;
        while (currv -> vnext.load() != end_Vnode && currv -> val < key) {
            currv = (Vnode * ) get_unmarked_ref((long) currv -> vnext.load());
        }
        Vnode * succv = currv -> vnext.load();
        if ( currv -> val == key && !is_marked_ref((long) succv)) {
            if(debug)
                (*logfile)<< "Vertex already present : " << currv->val <<"(" << currv << ")" << endl;
            reportVertex(currv , tid , 2, logfile, debug);// 
            return true;
        } else {
            if (is_marked_ref((long) succv)){
                if(debug)
                    (*logfile)<< "Vertex marked : " << currv->val <<"(" << currv << ")" << endl;   
                reportVertex(currv , tid , 1, logfile, debug);// 

            }
            return false;
        }
    }

    //Contains Enode       
    // returns 1 if vertex not present, 2 if edge already present and 3 if vertex/edge not present

    int ContainsE(int key1, int key2, int tid, fstream *logfile, bool debug) {
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
            reportEdge(curre ,u , tid, 2 ,logfile,debug);// 
            return 2;
        } else {
            if (is_marked_ref((long) u)) {
                if(debug)
                    (*logfile) << "Source vertex : " << u->val << "(" << u << ")" <<" marked" << endl;
                reportVertex(u , tid , 1,logfile, debug);// 
            } else if (is_marked_ref((long) v)) {
                if(debug)
                    (*logfile) << "Destination vertex : " << v->val << "(" << v << ")" <<" marked" << endl;
                reportVertex(v, tid , 1, logfile, debug);// 
            } else if (is_marked_ref((long) curre -> enext.load())) {
                if(debug)
                    (*logfile) << "Edge marked : " << u->val << " " << curre->val << "(" << curre << ")" <<" marked" << endl;
                reportEdge(curre , u , tid , 1,logfile,debug);// 
            }
            return 3;
        }
    }

    // Deletes an edge from the edge-list if present
    // returns 1 if vertex not present, 2 if edge not present and 3 if edge removed

    int RemoveE(int key1, int key2, int tid, fstream * logfile, bool debug) {
        Enode * prede, * curre, * succe;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2, tid, logfile,debug);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid , 1, logfile, debug);// 
                return 1;
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid, 1, logfile, debug);// 
                return 1;
            }
            locateE( & u, & prede, & curre, key2, tid, logfile, debug);
            
            if (curre -> val != key2) {
                (*logfile) << "Edge not found  : " << key1 << " " << key2 << endl;
                return 2; // edge not present
            }
            succe = curre -> enext.load();
            if (!is_marked_ref((long) succe)) {
                if (atomic_compare_exchange_strong( & curre -> enext, & succe, (Enode * ) get_marked_ref((long) succe))) //  logical deletion
                {
                    if(debug)
                        (*logfile) << "Logically deleted " << succe->val << "(" << succe << ")" << endl;
                    reportEdge(curre , u, tid , 1, logfile, debug);
                    if (!atomic_compare_exchange_strong( & prede -> enext, & curre, succe)) // physical deletion
                        break;
                    else if(debug)
                        (*logfile) << "Physically deleted " << succe->val << "(" << succe << ")" << endl;
                    
                }
            }
        }
        return 3;
    }

    // (locE) Find pred and curr for Enode(key) in the edge-list 
    // ********************DOUBTS
    //                      - Why two REPORTDELETE inside the third while loop
    void locateE(Vnode ** source_of_edge, Enode ** n1, Enode ** n2, int key, int tid, fstream *logfile, bool debug) {
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
                            (is_marked_ref((long) tv -> vnext.load()) || is_marked_ref((long) succe)) && curre -> val <= key) {
                            reportEdge(curre , *source_of_edge , tid , 1, logfile, debug);
                            //marking curr enode

                            if (!is_marked_ref((long) succe) and !atomic_compare_exchange_strong( & curre -> enext, & succe, (Enode * ) get_marked_ref((long) succe)))
                                goto retry;
                            
                            //physical deletion of enode if already marked
                            //Note : remove goto retry if physical deletion fails
                            if(!atomic_compare_exchange_strong( & prede -> enext, & curre, (Enode * ) get_unmarked_ref((long) succe)))
                                goto retry;
                                
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
    int AddEdge(int key1, int key2, int tid, fstream *logfile, bool debug) {
        Enode * prede, * curre;
        Vnode * u, * v;
        bool flag = ConVPlus( & u, & v, key1, key2,tid, logfile,debug);
        if (flag == false) {
            return 1; // either of the vertex is not present
        }
        //cout << key1 <<endl;
        //cout << key2 << endl;
        while (true) {
            if (is_marked_ref((long) u -> vnext.load())) {
                reportVertex(u , tid, 1, logfile,debug);// 
                return 1; // either of the vertex is not present
            } else if (is_marked_ref((long) v -> vnext.load())) {
                reportVertex(v , tid , 1, logfile, debug);// 
                return 1; // either of the vertex is not present
            }
            
            locateE( & u, & prede, & curre, key2, tid, logfile,debug);
             
            if (curre -> val == key2) {
                if(debug)
                    (*logfile) << "Edge : "<< key1 <<" " << key2  << " already present" << endl;
                reportEdge(curre , u , tid , 2 , logfile,debug);// 
                return 2; // edge already present
            }
            Enode * newe = createE(key2, v , curre); // create a new edge node
            
            if (atomic_compare_exchange_strong( & prede -> enext, & curre, newe)) // insertion
            {
                if(debug)
                    (*logfile) << "New Edge added  : " << key1 <<" " << key2 <<"(" << newe << ")"<< endl;
                reportEdge(newe , u , tid , 2, logfile,debug);// 
                return 3;
            }
            delete newe;
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

atomic<bool> continue_exec;
/**
 * @brief paramteter that are to be passed on to the threads
 * 
 */
struct thread_args{
    GraphList *graph ;
    string  logfilename ;
    int thread_num;
    bool debug ;  
    int max_nodes;
    int max_threads;
    double * max_times;
    double * avg_times;
    int sleep_time;
    vector<double> * dist_prob;
    
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
    string logFileName = ((struct thread_args *)t_args)->logfilename;
    bool debug = ((struct thread_args *)t_args)->debug;
    int thread_num = ((struct thread_args *)t_args)->thread_num;
    int max_nodes = ((struct thread_args *)t_args)->max_nodes;
    int max_threads = ((struct thread_args *)t_args)->max_threads;
    //int prob_arr[4] = ((struct thread_args *)t_args)->prob_arr;
    double * avg_times = ((struct thread_args *)t_args)->avg_times;
    double * max_times = ((struct thread_args *)t_args)->max_times;
    vector<double> * dist_prob = ((struct thread_args *)t_args)->dist_prob;
    int sleep_time = ((struct thread_args *)t_args)->sleep_time;

    vector<double> tts;//list of time taken for snapshot
    
    fstream logfile_th;
    if(debug){ 
        logFileName = logFileName + to_string(thread_num) +".txt";
        logfile_th.open(logFileName,ios::out);
    }
    random_device rd;
    mt19937 gen(rd());
    discrete_distribution<> d(dist_prob->begin() , dist_prob->end());
    while(continue_exec){
        random_device rd;
        mt19937 gen(rd());
        discrete_distribution<> d(dist_prob->begin() , dist_prob->end());
     
        int op_index = d(gen);
        switch(op_index) {
        case 0://add vertex
            {
                int rand_node_id = rand() % max_nodes; 
                if(debug) 
                    logfile_th << " thread id : " << thread_num << "Add vertex  : " << rand_node_id << endl;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                graph->AddVertex(rand_node_id,thread_num,&logfile_th,debug );
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }
            }
            break;
        case 1:
            // delete vertex
            {
                int rand_node_id = rand() % max_nodes;    
                if(debug)
                    logfile_th << " thread id : " << thread_num << "Delete vertex : " << rand_node_id << endl;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                graph->RemoveVertex(rand_node_id,thread_num,&logfile_th, debug);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }
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
                if(debug)   
                    logfile_th << " thread id : " << thread_num << "Add edge : " << rand_source << " " << rand_dest << endl;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                graph->AddEdge(rand_source , rand_dest , thread_num,&logfile_th,debug);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }
            }
            break;
        case 3:
            //delete edge
            {   int rand_source = rand() % max_nodes; 
                int rand_dest = rand() % max_nodes;
                while(rand_dest == rand_source){
                    rand_dest = rand() % max_nodes;
                }
                if(debug)
                    logfile_th << " thread id : " << thread_num << " Delete edge : " << rand_source << " " << rand_dest  << endl;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                graph->RemoveE(rand_source , rand_dest , thread_num,&logfile_th,debug);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }
            }
            break;
        case 4:
            //contains edge
            {   int rand_source = rand() % max_nodes; 
                int rand_dest = rand() % max_nodes;
                while(rand_dest == rand_source){
                    rand_dest = rand() % max_nodes;
                }
                if(debug)
                    logfile_th << " thread id : " << thread_num << " Contians Edge : " << rand_source << " " << rand_dest  << endl;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                graph->ContainsE(rand_source , rand_dest , thread_num,&logfile_th,debug);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }

            }
            break;
        case 5:
        //contains vertex
        {   int node_id = rand() % max_nodes; 
            
            if(debug)
                logfile_th << " thread id : " << thread_num << " Contains vertex : " << node_id  << endl;
            chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
            graph->ContainsV(node_id , thread_num,&logfile_th,debug);
            chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
            double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
            if(continue_exec){
                tts.push_back(timeTaken);
                if (max_times[thread_num] < timeTaken){
                    max_times[thread_num] = timeTaken;
                }
            }
        }
        break;
        case 6:
            //snapshot
            {   if(debug)
                    logfile_th << " thread id : " << thread_num << " Collecting snapshot"  << endl;
                //print_graph(&logfile_th , graph->head);
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                SnapCollector * sc =  takeSnapshot(graph->head , max_threads, &logfile_th,debug, sleep_time);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                if(continue_exec){
                    tts.push_back(timeTaken);
                    if (max_times[thread_num] < timeTaken){
                        max_times[thread_num] = timeTaken;
                    }
                }
                if(debug){
                    logfile_th << "TimeTaken : " << timeTaken << endl; 
                    sc->print_snap_graph(&logfile_th);
                }
            }
            break;
        
       
        }
    }
    //calculate average of all the timetaken
    if(tts.size() > 0){
        double total_tts = 0;
        for(double tt : tts){
            total_tts += tt;
        }
        avg_times[thread_num] = total_tts / tts.size();

    }
    if(debug)
        logfile_th.close();

    return nullptr;

}

GraphList * create_graph(int init_vertices , int init_edges){
    GraphList * graph = new GraphList();
    int max_vertices = 2 * init_vertices;

    //add vertices
    while(init_vertices--){
        int nodeid = rand() % max_vertices;
        bool ret = graph->AddVertex(nodeid , -1 , nullptr , false);
        while(ret){
            ret = graph->AddVertex(nodeid , -1 , nullptr , false);
        }
    }

    //add edges
    while(init_edges--){
        int source = rand() % max_vertices;
        int dest = rand() % max_vertices;
        while(dest ==  source){
            dest = rand() % max_vertices;
        }
        int ret = graph->AddEdge(source ,dest , -1 , nullptr , false);
        while(ret != 3){
            source = rand() % max_vertices;
            dest = rand() % max_vertices;
            while(dest ==  source){
                dest = rand() % max_vertices;
            }
            ret = graph->AddEdge(source ,dest , -1 , nullptr , false);
        }
    }
    return graph;
}

GraphList * create_graph_from_file(string file, int NT, int tid){
    GraphList * graph = new GraphList();
    ifstream cinn(file);
    long n,m;
    int u, v, v_;
    cinn>>n>>m;
  
    int i,j, e=0;

    for(i=1;i<=2*n;i++){
        graph->AddVertex(i , -1 , nullptr , false);
    }
    for(j=1; j<=m; j = j+1){
	    cinn>>u>>v;
        graph->AddEdge(u,v , -1 , nullptr , false);
	    e++;
      }   
  //cout<<"Edge:"<<e<<endl;
} 


// class GRAPH;


int main(int argc, char** argv) {
    // abc
    string logFileName = "../log/logfileParr";
    string opFileName = logFileName + ".txt";
    //will be used in script
    int num_of_threads = 13;
    int test_duration = 10;
    int initial_vertices = (int)pow(10 , 3);
    int initial_edges = 2 * (int)pow(10, 3);
    int sleep_threads = 2;
    int sleep_time = 30;
 
    bool debug = false;
    vector<double> dist_prob = {1,1,1,1,1,1,1};

    

    if(argc > 1){
		logFileName = argv[1] ;
        num_of_threads = stoi(argv[2]) ;
        test_duration = stoi(argv[3]);
        initial_vertices = stoi(argv[4]);
        initial_edges = stoi(argv[5]);
        //sleep threads and sleep time
        sleep_threads = stoi(argv[7]);
        sleep_time = stoi(argv[8]);

        if(argc > 10){
            //read dist probabilities
            for(int i = 0;i< 7 ; i++){
                dist_prob[i] = stoi(argv[9+i]);
            }
            
        }
        
        
        

        if(argc == 17){
            if(argv[16] == "debug"){
            	debug = true;
       		}
        }
	}

    fstream opfile;
    opfile.open(opFileName,ios::out);
    
    opfile << logFileName << endl;
    opfile << num_of_threads << endl;
    opfile << test_duration << endl;
    opfile << initial_vertices << endl;
    opfile << initial_edges <<endl;
    opfile << dist_prob[0] << " " << dist_prob[1] << " " << dist_prob[2] << " " << dist_prob[3] << " " << dist_prob[4]  << endl;
    opfile << debug << endl;

    GraphList * graph = create_graph(initial_vertices ,initial_edges);
    



    //SnapCollector * sc =  takeSnapshot(graph->head , 2);
    
    //sc->print_snap_graph(&logfile);
    //printf(graph->ContainsE(5,4,1) != 2? "False\n" : "True\n");
    struct thread_args t_args[num_of_threads];
    pthread_t threads[num_of_threads];
    
    double * max_times = new double[num_of_threads];
    double * avg_times = new double[num_of_threads];
    continue_exec.store(true);

    set<int> sleep_thread_nums;
    srand(time(0));
    while(sleep_thread_nums.size() == sleep_threads){
        int thread_num = rand() % num_of_threads;
        sleep_thread_nums.insert(thread_num);
    }

    //cout << "End snap Enode " << end_snap_Enode << endl;
    //cout << "Marked End snap Enode " << (Snap_Enode *)get_marked_ref((long) end_snap_Enode) << endl;
    //cout << "End snap Vnode " << end_snap_Vnode << endl;
    //cout << "Marked End snap Vnode " << (Snap_Vnode*) get_marked_ref((long)end_snap_Vnode) << endl;
    for( int i=0;i < num_of_threads ;i++){
        int thrd_sleep_time = 0;
        if(sleep_thread_nums.find(i) != sleep_thread_nums.end()){
            thrd_sleep_time = sleep_time;
            if(debug)
                opfile << "Thread num :: " << i << " will be slow" << endl;
        }
        
        t_args[i].logfilename = logFileName;
        t_args[i].graph = graph;
        t_args[i].debug = debug;
        t_args[i].thread_num = i;
        t_args[i].max_nodes = 10 * initial_vertices;
        t_args[i].max_threads = num_of_threads;
        t_args[i].debug = debug;
        t_args[i].max_times = max_times;
        t_args[i].avg_times = avg_times;
        t_args[i].dist_prob = &dist_prob;
        t_args[i].sleep_time = thrd_sleep_time;
        pthread_create(&threads[i], NULL, thread_funct, &t_args[i]);
        
    }
    sleep(test_duration);
    continue_exec.store(false);
    for(int i=0 ; i< num_of_threads;i++){
        pthread_join(threads[i], NULL);
    }

    double max_time = 0;
    double avg_time = 0;

    for( int i = 0;i < num_of_threads ; i++){
        //check max
        if(max_time < max_times[i]){
            max_time = max_times[i];
        }
        avg_time += avg_times[i];
    }

    avg_time = avg_time / num_of_threads;

    opfile << "Avg Time : " << avg_time << endl;
    opfile << "Max Time : " << max_time << endl;

    cout << avg_time << fixed << endl;
    cout << max_time << fixed << endl;

    opfile.close();

    return 0;
}