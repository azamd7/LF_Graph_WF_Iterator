 /*
 * File:main-10-90.cpp
 *  
 *
 * Author(s):
 *   Bapi Chatterjee <bapchatt@in.ibm.com>
 *   Sathya Peri <sathya_p@iith.ac.in>
 *   Muktikanta Sa   <cs15resch11012@iith.ac.in>
 *   Nandini Singhal <cs15mtech01004@iith.ac.in>
 *   
 * Description:
 *    implementation of a Coarse Graph GetPath
 * Copyright (c) 2018.
 * last Updated: 31/07/2018
 *
*/
#include"lfGraph_getPath.cpp"

using namespace std;

 //ofstream couttt("graphinput.txt");
  typedef struct 
{
    int     secs;
    int     usecs;
}TIME_DIFF;

TIME_DIFF * my_difftime (struct timeval * start, struct timeval * end)
{
	TIME_DIFF * diff = (TIME_DIFF *) malloc ( sizeof (TIME_DIFF) );
 
	if (start->tv_sec == end->tv_sec) 
	{
        	diff->secs = 0;
        	diff->usecs = end->tv_usec - start->tv_usec;
    	}
   	else 
	{
        	diff->usecs = 1000000 - start->tv_usec;
        	diff->secs = end->tv_sec - (start->tv_sec + 1);
        	diff->usecs += end->tv_usec;
        	if (diff->usecs >= 1000000) 
		{
        	    diff->usecs -= 1000000;
	            diff->secs += 1;
	        }
	}
        return diff;
}


atomic<long> vertexID;
double seconds;
struct timeval tv1, tv2;
TIME_DIFF * difference;



typedef struct infothread{
  long tid;
  slist G;
  int threadnum;
  double * ops;
  vector<double> * dist_prob;
  double * max_times;
  double * avg_times;
}tinfo;


atomic<bool> cont_exec;

void* pthread_call(void* t)
{
        tinfo *ti=(tinfo*)t;
        long Tid = ti->tid;
        slist G1=ti->G;
        int threadnum = ti->threadnum;
        double * avg_times = ti->avg_times;
        double * max_times = ti->max_times;
        
        double * ops = ti->ops;
        vector<double> * dist_prob = ti->dist_prob;
    vector<double> tts;//list of time taken for snapshot
	int u, v;;
    random_device rd;
    mt19937 gen(rd());
    discrete_distribution<> d(dist_prob->begin() , dist_prob->end());
	while(cont_exec)
	{
		gettimeofday(&tv2,NULL);
		difference = my_difftime (&tv1, &tv2);

		if(difference->secs >= seconds)
			break;
        
     
        int op= d(gen);
		//int op = rand()%7;	
        switch(op){
            case 0:
            {
                l:	v = rand() % (vertexID) + 1;		
                if(v == 0)
                    goto l;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.AddV(v,NTHREADS);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                break;
            }
            case 1:
            {
                l2:	v = rand() % (vertexID)+1;		
                if(v == 0)
                    goto l2;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.RemoveV(v);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                break;			
            }

                
            case 2:
            {
            l4:	u = (rand() % (vertexID))+1;	
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)	
                    goto l4;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.AddE(u,v); 
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                break;			
            }
               
            case 3:
            {
            l3:	u = (rand() % (vertexID))+1;		
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)	
                    goto l3;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.RemoveE(u,v); 
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                break;	
            }
            
           case 4:
            {
            l5:	u = (rand() % (vertexID))+1;		
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)		
                    goto l5;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.ContainsE(u,v); 
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                break;			
            }
            case 5:
            {
            
            l6:	v = rand() % (vertexID) + 1;	
                if(v == 0)
                    goto l6;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.ContainsV(v);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                
                break;
            }
            case 6:
            {
                //if(threadnum == 0){
                    chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                    snap_vlist * vhead1 = G1.snapshot();
                    snap_vlist * vhead2 = G1.snapshot();
                    while(!G1.compare_snapshot(vhead1 , vhead2) ){
                        vhead1 = vhead2;
                        vhead2 = G1.snapshot();
                    
                    }
                    G1.get_diameter( threadnum,vhead2);
                    //if(cont_exec)
                    //    ops[threadnum]++;
                    chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                    double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;
                    tts.push_back(timeTaken);
                    if (max_times[threadnum] < timeTaken){
                        max_times[threadnum] = timeTaken;
                    }
                //}
            }
        }
        
	}
    //calculate average of all the timetaken
    if(tts.size() > 0){
        double total_tts = 0;
        for(double tt : tts){
            total_tts += tt;
        }
        avg_times[threadnum] = total_tts / tts.size();

    }

    return nullptr; 
    
    		
}




int main(int argc, char*argv[])	
{
	slist sg;
	vertexID.store(1);
    int num_of_threads = 13;
    int test_duration = 10;
    int initial_vertices = (int)pow(10 , 3);
    int initial_edges = 2 * (int)pow(10, 3);
    vector<double> dist_prob = {1,1,1,1,1,1,1};


	if(argc > 1){
        num_of_threads = stoi(argv[2]) ;
        test_duration = stoi(argv[3]);
        initial_vertices = stoi(argv[4]);
        initial_edges = stoi(argv[5]);
        if(argc > 8){
            //read dist probabilities
            for(int i = 0;i< 7 ; i++){
                dist_prob[i] = stoi(argv[7+i]);
            }
            
        }
    }
	NTHREADS = num_of_threads;
	seconds = test_duration;		
	vertexID.store(10*initial_vertices);	
    //    sg.init();
	//sg.initGraph(initial_vertices, initial_edges, NTHREADS);
    sg.init();
	//sg.initGraph(initial_vertices, initial_edges, NTHREADS);
    sg.initGraphFromFile(argv[6],NTHREADS,1); // load graph from file
    //cout << "graph created" << endl;

    pthread_t thr[NTHREADS];
	tinfo attr[NTHREADS];

   	int dig,temp; 
	double duration = 0.0;
        gettimeofday(&tv1,NULL);
    double *ops = new double[NTHREADS];
    cont_exec.store(true);
	//cout << "timer started . . ." << endl;

    double * max_times = new double[NTHREADS];
    double * avg_times = new double[NTHREADS];

	for (int i=0;i < NTHREADS;i++)
    {
        attr[i].tid = i;
        attr[i].G = sg;
        attr[i].threadnum = i;
        attr[i].ops = ops;
        attr[i].dist_prob = &dist_prob;
        attr[i].max_times = max_times;
        attr[i].avg_times = avg_times;
        pthread_create(&thr[i], nullptr, pthread_call, &attr[i]);
    }
    sleep(seconds);
    cont_exec.store(false);
    //cout << "cont is false"  <<endl;

	for (int i = 0; i < NTHREADS; i++)
    {
		pthread_join(thr[i], NULL);
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


    cout << avg_time << fixed << endl;
    cout << max_time << fixed << endl;

	return 0;
}           

void print_graph(slist sg, fstream *logfile){
    (*logfile) << "Graph ----------" << endl;
    vlist_t *vnode = sg.Head->vnext;
    while(vnode->val != INT_MAX){
        string val = to_string(vnode->val);
        bool is_marked = is_marked_ref((long)vnode->vnext.load());
        if(is_marked){
            val = "!" + val;
        }
        (*logfile) << val ;

        elist_t *enode = vnode->enext.load()->enext;
        while(enode->val != INT_MAX){
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

void print_snap_graph(snap_vlist *vhead ,fstream * logfile ){
    (*logfile) << "Snap Graph ----------" << endl;
    snap_vlist * vsnap = vhead->vnext;

    while(vsnap != nullptr ){
        string val = to_string(vsnap->pointv->val);
        (*logfile) << val ;
        
        snap_elist *enode = vsnap->enext->enext;
        
        while(enode != nullptr ){
            string e_val = to_string(enode->pointe->val);
            e_val = " -> " + e_val ;
            (*logfile) << e_val ;
            enode = enode->enext;
        }
        (*logfile) << endl;
        (*logfile) << "|" <<endl;
        vsnap = vsnap->vnext;
        
    }
    
    (*logfile) << "Tail" << endl;
    (*logfile) << "Snap Graph(End)-------" << endl;
}
