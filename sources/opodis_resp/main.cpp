/*
 * File:
 *  
 *
 * Author(s):
 *     Muktikanta Sa   <cs15resch11012@iith.ac.in>
 *   
 *   
 * Description:
 *   
 *
*/
#include <bits/stdc++.h> 
#include <chrono>
#include <unistd.h>
#include <sys/resource.h>
#include <thread>
#include <pthread.h>
#include"BFS-PGCn.cpp"
#define Warmup 5
#define BILLION  1000000000.0;
time_t start1,end1;
atomic<long> vertexID;
double seconds;
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
enum type {APP=0, ADDE, REME, ADDV, REMV };
struct timeval tv1, tv2, tv3;
TIME_DIFF * difference1, *difference2;

atomic <int> ops;
atomic <int> opsUpdate;
atomic <int> opsLookup;
atomic <int> opsapp;
int ***dynamicops; // total number of ops, # rows same as # threads
double ***updateTime; // Update time with # of other operations
typedef struct infothread{
  int tid; //thread ID
  int core_id; // core id
  int cols; // column size
  int wp; // warm up value
  double * ops; //operations completed by each thread
  vector<double> * dist_prob;//distribution prob
  Hash G;
  double * max_times;
  double * avg_times;
}tinfo;

long n,m;
int  nops;
int itr = 0;


void loadOps(string file, int nocols){
int i,j,k;
dynamicops = (int ***)malloc(NTHREADS*sizeof(int**));
  for (i = 0; i< NTHREADS; i++) {
     dynamicops[i] = (int **) malloc(nocols*sizeof(int *));
        for (j = 0; j < nocols; j++) {
            dynamicops[i][j] = (int *)malloc(4*sizeof(int));
       }
    }
 ifstream cinn(file);   
 long n,m;
 int u, v, v_, wt;
 int nop;
 cinn>>nop;
 int optype;
 for (i = 0; i< NTHREADS; i++) {
   for (j = 0; j < nocols; j++) {
        //for (k = 0; j < 4; k++) {
            cinn>> optype;
            switch(optype){
                   
               
                case ADDE : 
                      cinn>>u>>v>>wt;  
                      dynamicops[i][j][0] = optype;
                      dynamicops[i][j][1] = u;
                      dynamicops[i][j][2] = v;
                      dynamicops[i][j][3] = wt;
                      
                    break;
              
                case REME : 
                      cinn>>u>>v;  
                      dynamicops[i][j][0] = optype;
                      dynamicops[i][j][1] = u;
                      dynamicops[i][j][2] = v;
                      dynamicops[i][j][3] = -1;
                break;                              
                
                case APP : 
                      cinn>>u;  
                      dynamicops[i][j][0] = optype;
                      dynamicops[i][j][1] = u;
                      dynamicops[i][j][2] = -1;
                      dynamicops[i][j][3] = -1;
                break; 
                case ADDV : 
                      cinn>>u;  
                      dynamicops[i][j][0] = optype;
                      dynamicops[i][j][1] = u;
                      dynamicops[i][j][2] = -1;
                      dynamicops[i][j][3] = -1;
                break;   
                case REMV : 
                      cinn>>u;  
                      dynamicops[i][j][0] = optype;
                      dynamicops[i][j][1] = u;
                      dynamicops[i][j][2] = -1;
                      dynamicops[i][j][3] = -1;
                break;                  
                default: break;                                          
                }          
            
       }
    }
   

}

//void* pthread_call(void* t)
//{
//        tinfo *ti=(tinfo*)t;
//        int tid = ti->tid;
//        int core_id = ti->core_id;
//        const pthread_t pid = pthread_self();
//        cpu_set_t cpuset;
//        CPU_ZERO(&cpuset);
//        CPU_SET(core_id, &cpuset);
//        int rc = pthread_setaffinity_np(pid,sizeof(cpu_set_t), &cpuset);
//        Hash G1=ti->G;
//        int u, v;
//        int optype;
//        int cols = ti->cols;
//        int wp = ti->wp;
//        struct timespec starttime, endtime;
//        for(int i=wp; i<cols; i++){
//            optype = dynamicops[tid][i][0];

//            switch(optype){

//                case APP : 
//                      u = dynamicops[tid][i][1];             
//                      G1.GetBFS(u, tid);
//                      opsapp++;
//                      ops++;
//                      break;

//                case ADDE : 
//                      u = dynamicops[tid][i][1];              
//                      v = dynamicops[tid][i][2];     
//                      G1.PutE(u,v, tid);
//                      opsUpdate++;
//                      ops++;
//                      break;
                                 
//                 case REME : 
//                      u = dynamicops[tid][i][1];              
//                      v = dynamicops[tid][i][2];  
//                      G1.RemoveE(u,v, tid);
//                      opsUpdate++;
//                      ops++;
//                      break;
//                 case ADDV : 
//                      u = dynamicops[tid][i][1];             
//                      G1.insert(u, NTHREADS, tid);
//                      opsapp++;
//                      ops++;
//                      break;
//                 case REMV : 
//                      u = dynamicops[tid][i][1];             
//                      G1.remove(u, NTHREADS, tid);
//                      opsapp++;
//                      ops++;
//                      break;     
//                 default: break;     
//        }
//      }                
      
//}


TIME_DIFF * difference;
atomic<bool> cont_exec;
void* pthread_call(void* t)
{
    tinfo *ti=(tinfo*)t;
    long Tid = ti->tid;
    Hash G1=ti->G;
    int threadnum = ti->tid;

    //int core_id = ti->core_id;
    //const pthread_t pid = pthread_self();
    //cpu_set_t cpuset;
    //CPU_ZERO(&cpuset);
    //CPU_SET(core_id, &cpuset);
    //int rc = pthread_setaffinity_np(pid,sizeof(cpu_set_t), &cpuset);
    double * ops = ti->ops;
    vector<double> * dist_prob = ti->dist_prob;
    double * avg_times = ti->avg_times;
        double * max_times = ti->max_times;
            vector<double> tts;//list of time taken for snapshot

	int u, v;;
    random_device rd;
    mt19937 gen(rd());
    discrete_distribution<> d(dist_prob->begin() , dist_prob->end());

    fstream logfile_th;
        //string  logFileName = "../../log/09_11_42" + to_string(threadnum) +".txt";
        //logfile_th.open(logFileName,ios::out);

	while(cont_exec)
	{

        int op= d(gen);
		//int op = rand()%7;	
        switch(op){
            case 0:
            {
                l:	v = rand() % (vertexID)+ 1;		
                if(v == 0)
                    goto l;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
               G1.insert(v, NTHREADS, threadnum);
               chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;
            }
            case 1:
            {
                l2:	v = rand() % (vertexID)+1;		
                if(v == 0)
                    goto l2;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.remove(v, NTHREADS, threadnum);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;			
            }

                
            case 2:
            {
            l4:	u = (rand() % (vertexID))+1;	
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)	
                    goto l4;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.PutE(u,v, threadnum);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;			
            }
               
            case 3:
            {
            l3:	u = (rand() % (vertexID))+1;		
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)	
                    goto l3;

                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.RemoveE(u,v, threadnum);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;	
            }
            case 4:
            {
            l5: u = (rand() % (vertexID))+1;
                v = (rand() % (vertexID))+1;
                if(u == v || u == 0 || v == 0)	
                    goto l5;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.GetE(u, v);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;
            }

            case 5:
            {
                v = (rand() % (vertexID))+1;
                chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                G1.contains(v);
                chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                tts.push_back(timeTaken);
                if (max_times[threadnum] < timeTaken){
                    max_times[threadnum] = timeTaken;
                }
                //if(cont_exec)
                //    ops[threadnum]++;
                break;
            }
          
            case 6:
            {
                    //G1.GetBFS(v, threadnum);
                    chrono::high_resolution_clock::time_point startT = chrono::high_resolution_clock::now();
                    SnapGraph *ss1 = new SnapGraph();
                    ss1->collect_ss(G1.Head);
                    SnapGraph *ss2 = new SnapGraph();
                    ss2->collect_ss(G1.Head);
                    while(!compare_ss_collect(ss1, ss2) and cont_exec ){
                        ss1->freeHNode();
                        ss1 = ss2;
                        ss2 = new SnapGraph();
                        ss2->collect_ss(G1.Head); 
                    } 

                    
                    
                    
                    chrono::high_resolution_clock::time_point endT = chrono::high_resolution_clock::now();
                    double timeTaken = chrono::duration_cast<chrono::microseconds>(endT-startT).count() ;

                    tts.push_back(timeTaken);
                    if (max_times[threadnum] < timeTaken){
                        max_times[threadnum] = timeTaken;
                    }
                    
                    ss1->freeHNode();
                    ss2->freeHNode();


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

void* pthread_callwp(void* t)
{
       tinfo *ti=(tinfo*)t;
        int tid = ti->tid;
        int core_id = ti->core_id;
        const pthread_t pid = pthread_self();
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        int rc = pthread_setaffinity_np(pid,sizeof(cpu_set_t), &cpuset);
        Hash G1=ti->G;
        int u, v;
        int optype;
        int cols = ti->wp;
        for(int i=0; i<cols; i++){
            optype = dynamicops[tid][i][0];
            switch(optype){

                case APP : 
                      u = dynamicops[tid][i][1];             
                      G1.GetBFS(u, tid);
                      break;
      
                case ADDE : 
                      u = dynamicops[tid][i][1];              
                      v = dynamicops[tid][i][2];     
                      G1.PutE(u,v, tid);
                      break;
                                 
                 case REME : 
                      u = dynamicops[tid][i][1];              
                      v = dynamicops[tid][i][2];  
                      G1.RemoveE(u,v, tid);
                      break;
                case ADDV : 
                      u = dynamicops[tid][i][1];             
                      G1.insert(u, NTHREADS, tid);
                      break;
                 case REMV : 
                      u = dynamicops[tid][i][1];             
                      G1.remove(u, NTHREADS, tid);
                      break; 
                 default: break;     
        }

      }        
      return nullptr;        
}

int main(int argc, char*argv[]) 
{
        Hash sg;
        vector<double> dist_prob = {1,1,1,1,1,1,1};
        int i;
        if(argc < 3)
        {
                cout << "Enter 3 command line arguments - #threads, init graph and ops file" << endl;
                return 0;
        }
       
       ifstream cingph(argv[6]); // Input graph
 	cingph>>n>>m;
	cingph.close();
	//th = atoi(argv[2]); // threshold value
    th = 10;//threshold value
    NTHREADS = atoi(argv[2]);  // # of threads 
    //nops =   atoi(argv[5]); // # of operation to be perform
    int cols_size = nops/NTHREADS;
    int wp = cols_size *(0.05);// 5% warm up
    sg.initHNode(n);
    //loadOps(argv[4], cols_size);// graph ops file
    
    sg.initGraphFromFile(argv[6],NTHREADS,1); // load graph from file
    pthread_t *thr = new pthread_t[NTHREADS];
    int test_duration = stoi(argv[3]);
    int initial_vertices = stoi(argv[4]);
    vertexID.store(10 * initial_vertices);
    //prob distribution of operations
    for(int i = 0;i< 7 ; i++){
        dist_prob[i] = stoi(argv[7+i]);
    }
    

    // Make threads Joinable for sure.
    pthread_attr_t attr;
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);
    
    ops = 0;
    opsUpdate = 0;
    opsLookup = 0;;
    opsapp = 0;
    gettimeofday(&tv1,NULL);
    double *ops = new double[NTHREADS];
    double * max_times = new double[NTHREADS];
    double * avg_times = new double[NTHREADS];
    cont_exec.store(true);
        for (i=0;i < NTHREADS;i++)
    {
            tinfo *t =(tinfo*) malloc(sizeof(tinfo));
            t->tid = i;
            if(i >= 14 && i<= 27 && NTHREADS == 28)
                t->core_id = i + 14; 
            else
                t->core_id = i; 
            
            t->G = sg;
            t->wp = wp;
            t->cols = cols_size;
            t->dist_prob = &dist_prob;
            t->ops = ops;
            t->max_times = max_times;
        t->avg_times = avg_times;
            pthread_create(&thr[i], &attr, pthread_call, (void*)t);
    }
    sleep(test_duration);
    cont_exec.store(false);

    for (i = 0; i < NTHREADS; i++)
    {
            pthread_join(thr[i], NULL);
    }
    double max_time = 0;
    double avg_time = 0;

    for( int i = 0;i < NTHREADS ; i++){
        //check max
        if(max_time < max_times[i]){
            max_time = max_times[i];
        }
        avg_time += avg_times[i];
    }

    avg_time = avg_time / NTHREADS;


    cout << avg_time << fixed << endl;
    cout << max_time << fixed << endl;

	return 0;
    //   gettimeofday(&tv2,NULL);
          
    //    for (i=0;i < NTHREADS;i++)
    //    {
    //          tinfo *t =(tinfo*) malloc(sizeof(tinfo));
    //            t->tid = i;
    //            if(i >= 14 && i<= 27 && NTHREADS == 28)
    //              t->core_id = i + 14; 
    //            else
    //              t->core_id = i; 
                
    //            t->G = sg;
    //            t->wp = wp;
    //            t->cols = cols_size;
    //            pthread_create(&thr[i], &attr, pthread_call, (void*)t);
    //    }
    //    for (i = 0; i < NTHREADS; i++)
    //    {
    //            pthread_join(thr[i], NULL);
    //    }
    //    gettimeofday(&tv3,NULL);
    //    delete []thr;
    //    difference1 = my_difftime (&tv1, &tv2);
    //    difference2 = my_difftime (&tv2, &tv3);
	//int dig1 = 1;
	//int temp1 = difference1->usecs;
	//while(temp1>=10)
	//{	
	//	dig1++;
	//	temp1 = temp1/10;
	//}
	//temp1 =1;
	//for(i=1;i<=dig1;i++)
	//	temp1 = temp1 * 10;
	//double duration1 = (double) difference1->secs + ((double)difference1->usecs / (double)temp1);
    //    int dig2 = 1;
	//int temp2 = difference2->usecs;
	//while(temp2>=10)
	//{	
	//	dig2++;
	//	temp2 = temp2/10;
	//}
	//temp2 =1;
	//for(i=1;i<=dig2;i++)
	//	temp2 = temp2 * 10;
	//double duration2 = (double) difference2->secs + ((double)difference2->usecs / (double)temp2);
	//cout<<"Number of Threads:"<<NTHREADS<<endl;
    //	cout << "Graph Size V: " << n <<" and E:"<<m<<endl;
    //	cout << "Execution Time : " << duration2 <<" secs."<<endl;

    //double tp = nops / duration2 ; //operations per sec
    //cout << tp << endl;
    //    return 0;
}


