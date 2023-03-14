 /// @brief processing edge reports


 class Snap_Vnode {
    public:
        atomic<Snap_Vnode *> vnext;
        Vnode * vnode;
        Snap_Enode * ehead; // head of edge linked list
        int * visitedArray;// size same as threads // used to indicate whether the node has beeen visited by the given thread
        //this will have value as the source node which was being processed when it was visited last
        atomic<int> edge_status ; //0->edges havent been processed by any thread 
                                    // 1-> being processed 2->completed processing
        atomic<int> iter_edge_status;
        atomic<long> report_index;

        int * dist_from_source;
        int * BC_path_indicator;
        int * path_cnt;//total shortest path from source 
        int * v_path_cnt;//total shortest path containing BC vertex
    //is_reconstruct is true then end enode is marked

 }


 class SnapCollector{
    private:
        atomic<bool> active = {false};
    public :
        //indicates if the snap collect field is currently active
        Snap_Vnode *head_snap_Vnode;//points to head of the collected vertex list
        atomic<Snap_Vnode *> tail_snap_V_ptr;//points to vertex last added too the collected vertex list
        atomic<Snap_Vnode *> tail_snap_E_V_ptr;//points to the vertex currently being iterated during edge iterations

        bool read_edge = false;// boolean value to indicate if that we are going through the edge
        Report **reports;//array of atomic report for each thread
        //vector <Report> delete_vertex_reports; //This will be used to check the while adding edges 
    
        int no_of_threads;
        //atomic<int> threads_accessing = {0} ; //no of threads accesssing the snapcollector
    

        //for reconstruction using report
    
        atomic<vector<VertexReport> *>sorted_vertex_reports_ptr = {nullptr};
        atomic<vector<EdgeReport> *>sorted_edge_reports_ptr = {nullptr};
        atomic<long> vertex_report_index = {0}; //used to store the highest index in sorted vertex reports currently being processed by any thread
        atomic<long> edge_report_index = {0} ;//at

        atomic<bool> reconstruction_completed;

        atomic<bool> iteration_completed;

 }

    edge_reconstruction(){
            vector<EdgeReport> *edge_reports = sorted_edge_reports_ptr.load();
            
            if (edge_reports == nullptr){
                edge_reports = new vector<EdgeReport>();
                for (int i = 0; i < no_of_threads; i++)
                {
                    EdgeReport *curr_head= reports[i]->head_edge_report;
                    curr_head = curr_head->nextReport; // ignore the dummy report
                    while(curr_head)
                    {
                        
                        edge_reports->push_back(*curr_head);
                        curr_head = curr_head->nextReport;
                    }
                }
                 
                sort(edge_reports->begin(), edge_reports->end(), &edge_comparator);
                vector<EdgeReport> * tmp = nullptr;
                if(!atomic_compare_exchange_strong(&sorted_edge_reports_ptr , &tmp , edge_reports)){            
                    delete edge_reports;
                    edge_reports = tmp;//cas fails tmp will have the current value
                }
                
            }
            
            Snap_Vnode * loc_snap_vertex_ptr = head_snap_Vnode->vnext;
            long loc_report_index = 0;
            //ist iteration
            while(is_marked_ref((long)loc_snap_vertex_ptr) and !this->reconstruction_completed){
                int tmp = 0;    
                if(atomic_compare_exchange_strong(&loc_snap_vertex_ptr->edge_status , &tmp , 1)){
                    //edges of this nodes isnt processed by any thread
                    //update the dest_vnode ptr to next of head snap_vnode

                    //fetch the report with source is the cur snap vertex ptr
                    while(edge_reports->at(loc_report_index).source->val < loc_snap_vertex_ptr->vnode->val and loc_report_index < edge_reports->size()){
                        loc_report_index++;
                    }
                    Snap_Enode * loc_snap_edge_ptr = loc_snap_vertex_ptr->ehead->enext;
                    Snap_Enode * loc_snap_dest_vertex = head_snap_Vnode->vnext;
                    if(loc_report_index == edge_reports->size() || edge_reports->at(loc_report_index).source !=loc_snap_vertex_ptr->vnode)
                    {
                        //no report exist for the given source...
                        //store -2 in snap_vnodes edge report to indicate no reports for edges of this snap vnode
                        long tmpInd = -1;
                        atomic_compare_exchange_strong(&loc_snap_vertex_ptr->report_index , &tmpInd , -2);//if fails some other thread
                        //would have done it in its 2nd iteration through the snap vnodes

                    }
                    else
                    {
                        //report found with same source

                        //while edge report increments to next source or end of the report list
                        while(loc_report_index == edge_reports->size() || loc_report_index < edge_reports->at(loc_report_index).source ==loc_snap_vertex_ptr->vnode )
                        {
                            //verify and update all the edges till the current report edge

                            //if snap edges reach end of the list
                            //break 

                            //if delete report
                                //if edge is present delete the edge
                                //else ignore

                                
                            
                            //else if insert report
                                //if edge is present ignore
                                //else verify if the dest_v's snap vnode exists and add the new snap enode

                            //ignore all the insert report belonging to the same edge and source
                            //update the report index for the given node
                        }
                        
                        
                        
                    }
                   //vertify and update the destination pointer for each vertex check if status is 2
                    //while(snapedge is marked)
                   

                    //update the vertex edge status to 1->2
                }

                loc_snap_vertex_ptr = loc_snap_vertex_ptr->vnext;
            }

            //once a thread has iterated all the vnodes
            //2nd iteration
            
            loc_snap_vertex_ptr = head_snap_Vnode->vnext;
            loc_report_index = 0
            
            //while the snap vertex is not marked ie. marked_end_snap_enode and recomstruction completed is false

                //if snap vertex edge status = 1 ie. edges are still not processed completely
                    //Snap_Enode * loc_snap_edge_ptr = loc_snap_vertex_ptr->ehead->enext
                    //Snap_Enode * loc_snap_dest_vertex = head_snap_Vnode->vnext;
                    
                    
                    //if snap vnode report index is still -1 ...not updated by other thread
                        //simillar to the above code....fetch the report corresponding to cuur vnode as source of edge report
                        //if no such report
                            //mark edge report as -2
                        //else
                            //update the edge index of snap vnode with index
                    
                    //if edge report is not -2
                                
                        // report found with same source

                        //while edge report increments to next source or end of the report list

                            //verify and update all the edges till the current report edge

                            //if snap edges reach end of the list
                            //break 

                            //if delete report
                                //if edge is present delete the edge
                                //else ignore

                                
                            
                            //else if insert report
                                //if edge is present ignore
                                //else verify if the dest_v's snap vnode exists and add the new snap enode

                            //ignore all the insert report belonging to the same edge and source
                            //update the report index for the given node
                        
                        //if reached end of edge list of snapvnode
                            //ignore
                        //else if we reach end of edge report list
                            //verify and update all the remaining edges in edge list
                    
                    //vertify and update the destination pointer for each vertex;   check if status is 2
                    //while(snapedge is marked)
                

                    //update the vertex edge status to 1->2
                
                
                
                //loc_snap_vertex_ptr = loc_snap_vertex_ptr->vnext;

    }