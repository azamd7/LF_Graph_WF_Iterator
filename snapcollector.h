
#include "graphDS.h"
#include <vector>
#include "utility.h"
#include <atomic>
#include <algorithm>

using namespace std;


class SnapCollector;

atomic<SnapCollector *> PSC = NULL;

// Vertex's Report Structure
class VertexReport {
    public:
        Vnode * vnode; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        VertexReport * nextReport;

        VertexReport( Vnode * vnode , int action){
            this->vnode = vnode;
            this->action = action;
        };
};

// Edge's Report Structure
class EdgeReport {
    public:
        Enode * enode;
        Vnode * source; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        EdgeReport * nextReport;

        EdgeReport(Enode * enode, Vnode* source, int action){
            this->enode = enode;
            this->source = source;
            this->action = action;
        }
};

bool vertex_comparator(const VertexReport &lhs, const VertexReport &rhs)
{
  if (lhs.vnode->val != rhs.vnode->val)
    return lhs.vnode->val < rhs.vnode->val;
  else if (lhs.vnode != rhs.vnode)
    return true;
  else
    return lhs.action > rhs.action; // delete report will have higher pref
}

bool edge_comparator(const EdgeReport &lhs, const EdgeReport &rhs)
{
  if (lhs.enode->val != rhs.enode->val)
    return lhs.enode->val < rhs.enode->val;
  else if (lhs.enode != rhs.enode)
    return true;
  else
    return lhs.action > rhs.action; // delete report will have higher pref
}

/*for each thread a report will be maintained which cantains a linked list of edge report and vertex report*/
class Report{
    public :
        atomic<EdgeReport *>  head_edge_report;
        atomic<VertexReport *> head_vertex_report;
    
        Report(){
            head_edge_report = nullptr;
            head_vertex_report = nullptr;
        }
};

class Snap_Enode {
    public:
        atomic<Snap_Enode *> enext;
        Enode * enode;
        Snap_Enode(Enode * arg1, Snap_Enode * arg2) {
            this -> enext = arg2; 
            this -> enode = arg1;
        }
};
// the common sentinel snap Enode
Snap_Enode * end_snap_Enode = new Snap_Enode(NULL, NULL);

// Structure for Collected graph's Vertex Node
class Snap_Vnode {
    public:
        Vnode * vnode;
        atomic<Snap_Vnode *> vnext;
        Snap_Enode * ehead; // head of edge linked list

    Snap_Vnode(Vnode * vnode, Snap_Vnode * next_snap_vnode) {
        this -> vnode = vnode;
        this -> vnext = next_snap_vnode;
        Snap_Enode * start_snap_Enode = new Snap_Enode(this->vnode -> ehead, end_snap_Enode);
        this -> ehead = start_snap_Enode;
    }
};

// the common sentinel snap Vnode
Snap_Vnode * end_snap_Vnode = new Snap_Vnode(NULL, NULL);

class SnapCollector{
    public :
        bool active = false;//indicates if the snap collect field is currently active
        Snap_Vnode *head_snap_Vnode;//points to head of the collected vertex list
        atomic<Snap_Vnode *> tail_snap_V_ptr;//points to vertex last added too the collected vertex list
        atomic<Snap_Enode *> tail_snap_E_ptr;//points to edge last added to the collected graph

        bool read_edge = false;// boolean value to indicate if that we are going through the edge
        Report **reports;//array of atomic report for each thread
        //vector <Report> delete_vertex_reports; //This will be used to check the while adding edges 
    
        int no_of_threads;
    

        //for reconstruction using report
    
        vector<VertexReport> *sorted_vertex_reports_ptr;
        vector<EdgeReport> *sorted_edge_reports_ptr;
        atomic<long> vertex_report_index = {0}; //used to store the highest index in sorted vertex reports currently being processed by any thread
        atomic<long> edge_report_index ;//at


        //Here head points to the "start_vnode" of the original graph 
        SnapCollector(Vnode * head , int no_of_threads){
            this->no_of_threads = no_of_threads;
            Snap_Vnode * start_snap_Vnode = new Snap_Vnode(head, end_snap_Vnode );
            head_snap_Vnode = start_snap_Vnode;
            tail_snap_V_ptr = start_snap_Vnode;
            tail_snap_E_ptr = nullptr;

            reports = new Report*[no_of_threads];
            for( int i ; i < no_of_threads ;i++){
                reports[i] = new Report();
            }
         
        }

        //Note : 
        // start_snap_Vnode / end_snap_Vnode indicates the start and end of vertex list 
        // start_snap_Enode / end_snap_Enode indicates the start and end of edge list 
        // tail_snap_Vnode points to vertex which was last updated
        // tail_snap_Enode points to vertex which was last updated
        // snap_vertex_ptr is the vertex which we currently iterating while adding edges
    
        bool isActive(){
            return active;
        }

        void deactivate(){
            this->active = false;
        }
   

        void iterator(){
            

            while( ! this->read_edge){
                Snap_Vnode * temp_tail_snap_V_ptr  = tail_snap_V_ptr;
                Vnode * next_Vnode = temp_tail_snap_V_ptr->vnode -> vnext;
                while(next_Vnode != end_Vnode && is_marked_ref((long) next_Vnode->vnext.load()))
                {
                    next_Vnode = next_Vnode->vnext;
                }
                
                if (next_Vnode == end_Vnode) //reaches the end of the vertex list in original graph
                {
                    while (atomic_compare_exchange_strong(&temp_tail_snap_V_ptr->vnext , &end_snap_Vnode ,(Snap_Vnode*)set_mark((long)end_snap_Vnode)))
                        //if the condition fails means some other thread added a new vnode
                        temp_tail_snap_V_ptr = temp_tail_snap_V_ptr->vnext;
                    read_edge = true;
                    break;
                }


                //create a new snap vertex vertex Node
                Snap_Vnode *snap_Vnode = new Snap_Vnode(next_Vnode,end_snap_Vnode);
                
                //this would fail if some other node is added
                if(atomic_compare_exchange_strong(&temp_tail_snap_V_ptr->vnext , &end_snap_Vnode, snap_Vnode))
                    atomic_compare_exchange_weak(&tail_snap_V_ptr, &temp_tail_snap_V_ptr, snap_Vnode) ;
                else 
                {
                    /// Note  : Can be optimized
                    if ((long)temp_tail_snap_V_ptr->vnext.load() == get_marked_ref((long)end_snap_Vnode))//no more vertex can be added
                    {
                        read_edge = true;
                        break;
                    }   
                    //helping : new edge has been added but vertex tail ptr if not updated
                    atomic_compare_exchange_weak(&tail_snap_V_ptr,  &temp_tail_snap_V_ptr, temp_tail_snap_V_ptr->vnext); 
                }
            }

            Snap_Vnode * snap_edge_vertex_ptr = head_snap_Vnode->vnext;// used to identify current vertex we are iterating
            Snap_Enode * temp_tail_snap_E_ptr = NULL;
            //iterate through the edge
            while (read_edge){
                
                temp_tail_snap_E_ptr = tail_snap_E_ptr;
                
                //temp_tail_E can be prev vertex tail_E in which case its next is marked(end_E_node) 
                //or some edge which belongs to current temp_snap_vertex_ptr or after that

                if (temp_tail_snap_E_ptr == nullptr){ //no edge has been added yet or tail_E has not been updated

                    Snap_Enode * start_snap_Enode = snap_edge_vertex_ptr->ehead ;//start of the edgelist
                    Snap_Enode * tmp = nullptr;
                    ///////////////
                    ///TEST : CAS on null ptr
                    //////////////
                    atomic_compare_exchange_weak(&tail_snap_E_ptr , &temp_tail_snap_E_ptr , start_snap_Enode);
                }
                else
                {
                    //Enode * next_enode = temp_tail_snap_E_ptr -> enode ->enext;
                    Enode * next_Enode = temp_tail_snap_E_ptr->enode;


                    //fetch next edge which is not end_E_node and not marked
                    while ( next_Enode != end_Enode && is_marked_ref((long)next_Enode) && is_marked_ref((long)next_Enode->v_dest)){
                        next_Enode = next_Enode ->enext;
                    }
                    
                    if( next_Enode == end_Enode ){
                        /// Note  : Can be optimized
                        while(atomic_compare_exchange_strong(&temp_tail_snap_E_ptr->enext , &end_snap_Enode , (Snap_Enode*)set_mark((long)end_snap_Enode)) ) //either some thread has updated to marked(end_snap_enode) or added another edge
                            //some other thread has added another node                                                                           // temp_tail_E->next is not marked(end_snap_enode))                         
                            temp_tail_snap_E_ptr = temp_tail_snap_E_ptr->enext;
                    
                        if (snap_edge_vertex_ptr ->vnext == end_snap_Vnode) 
                            break;

                        snap_edge_vertex_ptr = snap_edge_vertex_ptr->vnext;
                        atomic_compare_exchange_weak(&tail_snap_E_ptr , &temp_tail_snap_E_ptr , snap_edge_vertex_ptr->ehead);
                    }
                    else 
                    {//
                        Snap_Enode *snap_Enode = new Snap_Enode(next_Enode , end_snap_Enode);


                        //add the  next node to snap
                        if (atomic_compare_exchange_strong(&temp_tail_snap_E_ptr->enext , &end_snap_Enode ,snap_Enode ))
                            atomic_compare_exchange_weak(&tail_snap_E_ptr , &temp_tail_snap_E_ptr , snap_Enode);
                        else
                            atomic_compare_exchange_weak(&tail_snap_E_ptr , &temp_tail_snap_E_ptr  , temp_tail_snap_E_ptr->enext); //helping
                    }
                }
            }

        }
       
        /**
         * @brief This method adds block nodes at the head of each threads edge report and vertex report linked list
         * 
         * @return ** void 
         */

        void blockFurtherReports(){
            //the head of all the threads vertex reports and edge reports will be assigned to following block reports
            //block report action = 3
            
            for(int i=0;i<this->no_of_threads;i++)
            {
                //block vertex report of thread i
                VertexReport * block_vreport = new VertexReport(nullptr  , 3);
                VertexReport* vrep_head = this->reports[i]->head_vertex_report;
                block_vreport->nextReport = vrep_head;


                // since will only run limited numbner of times, therefore WF
                while(!atomic_compare_exchange_strong(&reports[i]->head_vertex_report, &vrep_head, block_vreport))
                {
                    vrep_head = this->reports[i]->head_vertex_report;
                    block_vreport->nextReport = vrep_head;
                }

                //block edge report of thread i
                EdgeReport * block_ereport = new EdgeReport(nullptr , nullptr ,3 );
                EdgeReport* erep_head = this->reports[i]->head_edge_report;
                block_ereport->nextReport = erep_head;

                 // since will only run limited numbner of times, therefore WF
                while(!atomic_compare_exchange_strong(&reports[i]->head_edge_report, &erep_head, block_ereport))
                {
                    erep_head = this->reports[i]->head_edge_report;
                    block_ereport->nextReport = erep_head;
                }

            }
        }

        void reconstructUsingReports(){
            Snap_Vnode *next_V = head_snap_Vnode;
            vector<VertexReport> vertex_reports ;
            if (sorted_vertex_reports_ptr == nullptr){
                
                for (int i = 0; i < no_of_threads; i++)
                {
                    VertexReport *curr_head= reports[i]->head_vertex_report;
                    curr_head = curr_head->nextReport; // ignore the dummy report
                    while(curr_head)
                    {
                        vertex_reports.push_back(*curr_head);
                        curr_head = curr_head->nextReport;
                    }
                }
                sort(vertex_reports.begin(), vertex_reports.end(), &vertex_comparator);
                sorted_vertex_reports_ptr = &vertex_reports;

            }
            else
                vertex_reports = *sorted_vertex_reports_ptr;
            
            
            Snap_Vnode *prev_snap_Vnode = head_snap_Vnode; 
            Snap_Vnode *next_snap_Vnode = prev_snap_Vnode->vnext;

            while (vertex_report_index < vertex_reports.size()){
                long index = vertex_report_index;
                long prev_index = index;

                VertexReport report = vertex_reports[index];
                while ( (long)next_snap_Vnode != get_marked_ref((long)end_snap_Vnode) and next_snap_Vnode->vnode->val < report.vnode->val){
                    prev_snap_Vnode = next_snap_Vnode;
                    next_snap_Vnode = next_snap_Vnode->vnext;
                } 
                
                if (report.action == 2){
                    //No delete report as the reports are sorted by delete and then insert for same address and value
                    if( next_snap_Vnode->vnode->val != report.vnode->val){
                        Snap_Vnode *s_vnode = new Snap_Vnode(report.vnode, next_snap_Vnode);
                        atomic_compare_exchange_weak(&prev_snap_Vnode->vnext , &next_snap_Vnode , s_vnode);
                    }
                    index++;
                }
                else 
                {

                    
                    if (next_snap_Vnode->vnode == report.vnode) {
                        atomic_compare_exchange_weak(&prev_snap_Vnode->vnext , &next_snap_Vnode , next_snap_Vnode->vnext );
                        next_snap_Vnode = next_snap_Vnode ->vnext;
                    }
      
                    
                    index++;
                    while(report.vnode == vertex_reports[index].vnode){
                        index++;
                    }
                    
                    atomic_compare_exchange_strong(&vertex_report_index , &prev_index, index); //update vertex index
                }
            }
                        
            vector<EdgeReport> edge_reports;
            if (sorted_edge_reports_ptr == nullptr){
                for (int i = 0; i < no_of_threads; i++)
                {
                    EdgeReport *curr_head= reports[i]->head_edge_report;
                    curr_head = curr_head->nextReport; // ignore the dummy report
                    while(curr_head)
                    {
                        edge_reports.push_back(*curr_head);
                        curr_head = curr_head->nextReport;
                    }
                }
                sort(edge_reports.begin(), edge_reports.end(), &edge_comparator);
                sorted_edge_reports_ptr = &edge_reports;
            }
            else
                edge_reports = *sorted_edge_reports_ptr;

            

            
       
            // In one iteration of the while we go to the source vertex of the report and then the locate the edge if present
            // The edges(also for all prev sources) present before we reach the report's edge are verified to check of the destination
            // vertex is present
            // After performing the operation based on the report if the next report is for different source then we check the destination vertex
            // for the remaining edges in the current source  
            // 
            //prev_source/curr_source optimization : In 1 iteration of while loop 1 report is processed and reports belonging to same edge
            // are ignored and also reports belonging to same source are ignored in case the source are not present. Now if source is present 
            // and there exist a report for another edge for same source it goes to next iteration in which case we can reuse the traversing edge 
            // and vertex pointer from previous iteration.
            Snap_Vnode * loc_snap_vertex_ptr = head_snap_Vnode->vnext;
            Vnode * curr_source = nullptr;
            Vnode * prev_source = nullptr;
            Snap_Enode *prev_snap_edge = nullptr;
            Snap_Enode *curr_snap_edge = nullptr;
            Snap_Vnode *dest_vsnap_ptr  = nullptr;//verify the destination vertex of all edges of the source
            while (edge_report_index < edge_reports.size()){

                   
                long index = edge_report_index;
                long prev_index = index;

                //check if edge reports is empty
                EdgeReport report = edge_reports[index]    ;
                curr_source = report.source;

                //fetch the next vertex pointer
                while ((long)loc_snap_vertex_ptr != get_marked_ref((long)end_snap_Vnode)  && (loc_snap_vertex_ptr->vnode->val < curr_source->val )){
                     //remove edge from edge list of loc_snap_vertex_ptr with TO vertex present in DELETE reports
                    dest_vsnap_ptr  = head_snap_Vnode->vnext;
                    Snap_Enode * prev_snap_edge = loc_snap_vertex_ptr->ehead;
                    Snap_Enode * curr_snap_edge = loc_snap_vertex_ptr->ehead->enext;
                    while ((long)curr_snap_edge != get_marked_ref((long)end_snap_Enode)){
                        while((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val)
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        if (dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                            //delete the edge
                            atomic_compare_exchange_strong(&prev_snap_edge->enext , &curr_snap_edge , curr_snap_edge->enext);
                            curr_snap_edge = curr_snap_edge->enext;
                        }
                        else {
                            prev_snap_edge = curr_snap_edge;
                            curr_snap_edge = curr_snap_edge->enext;
                        }
                    }

                   
                    loc_snap_vertex_ptr = loc_snap_vertex_ptr->vnext;
                }
                //if the correct source is not found ie. with same address
                if (loc_snap_vertex_ptr->vnode != curr_source){
                    index++; 
                    //skip all the reports belonging to same source
                    while (edge_reports[index].source == curr_source) //ignore all report belonging to same dest and source
                        index++;
                    atomic_compare_exchange_strong(&edge_report_index, &prev_index,index);//update edge index
                    continue ;
                }
                //Found the correct source

                if(prev_source != curr_source){ 
                    prev_snap_edge = loc_snap_vertex_ptr->ehead;
                    curr_snap_edge = prev_snap_edge->enext;
                    dest_vsnap_ptr  = head_snap_Vnode->vnext;//verify the destination vertex of all edges of the source
                }
                while( (long)curr_snap_edge != get_marked_ref((long)end_snap_Enode) and curr_snap_edge->enode->val < report.enode->val){
                    //check if edge dest vertex is present if not remove
                    while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val)
                        dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                    if (dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                        //delete the edge
                        atomic_compare_exchange_strong(&prev_snap_edge->enext , &curr_snap_edge , curr_snap_edge->enext);
                        curr_snap_edge = curr_snap_edge->enext;
                    }
                    else{
                        prev_snap_edge = curr_snap_edge;
                        curr_snap_edge = curr_snap_edge->enext;
                    }
                }

                if(report.action == 2){//report action
                    if(curr_snap_edge->enode != report.enode){

                        while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < report.enode->v_dest->val)
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        
                        if (dest_vsnap_ptr->vnode != report.enode->v_dest){ //no delete report TO edge address and value
                            Snap_Enode *snode = new Snap_Enode(report.enode,curr_snap_edge);
                            atomic_compare_exchange_strong(&prev_snap_edge->enext ,&curr_snap_edge ,snode);
                        }
                    }
                    index++;
                }
                else//delete report
                {
                    if (curr_snap_edge->enode == report.enode){
                        atomic_compare_exchange_strong(&prev_snap_edge->enext ,&curr_snap_edge ,curr_snap_edge->enext);
                        curr_snap_edge = curr_snap_edge->enext;
                    }
                    Enode * curr_edge = report.enode;
                    index++;
                    while( edge_reports[index].enode == report.enode  )//ignore all report belonging to edge
                        index++;
                }
                if (edge_reports[index].source != curr_source){ //next report is of different source
                    //verify the remaining edges of current source
                    while ((long)curr_snap_edge != get_marked_ref((long)end_snap_Enode)){
                        while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val)
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        if (dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                            //delete the edge
                            atomic_compare_exchange_strong(&prev_snap_edge->enext , &curr_snap_edge , curr_snap_edge->enext);
                            curr_snap_edge = curr_snap_edge->enext;
                        }
                        else{
                            prev_snap_edge = curr_snap_edge;
                            curr_snap_edge = curr_snap_edge->enext;
                        }
                    }
                }
                
                atomic_compare_exchange_weak(&edge_report_index,&prev_index,index);//update edge index
                prev_source = curr_source;
            }
        }


};
/**
 * @brief  Checks whether there is active reference to snapcollector object if not creates a new snapcollector object
 * 
 * @param graph_head head of graph vertex list which we need to iterate
 * @param max_threads max number of threads that will can access/create the snapshot object
 * @return ** SnapCollector 
 */
SnapCollector * acquireSnapCollector(Vnode * graph_head, int max_threads){
    SnapCollector *SC = PSC;
    if (SC != nullptr and SC->active){
        return SC;
    }
    SnapCollector *newSC = new SnapCollector(graph_head , max_threads);
    atomic_compare_exchange_strong(&PSC , &SC , newSC);//if this fails some other thread has created and updated a new snapcollector
    newSC = PSC ;
    return newSC;
    
}

/**
 * @brief Creates a snapshot of a graph object
 * 
 * @param graph_head head of graph vertex list which we need to iterate
 * @param max_threads max number of threads that will can access/create the snapshot object
 * @return  ** SnapCollector 
 */
SnapCollector * takeSnapshot(Vnode * graph_head ,  int max_threads){
    SnapCollector *SC = acquireSnapCollector(graph_head , max_threads);
    SC-> iterator();
    
    SC->deactivate();
    
    SC->blockFurtherReports();
    SC->reconstructUsingReports();
    return SC;
}
      


      



/**
 * @brief This method is called by the graph operations to add vertex report incase of insertion or deletion 
 * of vertex

 * @param victim 
 * @param tid 
 * @param action insert->2/delete->1/block->3
 */
void reportVertex(Vnode *victim,int tid, int action){
    SnapCollector * SC = PSC;
    if(SC->isActive()) {
        if(action == 2 && is_marked_ref((long) victim->vnext.load()))
            return;
        VertexReport *rep = new VertexReport(victim, action);
        VertexReport *vreport_head = SC->reports[tid]->head_vertex_report   ;
        if (vreport_head == nullptr && vreport_head->action != 3){
            rep-> nextReport = vreport_head;
            atomic_compare_exchange_strong(&SC->reports[tid]->head_vertex_report, &vreport_head, rep);
        }
    }
}

/**
 * @brief This method is called by the graph operations to add edge report incase of insertion or deletion 
 * of edge
 * @param victim
 * @param source_enode
 * @param tid 
 * @param action insert->2/delete->1/block->3
 */
void reportEdge(Enode *victim,Vnode *source_enode, int tid, int action){
    SnapCollector * SC = PSC;
    if(SC->isActive()) {
        if(action == 2 && is_marked_ref((long) victim->enext.load()))
            return;
        EdgeReport *rep = new EdgeReport(victim,source_enode, 2);
        EdgeReport *vreport_head = SC->reports[tid]->head_edge_report   ;
        if (vreport_head == nullptr && vreport_head->action != 3){
            rep-> nextReport = vreport_head;
            atomic_compare_exchange_strong(&SC->reports[tid]->head_edge_report, &vreport_head, rep);
        }
    }
}

