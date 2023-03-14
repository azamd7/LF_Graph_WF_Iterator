
#include "graphDS.h"
#include <vector>
#include "utility.h"
#include <atomic>
#include <algorithm>
#include <fstream>
using namespace std;


class SnapCollector;

atomic<SnapCollector *> PSC = {nullptr};

bool flag = false;
// Vertex's Report Structure
class VertexReport {
    public:
        Vnode * vnode; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        VertexReport * nextReport;

        VertexReport( Vnode * vnode , int action, VertexReport *nextReport){
            this->vnode = vnode;
            this->action = action;
            this->nextReport = nextReport;
        };

        VertexReport(){
        }
};

// Edge's Report Structure
class EdgeReport {
    public:
        Enode * enode;
        Vnode * source; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        EdgeReport * nextReport;

        EdgeReport(Enode * enode, Vnode* source, int action, EdgeReport *nextReport){
            this->enode = enode;
            this->source = source;
            this->action = action;
            this->nextReport = nextReport;
        }
};

bool vertex_comparator(const VertexReport &lhs, const VertexReport &rhs)
{   

    if (lhs.vnode->val != rhs.vnode->val)
        return lhs.vnode->val < rhs.vnode->val;
    else if (lhs.vnode != rhs.vnode)
        return true;
    else
        return lhs.action < rhs.action; // delete report will have higher pref

}

bool edge_comparator(const EdgeReport &lhs, const EdgeReport &rhs)
{
    if (lhs.source->val != rhs.source->val)
        return lhs.source->val < rhs.source->val;
    if (lhs.enode->val != rhs.enode->val)
        return lhs.enode->val < rhs.enode->val;
    else if (lhs.enode != rhs.enode)
        return true;
    else
        return lhs.action < rhs.action; // delete report will have higher pref
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
        ~Report(){
            EdgeReport * tmp_rep = head_edge_report.load();
            EdgeReport * next_tmp_rep;
            while(tmp_rep!= nullptr){
                next_tmp_rep = tmp_rep->nextReport;
                delete tmp_rep;
            }
            
            VertexReport * tmp_rep1 = head_vertex_report.load();
            VertexReport * next_tmp_rep1;
            while(tmp_rep1!= nullptr){
                next_tmp_rep1 = tmp_rep1->nextReport;
                delete tmp_rep1;
            }
            
        }
};

class Snap_Enode {
    public:
        atomic<Snap_Enode *> enext;
        Enode * enode;
        Snap_Enode(Enode * enode, Snap_Enode * enext) {
            this -> enext = enext; 
            this -> enode = enode;
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

    //is_reconstruct is true then end enode is marked
    Snap_Vnode(Vnode * vnode, Snap_Vnode * next_snap_vnode, bool is_reconstruct = false) {
        
        this -> vnode = vnode;
        this -> vnext = next_snap_vnode;
        Snap_Enode * start_snap_Enode;
        if (!is_reconstruct)
            start_snap_Enode = new Snap_Enode(this->vnode-> ehead, end_snap_Enode);
        else
            start_snap_Enode = new Snap_Enode(this->vnode-> ehead, (Snap_Enode *)set_mark((long)end_snap_Enode));           
        this -> ehead = start_snap_Enode;
       
    }


    Snap_Vnode(Vnode * vnode, Snap_Vnode * next_snap_vnode , Snap_Enode *end) {
        
        this -> vnode = vnode;
        this -> vnext = next_snap_vnode;
        Snap_Enode * start_snap_Enode = new Snap_Enode(this->vnode-> ehead, end_snap_Enode);
        this -> ehead = start_snap_Enode;
       
    }

    ~Snap_Vnode(){
        Snap_Enode * tmp = ehead;
        Snap_Enode * tmp_next = ehead->enext;
        delete tmp;
        while(tmp_next != end_snap_Enode){
            tmp = tmp_next;
            tmp_next = tmp_next->enext;
            delete tmp;
        }
    }
};

// the common sentinel snap Vnode
Snap_Vnode *  end_snap_Vnode = new Snap_Vnode(end_Vnode, NULL);

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
        atomic<long> edge_report_index ;//at


        //Here head points to the "start_vnode" of the original graph 
        SnapCollector(Vnode * head , int no_of_threads){
            this->no_of_threads = no_of_threads;
            Snap_Vnode * start_snap_Vnode = new Snap_Vnode(head, end_snap_Vnode );
            head_snap_Vnode = start_snap_Vnode;
            tail_snap_V_ptr = start_snap_Vnode;
            tail_snap_E_V_ptr = nullptr;
            this->activate();

            reports = new Report*[no_of_threads];
            for( int i ; i < no_of_threads ;i++){
                reports[i] = new Report();
            }
            //++threads_accessing;
            

        }

        ~SnapCollector(){
            Snap_Vnode * tmp = head_snap_Vnode;
            Snap_Vnode * tmp_next = head_snap_Vnode->vnext;

            delete tmp;
            while(tmp_next != end_snap_Vnode){
                tmp = tmp_next;
                tmp_next = tmp_next ->vnext;
                delete tmp;
            }
            
            vector<EdgeReport>  *a = sorted_edge_reports_ptr.load();
            if(a != nullptr)
                delete a;
            vector<VertexReport>  *b = sorted_vertex_reports_ptr.load();
            if(b != nullptr)
                delete b;
            
            for( int i ; i < no_of_threads ;i++){
                    delete reports[i];
            }
        }

        //Note : 
        // start_snap_Vnode / end_snap_Vnode indicates the start and end of vertex list 
        // start_snap_Enode / end_snap_Enode indicates the start and end of edge list 
        // tail_snap_Vnode points to vertex which was last updated
        // tail_snap_Enode points to vertex which was last updated
        // snap_vertex_ptr is the vertex which we currently iterating while adding edges
    
        bool isActive(){
            return active.load();
        }

        void deactivate(){
            this->active = false;
        }
        void activate(){
            this->active = true;
        }
   

        void iterator(fstream * logfile, bool debug){
            if(debug)
                (*logfile) << "Vertex Iteration" << endl;
            while( ! this->read_edge){
                
                Snap_Vnode * temp_tail_snap_V_ptr  = tail_snap_V_ptr;
                Vnode * next_Vnode = (Vnode *)get_unmarked_ref((long) temp_tail_snap_V_ptr->vnode -> vnext.load());

                while(next_Vnode != end_Vnode && is_marked_ref((long) next_Vnode->vnext.load()))
                {
                    next_Vnode = (Vnode *)get_unmarked_ref((long)next_Vnode->vnext.load());
                    
                }
                if (next_Vnode == end_Vnode) //reaches the end of the vertex list in original graph
                {
                    
                    Snap_Vnode * tmp_end_snap_Vnode = end_snap_Vnode;//For CAS 
                    while (!atomic_compare_exchange_strong(&temp_tail_snap_V_ptr->vnext , &tmp_end_snap_Vnode ,(Snap_Vnode*)set_mark((long)end_snap_Vnode)))
                    {   
                        
                        Snap_Vnode * temp_snapv_ptr = temp_tail_snap_V_ptr->vnext.load();

                        
                        //if the condition fails means some other thread added a new vnode
                        if(is_marked_ref((long)temp_snapv_ptr)){
                            break;//if some other node added a endnode
                        }
                        tmp_end_snap_Vnode = end_snap_Vnode;
                        temp_tail_snap_V_ptr = temp_snapv_ptr;
                    }
                    read_edge = true;
                    break;
                }
                else
                {
                    //create a new snap vertex vertex Node
                    Snap_Vnode *snap_Vnode = new Snap_Vnode(next_Vnode,end_snap_Vnode);
                    if(debug)
                        (*logfile) << "To Add snap Vertex : " << next_Vnode->val << "(" << next_Vnode << ")" << endl;
                    Snap_Vnode * tmp_end_snap_Vnode = end_snap_Vnode;
                    //this would fail if some other node is added
                    if(atomic_compare_exchange_strong(&temp_tail_snap_V_ptr->vnext , &tmp_end_snap_Vnode, snap_Vnode)){
                        (*logfile) << "Snap Added Vertex : " << snap_Vnode->vnode->val << "(" << snap_Vnode->vnode << ")" << endl;
                        if(atomic_compare_exchange_strong(&tail_snap_V_ptr, &temp_tail_snap_V_ptr, snap_Vnode) and debug)
                            (*logfile) << "Updated tail Snap Vertex : " << next_Vnode->val << "(" << next_Vnode << ")" << endl;
                    }
                    else 
                    {
                        
                        Snap_Vnode *tmp_ptr = temp_tail_snap_V_ptr->vnext.load();
                        /// Note  : Can be optimized
                        if ((long)tmp_ptr == get_marked_ref((long)end_snap_Vnode))//no more vertex can be added
                        {
                            read_edge = true;
                            break;
                        }   
                       
                        //helping : new edge has been added but vertex tail ptr is not updated
                        if(atomic_compare_exchange_strong(&tail_snap_V_ptr,  &temp_tail_snap_V_ptr, tmp_ptr) and debug)
                            (*logfile) << "Updated tail Snap Vertex : " << tmp_ptr->vnode->val << "(" << tmp_ptr->vnode << ")" << endl;; 
                    }
                }
            }
            
            Snap_Vnode * snap_edge_vertex_ptr = nullptr;// used to identify current vertex we are iterating
            //iterate through the edge
            while (read_edge){
                
                snap_edge_vertex_ptr = tail_snap_E_V_ptr; //The vertex whose edge list is currently being iterated
                
               

                if (snap_edge_vertex_ptr == nullptr){ 
                    
                    if(is_marked_ref((long)head_snap_Vnode->vnext.load()))//This will be marked only in case if there are no vertice in snapshot
                    {   
                        break;
                    }
                    if(atomic_compare_exchange_strong(&tail_snap_E_V_ptr , &snap_edge_vertex_ptr , head_snap_Vnode->vnext.load()) and debug)
                        (*logfile) << "Updated curretly iterating vertex  : " << head_snap_Vnode->vnext.load()->vnode->val << "(" << head_snap_Vnode->vnext.load() << ")" <<endl;
                }
                else
                {
                    //if some node is iterating through a vertex's edge list
                    
                    //goto the end of the snap edge list (end_snap_enode)
                    //if end_snap_enode is marked then update the vertex 

                    Snap_Enode *prev_snap_Enode = snap_edge_vertex_ptr->ehead;
                    Snap_Enode *curr_snap_Enode = prev_snap_Enode->enext.load();
                    
                    next_iter:
                    while(get_unmarked_ref((long)curr_snap_Enode ) != (long)end_snap_Enode){
                        prev_snap_Enode = curr_snap_Enode;
                        curr_snap_Enode = prev_snap_Enode->enext.load();
                    }
                    
                    if(is_marked_ref((long)curr_snap_Enode)){
                        if(is_marked_ref((long)snap_edge_vertex_ptr->vnext.load()))
                            break;
                        if(atomic_compare_exchange_strong(&tail_snap_E_V_ptr , &snap_edge_vertex_ptr , snap_edge_vertex_ptr->vnext.load()) and debug)
                            (*logfile) << "Updated curretly iterating vertex  : " << head_snap_Vnode->vnext.load()->vnode->val << "(" << head_snap_Vnode->vnext.load() << ")" <<endl;
                    }
                    else{
                        
                        //Fetch the next node in the original graph for the prev snap node's enode
                        Enode * next_Enode = (Enode *)get_unmarked_ref((long) prev_snap_Enode->enode->enext.load());
                        //fetch next edge which is not end_E_node and not marked or dest is not marked
                        while ( next_Enode != end_Enode && (is_marked_ref((long)next_Enode) || is_marked_ref((long)next_Enode->v_dest->vnext.load()))){
                            next_Enode = (Enode *)get_unmarked_ref((long)next_Enode ->enext.load());
                        }

                        //if next Enode is end_Enode then mark the next and if marked update the edge vertex ptr to next vertex 
                        //check next vertex pointer is marked or not  if marked then break
                        if(next_Enode == end_Enode){
                            Snap_Enode * tmp_end_snap_Enode = end_snap_Enode;
                            if(atomic_compare_exchange_strong(&prev_snap_Enode->enext , &tmp_end_snap_Enode , (Snap_Enode*)set_mark((long)end_snap_Enode)) ) //either some thread has updated to marked(end_snap_enode) or added another edge
                            {
                                if(is_marked_ref((long)snap_edge_vertex_ptr->vnext.load()))
                                    break;
                                if(atomic_compare_exchange_strong(&tail_snap_E_V_ptr , &snap_edge_vertex_ptr , snap_edge_vertex_ptr->vnext.load()) and debug)
                                    (*logfile) << "Updated curretly iterating vertex  : " << snap_edge_vertex_ptr->vnext.load()->vnode->val << "(" << snap_edge_vertex_ptr->vnext.load()<< ")" <<endl;
                            }
                        }
                        else{
                            //if not end_Enode then 
                            Snap_Enode * tmp_end_snap_Enode = end_snap_Enode;
                            Snap_Enode *snap_Enode = new Snap_Enode(next_Enode , end_snap_Enode);

                        
                            if (atomic_compare_exchange_strong(&prev_snap_Enode->enext , &tmp_end_snap_Enode ,snap_Enode ))
                            {
                                if(debug)
                                    (*logfile) << "Added Snap Edge : " <<  next_Enode->val << "("<< next_Enode << ")" << endl;
                                prev_snap_Enode = snap_Enode;
                                curr_snap_Enode = prev_snap_Enode->enext;
                                goto next_iter;
                            }
                            else{
                                //helping part
                                if(is_marked_ref((long) prev_snap_Enode->enext.load())){
                                    if(is_marked_ref((long)snap_edge_vertex_ptr->vnext.load()))
                                        break;
                                    if(atomic_compare_exchange_strong(&tail_snap_E_V_ptr , &snap_edge_vertex_ptr , snap_edge_vertex_ptr->vnext.load()) and debug)
                                        (*logfile) << "Updated curretly iterating vertex  : " << snap_edge_vertex_ptr->vnext.load()->vnode->val << "(" << snap_edge_vertex_ptr->vnext.load()<< ")" <<endl;
                                }
                                else{
                                    prev_snap_Enode = prev_snap_Enode->enext;
                                    curr_snap_Enode = prev_snap_Enode->enext;
                                    goto next_iter;
                                }
                            }
                            
                        }                       
                    }
                }
            }
      
        }
       
        /**
         * @brief This method adds block nodes at the head of each threads edge report and vertex report linked list
         * 
         * @return ** void 
         */

        void blockFurtherReports(fstream * logfile, bool debug){
            //the head of all the threads vertex reports and edge reports will be assigned to following block reports
            //block report action = 3
            if(debug)
                (*logfile) << "Blocking reports" << endl;
            for(int i=0;i<this->no_of_threads;i++)
            {
                //block vertex report of thread i
                
                VertexReport* vrep_head = this->reports[i]->head_vertex_report;
                VertexReport * block_vreport = new VertexReport(nullptr  , 3 , vrep_head);

                if(vrep_head == nullptr || vrep_head->action != 3){
                    // since will only run limited numbner of times, therefore WF
                    while( !atomic_compare_exchange_strong(&reports[i]->head_vertex_report, &vrep_head, block_vreport))
                    {
                        vrep_head = this->reports[i]->head_vertex_report;
                        if(vrep_head->action == 3)
                        {
                            delete block_vreport;
                            break;
                        }
                        block_vreport->nextReport = vrep_head;
                    }
                }
                if(debug)
                    (*logfile) << "Blocked vertex reports" << endl;
                //block edge report of thread i
                
                EdgeReport* erep_head = this->reports[i]->head_edge_report;
                EdgeReport * block_ereport = new EdgeReport(nullptr , nullptr ,3, erep_head );

                 // since will only run limited numbner of times, therefore WF
                 if(erep_head== nullptr || erep_head->action!= 3 ){
                    while(!atomic_compare_exchange_strong(&reports[i]->head_edge_report, &erep_head, block_ereport))
                    {
                        erep_head = this->reports[i]->head_edge_report;
                        if(erep_head->action == 3){
                            delete block_ereport ;
                            break;
                        }
                        block_ereport->nextReport = erep_head;
                    }
                 }
                if(debug)
                    (*logfile) << "Blocked edge reports" << endl;

            }
        }

        void reconstructUsingReports(fstream * logfile , bool debug){
            Snap_Vnode *next_V = head_snap_Vnode;
           
            vector<VertexReport> *vreports  = sorted_vertex_reports_ptr.load();
            if(vreports == nullptr){
                vector<VertexReport> *vreports1  = new vector<VertexReport>() ;
                for (int i = 0; i < no_of_threads; i++)
                {
                    VertexReport *curr_head= reports[i]->head_vertex_report;
                    curr_head = curr_head->nextReport; // ignore the dummy report
                    while(curr_head)
                    {
                        if(debug)
                            (*logfile)<< "Vertex add report " << curr_head->vnode->val <<"("<< curr_head->vnode<<") " << " action " << curr_head->action << endl; 
                        vreports1->push_back(*curr_head);
                        curr_head = curr_head->nextReport;
                        
                    }
                }

                vreports = new vector<VertexReport>(vreports1->size());
                partial_sort_copy(vreports1->begin(), vreports1->end(),vreports->begin(), vreports->end() ,&vertex_comparator);
                delete vreports1;
                vector<VertexReport> *tmp = nullptr;
                if(!atomic_compare_exchange_strong(&sorted_vertex_reports_ptr , &tmp , vreports)){
                    delete vreports;
                    vreports = tmp; //if cas fails tmp will have the current value
                }
            }
            
            
           
            
            Snap_Vnode *prev_snap_Vnode = head_snap_Vnode; 
            Snap_Vnode *next_snap_Vnode = prev_snap_Vnode->vnext;
            
            long vreport_size = vreports->size();
            while (true){
                long index = vertex_report_index;
                if(index >= vreport_size)
                    break;
                long prev_index = index;

                VertexReport report = vreports->at(index);
                if(debug)
                    (*logfile) << "Vertex report : " << report.vnode->val << " action : " << report.action << endl;
                while ( (long)next_snap_Vnode != get_marked_ref((long)end_snap_Vnode) and next_snap_Vnode->vnode->val < report.vnode->val){
                    prev_snap_Vnode = next_snap_Vnode;
                    next_snap_Vnode = next_snap_Vnode->vnext;
                } 
                   
                if (report.action == 2){
                    //No delete report as the reports are sorted by delete and then insert for same address and value
                    if( (long)next_snap_Vnode == get_marked_ref((long)end_snap_Vnode)&& next_snap_Vnode->vnode != report.vnode){
                         
                        Snap_Vnode *s_vnode = new Snap_Vnode(report.vnode, next_snap_Vnode,true);
                        atomic_compare_exchange_strong(&prev_snap_Vnode->vnext , &next_snap_Vnode , s_vnode);
                        if(debug)
                            (*logfile) << "Vertex Added : " <<  prev_snap_Vnode->vnext.load()->vnode->val <<"(" <<  prev_snap_Vnode->vnext.load()->vnode << ")  " << report.action << endl;
                        next_snap_Vnode = prev_snap_Vnode->vnext ;
                       
                    }
                    else
                    {
                        if(debug)
                            (*logfile) << "Vertex already present : " <<  report.vnode->val <<"(" <<  report.vnode << ")  " << report.action << endl;
                    }
                    index++;
                   
                }
                else 
                {
                    //vertex deletion

                    if (next_snap_Vnode->vnode == report.vnode) {
                        Snap_Vnode * tmp_snap_Vnode = next_snap_Vnode;
                        if(atomic_compare_exchange_strong(&prev_snap_Vnode->vnext , &tmp_snap_Vnode , next_snap_Vnode->vnext.load() ) and debug)
                            (*logfile) << "Vertex deleted : " <<  next_snap_Vnode->vnode->val <<"(" <<  next_snap_Vnode->vnode << ")  " << report.action << endl;
                        next_snap_Vnode = next_snap_Vnode ->vnext;
                    }
                    else{
                        if(debug)
                            (*logfile) << "Vertex not found : " <<  report.vnode->val <<"(" <<  report.vnode << ")  " << report.action << endl;
                    }
      
                    
                    index++;
                    while(index < vreport_size and report.vnode == vreports->at(index).vnode){
                        index++;
                    }
                    
                    
                }
                atomic_compare_exchange_strong(&vertex_report_index , &prev_index, index); //update vertex index
            }

            

            /// @brief processing edge reports
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
            
            
          
            
            
       
            // In one iteration of the while we go to the source vertex of the report and then the locate the edge if present
            // The edges(also for all prev sources) present before we reach the report's edge are verified to check of the destination
            // vertex is present
            // After performing the operation based on the report if the next report is for different source then we check the destination snap vertex
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
            long ereport_size = edge_reports->size();
            while (true){
                
                long index = edge_report_index;
                if(index >= ereport_size)
                    break;
                long prev_index = index;
                
                //check if edge reports is empty
                EdgeReport report = edge_reports->at(index);
                curr_source = report.source;
                if(debug)
                    (*logfile) << "Edge report: " << report.source->val<<"(" << report.source << ") " << report.enode->val <<"(" << report.enode->v_dest << ") "<<" "<< report.action<< endl;
                //fetch the next vertex pointer
                while ((long)loc_snap_vertex_ptr != get_marked_ref((long)end_snap_Vnode)  && (loc_snap_vertex_ptr->vnode->val < curr_source->val )){
                     //remove edge from edge list of loc_snap_vertex_ptr with TO vertex present in DELETE reports
                    if(debug)
                        (*logfile) << "checking vertex : " << loc_snap_vertex_ptr->vnode->val  << "(" <<loc_snap_vertex_ptr->vnode << ")" <<endl;
                    dest_vsnap_ptr  = head_snap_Vnode->vnext;
                    prev_snap_edge = loc_snap_vertex_ptr->ehead;
                    curr_snap_edge = loc_snap_vertex_ptr->ehead->enext;
                    while ((long)curr_snap_edge != get_marked_ref((long)end_snap_Enode)){
                        if(debug)
                            (*logfile) << "check if edge dest vertex is present " << curr_snap_edge->enode->val << "(" <<curr_snap_edge<< ")" <<endl;
                        //check if dest vertex is present for curr snap edge(not the report related edge)
                        while((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) 
                                    and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val){
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        }
                        if ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) && dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                            //delete the edge
                            Snap_Enode * tmp_snap_edge =  curr_snap_edge;
                            if(atomic_compare_exchange_strong(&prev_snap_edge->enext , &tmp_snap_edge , curr_snap_edge->enext.load()) and debug)
                                (*logfile) << "Edge Deleted: " << loc_snap_vertex_ptr->vnode->val<<"(" << loc_snap_vertex_ptr->vnode << ") " << curr_snap_edge->enode->val <<"(" << curr_snap_edge->enode->v_dest << ") " <<" "<< endl;
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
                    if(debug)
                        (*logfile) <<"For edge : " <<report.source->val<<"(" << report.source << ") " << report.enode->val <<"(" << report.enode->v_dest << ")  "<< " source"<<curr_source->val << "("<<curr_source <<")  not found" << endl;
                    index++; 
                    //skip all the reports belonging to same source
                    while (index < ereport_size and edge_reports->at(index).source == curr_source) //ignore all report belonging to same dest and source
                        index++;
                    atomic_compare_exchange_strong(&edge_report_index, &prev_index,index);//update edge index
                    continue ;
                }
                //Found the correct source
               
                if(prev_source != curr_source){ 
                    if(debug)
                        (*logfile) << "prev source " << "(" <<prev_source << ") is not same as current source " << curr_source->val << "("<<curr_source << ")"<< endl;
                    prev_snap_edge = loc_snap_vertex_ptr->ehead;
                    curr_snap_edge = prev_snap_edge->enext;
                    dest_vsnap_ptr  = head_snap_Vnode->vnext;//verify the destination vertex of all edges of the source
                
                }

                while( (long)curr_snap_edge != get_marked_ref((long)end_snap_Enode) 
                                and curr_snap_edge->enode->val < report.enode->val){
                    //check if edge dest vertex is present if not remove
                    while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val)
                        dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                    if (dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                        //delete the edge
                        Snap_Enode * temp_snap_Enode = curr_snap_edge;
                        if(atomic_compare_exchange_strong(&prev_snap_edge->enext , &temp_snap_Enode , curr_snap_edge->enext.load()) and debug)
                            (*logfile) << "Edge Deleted: " << curr_source->val<<"(" << curr_source << ") " << curr_snap_edge->enode->val <<"(" << curr_snap_edge->enode->v_dest << ") " <<" "<< endl;
                        curr_snap_edge = curr_snap_edge->enext;
                    }
                    else{
                        prev_snap_edge = curr_snap_edge;
                        curr_snap_edge = curr_snap_edge->enext;
                    }

                }
                if(report.action == 2){//report action
                    if(curr_snap_edge->enode != report.enode){
                        
                        while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) 
                                and dest_vsnap_ptr->vnode->val < report.enode->v_dest->val){
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        }
                        if ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode == report.enode->v_dest){ //no delete report TO edge address and value
                                           

                            Snap_Enode *snode = new Snap_Enode(report.enode,curr_snap_edge);
                            if(atomic_compare_exchange_strong(&prev_snap_edge->enext ,&curr_snap_edge ,snode) and debug)
                                (*logfile) << "Edge Added: " << loc_snap_vertex_ptr->vnode->val<<"(" << loc_snap_vertex_ptr->vnode << ") " << prev_snap_edge->enext.load()->enode->val <<"(" << prev_snap_edge->enext << ") " <<" "<< report.action<< endl;
                            curr_snap_edge = prev_snap_edge->enext;
                            
                        }
                        else{
                            if(debug)
                                (*logfile) <<"For edge : " <<report.source->val<<"(" << report.source << ") " << report.enode->val <<"(" << report.enode->v_dest << ")  "<< " dest "<<report.enode->v_dest->val << "("<<report.enode->v_dest <<")  not found" << endl;
                        }
                    }
                    else{
                        if(debug)
                            (*logfile) <<"Edge : " <<report.source->val<<"(" << report.source << ") " << report.enode->val <<"(" << report.enode->v_dest << ")  already present" << endl;
                    }
                    index++;
                }
                else//delete report
                {
                    
                    if (curr_snap_edge->enode == report.enode){
                        if(atomic_compare_exchange_strong(&prev_snap_edge->enext ,&curr_snap_edge ,curr_snap_edge->enext.load()) and debug)
                            (*logfile) << "Edge Deleted: " << loc_snap_vertex_ptr->vnode->val<<"(" << loc_snap_vertex_ptr->vnode << ") " << curr_snap_edge->enode->val <<"(" << curr_snap_edge->enode->v_dest << ") " <<" "<< report.action<< endl;
                        curr_snap_edge = curr_snap_edge->enext;
                    }
                    else{
                        if(debug)
                            (*logfile) <<"Edge" <<report.source->val<<"(" << report.source << ") " << report.enode->val <<"(" << report.enode->v_dest << ")  not found for deletion" << endl;
                    }
                    Enode * curr_edge = report.enode;
                    index++;
                    while( index < ereport_size and edge_reports->at(index).enode == report.enode  )//ignore all report belonging to edge
                        index++;
                }
                if (index < ereport_size and edge_reports->at(index).source != curr_source){ //next report is of different source
                    //verify the remaining edges of current source
                    while ((long)curr_snap_edge != get_marked_ref((long)end_snap_Enode)){
                        while ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode->val < curr_snap_edge->enode->val)
                            dest_vsnap_ptr = dest_vsnap_ptr->vnext;
                        if ((long)dest_vsnap_ptr != get_marked_ref((long)end_snap_Vnode) and dest_vsnap_ptr->vnode != curr_snap_edge->enode->v_dest){
                            //delete the edge
                            Snap_Enode * tmp_snap_Edge = curr_snap_edge;
                            if(atomic_compare_exchange_strong(&prev_snap_edge->enext , &tmp_snap_Edge , curr_snap_edge->enext.load()) and debug)
                                (*logfile) << "Edge Deleted: " << curr_source->val<<"(" << curr_source << ") " << curr_snap_edge->enode->val <<"(" << curr_snap_edge->enode->v_dest << ") " <<" "<< report.action<< endl;

                            curr_snap_edge = curr_snap_edge->enext;
                        }
                        else{
                            prev_snap_edge = curr_snap_edge;
                            curr_snap_edge = curr_snap_edge->enext;
                        }
                    }
                }
                
                atomic_compare_exchange_strong(&edge_report_index,&prev_index,index);//update edge index
                prev_source = curr_source;
            }
        }
    
        void print_snap_graph(fstream *logfile){
            (*logfile) << "Snapped Graph ---------- of snapshot : " << this  << endl;
            Snap_Vnode * snap_vnode = head_snap_Vnode->vnext;
            while(get_unmarked_ref((long)snap_vnode) != (long)end_snap_Vnode){
                string val = to_string(snap_vnode->vnode->val);
                
                (*logfile) << val << "(" << snap_vnode->vnode << ") " <<endl ;

                Snap_Enode *snap_enode = snap_vnode->ehead->enext;
                
                while(get_unmarked_ref((long)snap_enode) != (long)end_snap_Enode){
                    
                    string e_val = to_string(snap_enode->enode->val);
                    
                    e_val = " -> " + e_val ;
                    (*logfile) << e_val <<"(" << snap_enode->enode->v_dest << ") " <<endl ;
                    snap_enode = snap_enode -> enext;
                    
                }
                (*logfile) <<" Tailp-e " <<snap_enode << endl;
                
                (*logfile) << endl;
                (*logfile) << "|" <<endl;
                snap_vnode = snap_vnode->vnext;
                

            }
            (*logfile) << "Tail" << endl;
            (*logfile) << "Graph(End)-------" << endl;
        }
};
/**
 * @brief  Checks whether there is active reference to snapcollector object if not creates a new snapcollector object
 * 
 * @param graph_head head of graph vertex list which we need to iterate
 * @param max_threads max number of threads that will can access/create the snapshot object
 * @return ** SnapCollector 
 */
SnapCollector * acquireSnapCollector(Vnode * graph_head, int max_threads,fstream * logfile, bool debug){
    SnapCollector *SC = PSC;
  
    if (SC != nullptr and SC->isActive()){
        //int num = ++SC->threads_accessing;
        return SC;
    }
    
    SnapCollector *newSC = new SnapCollector(graph_head , max_threads);
    
    if(!atomic_compare_exchange_strong(&PSC , &SC , newSC)){
        //if this fails some other thread has created and updated a new snapcollector
        delete newSC ;
        newSC = SC ;
        //int num = ++newSC->threads_accessing ;
        
        
    }
    
    
    return newSC;
    
}

/**
 * @brief Creates a snapshot of a graph object
 * 
 * @param graph_head head of graph vertex list which we need to iterate
 * @param max_threads max number of threads that will can access/create the snapshot object
 * @return  ** SnapCollector 
 */
SnapCollector * takeSnapshot(Vnode * graph_head ,  int max_threads,fstream * logfile ,bool debug){
    SnapCollector *SC = acquireSnapCollector(graph_head , max_threads , logfile, debug);
    if(debug)
        (*logfile) << "Snapshot : " << SC << endl;
    
    SC-> iterator(logfile,debug);
    if(debug)
        (*logfile) << "Iterator Completed" << endl;
    
    SC->deactivate();
    if(debug)
        (*logfile) << "Deactivated Snapshot : " << SC  << " " << SC->isActive() << endl;
    
    SC->blockFurtherReports(logfile,debug);
    if(debug)
        (*logfile) << "Reports Blocked" << endl;
    SC->reconstructUsingReports(logfile,debug);
    if(debug)
        (*logfile) << "Reconstruction Completed" << endl;

   
    return SC;
}
      


      



/**
 * @brief This method is called by the graph operations to add vertex report incase of insertion or deletion 
 * of vertex

 * @param victim 
 * @param tid 
 * @param action insert->2/delete->1/block->3
 */
void reportVertex(Vnode *victim,int tid, int action, fstream * logfile, bool debug){
    if(debug)
        (*logfile) << "Report vertex : " << victim->val <<" action " << action<< endl;   

    SnapCollector * SC = PSC;
    if(SC != nullptr and SC->isActive()) {
        if(action == 2 && is_marked_ref((long) victim->vnext.load()))
            return;
        
        
        VertexReport *vreport_head = SC->reports[tid]->head_vertex_report;
        VertexReport *rep = new VertexReport(victim, action, vreport_head);
        
        if (vreport_head != nullptr && vreport_head->action == 3){
           return;
        }
            
        
        if(atomic_compare_exchange_strong(&SC->reports[tid]->head_vertex_report, &vreport_head, rep) and debug)
            (*logfile) << "Added Vertex Report " << victim ->val <<" action " << action << endl;
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
void reportEdge(Enode *victim,Vnode *source_enode, int tid, int action, fstream *logfile , bool debug){
    SnapCollector * SC = PSC;
    if(debug)
        (*logfile) << "Report edge " << source_enode->val<<" " << victim->val << " action : " << action<<endl;
    if(SC != nullptr and SC->isActive()) {
        if(action == 2 && is_marked_ref((long) victim->enext.load()))
            return;
        
        EdgeReport *vreport_head = SC->reports[tid]->head_edge_report;
        EdgeReport *rep = new EdgeReport(victim,source_enode, 2, vreport_head);
        if (vreport_head != nullptr && vreport_head->action == 3){
            return;
        }
        if(atomic_compare_exchange_strong(&SC->reports[tid]->head_edge_report, &vreport_head, rep) and debug)
            (*logfile) << "Added Edge report " << source_enode->val<<" " << victim->val << " action : " << action<<endl;
    }
}

