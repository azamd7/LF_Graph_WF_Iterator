
#include "graphDS.h"
#include <vector>
#include "utility.h"
#include <atomic>

using namespace std;
// Vertex's Report Structure
class VertexReport {
    public:
        Enode * enode;
        Vnode * vnode; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        VertexReport * nextReport;
};

// Edge's Report Structure
class EdgeReport {
    public:
        Enode * enode;
        Vnode * source; // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
        int action; // 1-insert 2-Delete 3-Block
        EdgeReport * nextReport;
};



/*for each thread a report will be maintained which cantains a linked list of edge report and vertex report*/
class Report{
    public :
        EdgeReport *head_edge_report;
        VertexReport *head_vertex_report;
    
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
        atomic<Snap_Enode *> ehead; // head of edge linked list

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
        atomic<Report *> *heads_of_reports;//array of atomic report heads
        //vector <Report> delete_vertex_reports; //This will be used to check the while adding edges 
        bool read_edge = false; //used indicate that the threads are collecting the edges
    
     
    

        //for reconstruction using report
    
        vector<VertexReport> *sorted_vertex_report;
        vector<EdgeReport> *sorted_edge_report;
        atomic<int> vertex_report_index = {0}; //used to store the highest index in sorted vertex reports currently being processed by any thread
        atomic<int> edge_report_index ;//at


        //Here head points to the "start_vnode" of the original graph 
        SnapCollector(Vnode * head , int no_of_threads){
            Snap_Vnode * start_snap_Vnode = new Snap_Vnode(head, end_snap_Vnode );
            head_snap_Vnode = start_snap_Vnode;
            tail_snap_V_ptr = start_snap_Vnode;
            tail_snap_E_ptr = nullptr;
            sorted_vertex_report = NULL;

            heads_of_reports = new atomic<Report*>[no_of_threads];
            for( int i ; i < no_of_threads ;i++){
                heads_of_reports[i].store(new Report());
            }
         
        }

        //Note : 
        // start_snap_Vnode / end_snap_Vnode indicates the start and end of vertex list 
        // start_snap_Enode / end_snap_Enode indicates the start and end of edge list 
        // tail_snap_Vnode points to vertex which was last updated
        // tail_snap_Enode points to vertex which was last updated
        // snap_vertex_ptr is the vertex which we currently iterating while adding edges
    
   

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

    /*
    addReport(Report * report, int tid)
    {     
        temp = reports[tid]
        if(cas(reports[tid], temp, report))
            temp->next <- report
    }

    ReportDeleteVertex(Node *victim)
        SC = (dereference) PSC
        If (SC.IsActive()) 
           report = VertexReport(victim, DELETED)
           temp = Vertexeports[tid]
            if(cas(VertexReports[tid], temp, report))
                temp->next <- report

    ReportInsertVertex(Node* node)
        SC = (dereference) PSC
        if (SC.IsActive())
            if (node is not marked) #Case we insert and delete happened before the snapshot and then insert thread reads isActive after the snapshot starts
                addReport(Report(newNode, INSERt),tid)

    ReportDeleteVertex(Node *victim,Vnode from)
        SC = (dereference) PSC
        If (SC.IsActive()) 
            addReport(Report(victim, DELETED,from),tid)

    ReportInsertVertex(Node* node,Vnode from)
        SC = (dereference) PSC
        if (SC.IsActive())
            if (node is not marked) 
                addReport(report(newNode, INSERt,from),tid)


    



    TakeSnapshot()
        SC = AcquireSnapCollector()
        CollectSnapshot(SC)
        ReconstructUsingReports(SC)

    AcquireSnapCollector()
        SC = (dereference) PSC 
        if (SC is not NULL and SC.IsActive()) 
            return SC 
        newSC = NewSnapCollector() 
        CAS(PSC, SC, newSC) 
        newSC = (dereference) PSC 
        return newSC 
    

    //Note : Make sentinel node next as marked sentinel node
    SC.BlockFurtherNodes(){
        temp_tail_V = tail_V
        while(CAS(temp_tail_V->next , sentinelVnode , marked(sentinelVnode)))
            temp_tail_V := temp_tail_V->next
        
        //in case of edge we need to do update tail_E because temp_tail_E could be pointing to the last node
        // in the edge list in case tail_E could change to some other edgelist edge and cas may succeed
        temp_tail_E := tail_E
        while(CAS(tail_E , temp_tail_E , sentinelEnode))
            temp_tail_E := tail_E  //cant be next as it can be edge of other node
        
    }


    CollectSnapshot(SC)
        iterator()
        SC.BlockFurtherNodes()
        
        SC.Deactivate()
        
        SC.BlockFurtherReports()

    ReconstructUsingReports(SC)
        next_V := head_V
        Vertex_reports = SC.ReadVertexReports()
        if sorted_vertex_report is Null
            sorted_vertex_report := sort Vertex_reports based on value and address(First delete then insert report)
        else
            sorted_vertex_report := read(sorted_vertex_report)
        
        
        prev_snap_vnode = start_snap_Vnode 
        next_snap_vnode = start__snap_Vnode->next
        while vertex_report_index < size(sorted_edge_report)
            index := vertex_report_index
            prev_index := index

            report = sorted_vertex_report[index] 
            
            if report is insert :
                //No delete report as the reports are sorted by delete and then insert for same address and value
                while next_snap_vnode->vnode.val < report->vnode.val and next_snap_vnode is not end_snap_Vnode
                        prev_vnode := next_snap_vnode
                        next_snap_vnode := next_snap_vnode ->vnext
                
                if next_snap_vnode->vnode != report->vnode.val
                    snap_vnode(report->vnode)
                    snap_vnode->next = next_snap_vnode
                    CAS(prev_vnode->next , next_snap_vnode , snap_vnode)
                
            else 
                add report to Delete_vertext_reports

                //Note : there cant be 2 snap nodes with same value
                
                //goto vertex location
                while next_snap_vnode->vnode < report->vnode.val and not end_snap_Vnode
                        prev_vnode := next_snap_vnode
                        next_snap_vnode := next_snap_vnode ->vnext

                if next_snap_vnode->vnode == report->vnode 
                    CAS(prev_snap_vnode->next , next_snap_vnode , next_snap_vnode->next )
                    next_snap_vnode = next_snap_vnode ->next
                
                while report belongs to the same vertex address //insert or delete
                    index++
                    continue
                
                CAS(vertex_report_index , prev_index, index) //update vertex index

                
        edge_reports = SC.ReadVertexReports()
        if sorted_edge_report is Null
            sorted_edge_report := sort edge_reports based on from value and address(First delete then insert report)
        else
            sorted_vertex_report := read(sorted_vertex_report)

        

        
        //check if edge reports is empty
        report = edge_reports[index]    
        from_Vnode = report->from_Vnode

        index = edge_report_index //default value is 0
        
        while index < size(edge_reports)
            loc_snap_vertex_ptr := report_snap_vertex_ptr  
            
        
            prev_index = index

             //check if edge reports is empty
            report = edge_reports[index]    
            from_Vnode = report->from_Vnode

            //fetch the next vertex pointer
            while loc_snap_vertex_ptr is not end_snap_Vnode and loc_snap_vertex_ptr->vnode.val < from_Vnode.val 
                remove edge from edge list of loc_snap_vertex_ptr with TO vertex present in DELETE reports
                loc_snap_vertex_ptr := loc_snap_vertex_ptr->vnext

            if loc_snap_vertex_ptr is end_snap_Vnode //reached end of valid from vertex hence reconstruction completed
                break
            
            prev_snap_enode := loc_snap_vertex_ptr->ehead
            next_snap_enode := prev_snap_enode->enext

            while next_snap_enode is not end_snap_Enode and next_snap_enode.val < report->enode.val
                next_snap_enode := next_snap_enode.enext
        
            

            if insert report and  next_snap_enode.val != report->enode.val
                if report->enode->vnode is not in  delete report : //no delete report TO edge address and value
                    new snap_Enode(report->enode)
                    CAS(prev_snap_enode->next ,next_snap_enode ,snap_Enode)

            else//delete report
                if next_snap_enode.val = report->enode.val
                    CAS(prev_snap_enode->next ,next_snap_enode ,next_snap_enode->enext)
                
                while report for same address and value //ignore all report belonging to same TO address and value
                    index++
                    continue
            
            CAS(edge_report_index,prev_index,index)//update edge index
            index = edge_report_index 
                */

    

};
