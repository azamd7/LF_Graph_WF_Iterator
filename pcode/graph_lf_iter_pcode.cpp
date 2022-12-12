

####################################
// check start from here
####################################

// Graph Structure

class VNode {
    int k ;
    VNode vnxt;
    ENode ehead;

}


class ENode {
    int l ;
    VNode ptv;
    ENode enxt;
    VNode from_node;
}


class VertexReport{
    ENode * enode
    VNode * vnode // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
    action                              //insert or Delete
    VertexReport * nextReport
}

class EdgeReport{
    ENode * enode
    VNode * from // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
    action                              //insert or Delete
    EdgeReport * nextReport
}

class Snap_Vnode{ 
    Vnode *vnode
    Snap_VNode* vnext
    Snap_Enode *ehead   //head of edge linked list

    Snap_Vnode(vnode){
        this->vnode = vnode
        this->vnext := end_snap_VNode
        start_snap_Enode := Snap_Enode(vnode->ehead)
        this->ehead := start_snap_Enode
    }
}

class Snap_Enode{
    Snap_Enode *enext
    Enode *enode

    Snap_Enode(enode)
    {
        this->enode = enode
        this->enext = end_snap_Enode

    }
}


Operation AddVertex (k){
    while (true) do
            ⟨pv, cv⟩ ← locV (vh, k);
        if (cv .k = k ) then
            ReportInsert(cv,V) 
            return false;
        else
            nv ← CVnode (k); 
            nv .vnxt ← cv;
            if (CAS(pv .vnxt, cv, nv)) then
                ReportInsert(n,V)
                return true;
            end if
        end if
    end while
}


Operation RemoveVertex(k ){
    while (true) do
        ⟨pv, cv⟩ ← locV (vh, k);
        
        if (cv .k != k ) then
            return false;
        end if
    
        cn ← cv .vnxt;
        if (!isMrkd (cn)) then
            if (CAS(cv .vnxt, cn, MrkdRf (cn))) then
                ReportDelete(cv,V)
                if (CAS(pv .vnxt, cv, cn)) then
                    break;
                end if
            end if
        end if
    end while
    return true;
}


Operation ContainsVertex(k){
    cv ← vh.vnxt;
    while (cv .k < k ) do
        cv ← UnMrkdRf(cv .vnxt);
    end while
    if (cv .k = k and !isMrkd(cv)) then
        ReportInsert(cv,V)
        return true;
    else 
        if isMrkd(cv)
            ReportDelete(cv,V)
        return false;
    end if
}


Operation AddEdge(k, l){
    ⟨ u, v, st ⟩ ← ConVPlus(k, l);
    if (st = false) then
        return “VERTEX NOT PRESENT” ;
    end if
    
    while (true) do
        if (isMrkd (u) || isMrkd (v)) then
            ReportDelete(u||v, V)
            return “VERTEX NOT PRESENT”;
        end if
        ⟨pe, ce⟩ ← locE (u.enxt, l);
        if (ce .l = l) then
            ReportInsert(ce, E) //check if edge is marked before deletion
            return “EDGE PRESENT”;
        end if
        ne ← CEnode (l);
        ne.enxt ← ce;   
        ne.ptv ← v;
        if (CAS(pe .enxt, ce, ne)) then
            ReportInsert(ne,E)
            return “EDGE ADDED”;
        end if
    end while
}


Operation ContainsEdge(k, l){
    ⟨ u, v, st ⟩ ← ConCPlus(k, l);
    if (st = false) then
        return “VERTEX NOT PRESENT”;
    end if
    
    ce ← u.enxt;
    
    while (ce .l < l) do
        ce ← UnMrkdRf (ce .enxt);
    end while
    
    if (ce .l = l && !isMrkd (u) && !isMrkd(v) && isMrkd (ce)) then
        ReportInsert(ce , E )
        return “EDGE FOUND” ;
    else
        if isMrkd (u)
            ReportDelete(u , V)
        if isMrkd (v)
            ReportDelete(v , V)
        if isMrkd (ce)
            ReportDelete(ce , E)
        return “VERTEX OR EDGE NOT PRESENT”;
    end if
}


Operation RemoveEdge(k, l){
    ⟨ u, v, st ⟩ ← ConVPlus(k, l);
    if (st = false) then    
        return “VERTEX NOT PRESENT”;
    end if
    
    while (true) do
        if ( isMrkd(u) || isMrkd(v) ) then
            ReportDelete(u||v, V)
            return “VERTEX NOT PRESENT”;
        end if
        ⟨pe, ce⟩ ← locE (u.enxt, l);
        if (ce .l != l) then
            ReportDelete(ce, E)
            return “EDGE NOT PRESENT”;
        end if
        
        cnt ← ce .enxt;
        
        if (!isMrkd (cnt)) then
            if (CAS(ce.enxt, cnt, MrkdRf(cnt))) then
                ReportDelete(ce, E)
                if (CAS(pe .enxt, ce, cnt)) then 
                    break;
                end if
            end if
        end if
    end while
    return “EDGE REMOVED”;
}

procedure locV(v, k){
    while (true) do
        pv ← v; cv ← pv.vnxt;
        while (true) do
            cn ← cv.vnxt;
            while (isMrkd (cn)) && (cv.k < k)) do
                ReportDelete(cv, V)
                if (!CAS(pv.vnxt, cv, UnMrkdRf(cv.vnxt))) then
                    goto 102;
                end if
                cv ← cn; cn ← cv.vnxt;
            end while
        
            if (cv.k ≥ k) then
                return ⟨pv, cv⟩;
            end if
            pv ← cv; cv ← cn;
        end while
    end while
}

####################################
// check end
####################################

procedure locE(, k){
    while (true) do
        pe <- v ; ce <- pe.enxt;
        while (true) do
            cnt <- ce.enxt; VNode vn <- ce.ptv;
            while (isMrkd (vn) or ¬ isMrkd (cnt)) do
                DeleteReport(ce, E)
                if (¬CAS(ce.enxt, cnt, MrkdRf (cnt))) then
                    goto Line 119;
                end if
                DeleteReport(ce, E)
                if (¬CAS(pe.enxt, ce, cnt)) then 
                    goto Line 119;
                end if
                ce <- cnt; n <- ce.ptv;
                cnt <- UnMrkdRf(ce.enxt);
            end while
            while (isMrkd (cnt)) do
                DeleteReport(ce, E)
                if (¬ CAS(pe .enxt, ce,cnt)); then goto 119;
                end if
                ce <- cnt; n <- ce.ptv;
                cnt <- UnMrkdRf(ce .enxt);
            end while
            if (isMrkd(vn)) then goto Line 123;
            end if
            if (ce.l >= k) then return (pe, ce)
            end if
            pe <- ce; ce <- cnt;
        end while
    end while
}


procedure ConVPlus (k, l){
    if (k < l) then
        (pv1, cv1) <- locV(vh, k);
        if (cv1.k != k) then
            return (NULL, NULL, false);
        end if
        (pv2, cv2) <- locV(c1, l);
        if (cv2.k != l) then
            return (NULL, NULL, false);
        end if
    else
        (pv2, cv2) <- locV(vh, l);
        if (cv2.k != l) then
            return (NULL, NULL, false);
        end if
        (pv1, cv1) <- locV(cv2, k);
        if (cv1.k != k) then
            return (NULL, NULL, false) ;
        end if
    end if
    returns (cv1, cv2, true);
}


procedure locC(v, k){
    pv <- v ; cv <- p.vnxt;
    while (true) do
        if (cv.k >= k) then
            return (p, c);
        end if
            pv <- cv ; cv <- UnMrkdRf(cv.vnxt);
        end while
}


procedure ConCPlus (k, l){
    if (k < l) then
        ⟨pv1, cv1⟩ ← locC(vh, k);
        if (cv1.k != k) then
            return ⟨NULL, NULL, false⟩;
        end if
    
        ⟨pv2, cv2⟩ ← locC(cv1, l);
    
        if (cv2.k != 2) then
            return ⟨NULL, NULL, false⟩;
        end if

    else
        ⟨pv2, cv2⟩ ← locC(vh, l);
        if (cv2.k != l) then
            return ⟨NULL, NULL, false⟩;
        end if
        
        ⟨pv1, cv1⟩ ← locC(cv2,k);
        if (cv1.k != k) then
            return ⟨NULL, NULL, false⟩ ;
        end if
    end if
    returns ⟨cv1, cv2, true⟩;
}








class SnapCollector
{
    active <- false   //indicates if the snap collect field is currently active
    Snap_Vnode * head_snap_VNode  
    Snap_Vnode * snap_tail_V_ptr
    Snap_Enode * snap_tail_E_ptr


    read_edge // boolean value to indicate if that we are going through the edge
    

    //for reconstruction using report
    vertex_reports[]
    sorted_vertex_report[]
    vertex_report_index //atomic int
    delete_vertex_report //This will be used to check the while adding edges 
    report_snap_vertex_ptr //used while iterating through the edge reports

    edge_reports[]//for each thread there is a linked list maintained
    sorted_edge_report[]
    edge_report_index //atomic int


    SnapCollector(){
        Snap_Vnode * start_snap_VNode(start_VNode)
        Snap_Vnode * end_snap_Vnode(end_Vnode)
        start_snap_VNode->next := end_snap_VNode
        head_snap_Vnode := start_snap_VNode
        tail_snap_Vnode := start_snap_VNode
        tail_snap_Enode := Null
        snap_vertex_ptr := head_snap_VNode
        sorted_vertex_report := Null
        report_snap_vertex_ptr := start_snap_VNode

        
        //initialize vertex reports
        //initialize edge reports
        //Note : 
        // start_snap_VNode / end_snap_Vnode indicates the start and end of vertex list 
        // start_snap_ENode / end_snap_Enode indicates the start and end of edge list 
        // tail_snap_VNode points to vertex which was last updated
        // tail_snap_ENode points to vertex which was last updated
        // snap_vertex_ptr is the vertex which we currently iterating while adding edges

    }
    
    

    iterator(){
        

        while not read_edge
            temp_tail_snap_Vnode  := tail_snap_Vnode
            from_Vnode := temp_tail_snap_Vnode->vnode
            next_Vnode := fetch the next unmarked vertex from from_vnode
            
            if next_Vnode is end_Vnode //reaches the end of the vertex list in original graph
                while CAS(temp_tail_snap_Vnode->vnext , end_snap_Vnode ,marked(end_snap_VNode))
                    temp_tail_snap_Vnode := tail_snap_VNode
                read_edge = True
                break
            endif


            //create a new snap vertex vertex Node
            snap_Vnode := Snap_VNode(next_Vnode)
            
            //this would fail if some other node is added
            if(CAS(temp_snap_tail_Vnode->vnext , end_snap_VNode, snap_Vnode))
                CAS(tail_snap_Vnode, temp_snap_tail_Vnode, snap_Vnode) 
            else 
                if temp_snap_tail_Vnode->vnext is marked(end_snap_VNode)//no more vertex can be added
                    read_edge = true
                    break
                endif

                CAS(tail_snap_Vnode, temp_snap_tail_Vnode, temp_snap_tail_Vnode->vnext) //helping : new edge has been added but vertex tail ptr if not updated
            endif
        

        //iterate through the edge
        while read_edge
            
            temp_snap_vertex_ptr := snap_vertex_ptr // used to identify current vertex we are iterating can be stored locally
            temp_tail_snap_Enode := tail_snap_Enode
            
            //temp_tail_E can be prev vertex tail_E in which case its next is marked(end_E_node) 
            //or some edge which belongs to current temp_snap_vertex_ptr or after that

            if temp_tail_snap_Enode is NUll //no edge has been added yet or tail_E has not been updated
                //Note :temp_snap_vertex_ptr can only be start node or the first vertex or else tail_E would not be null
                //      or if there are no edges for some vertex in that case it will be at vertex with an edge
                if temp_tail_snap_Vnode is start_snap_VNode :
                    CAS(snap_vertex_ptr , temp_tail_snap_Vnode , temp_tail_snap_Vnode->vnext)
                    temp_tail_snap_Vnode := snap_vertex_ptr
                
                start_snap_Enode = temp_tail_snap_Vnode->head //start of the edgelist
                CAS(tail_snap_ENode , NULL , start_snap_Enode)
            else
                next_enode := temp_tail_snap_Enode -> enode

                //next_enode := fetch next edge which is not end_E_node and not marked
                while next_enode is not end_Enode or edge is marked or edge TO vertex is marked
                    next_enode := next_enode ->enext
                
                if next_enode is end_enode :
                    
                    while(CAS(temp_tail_snap_Enode->enext , end_snap_ENode , marked(end_snap_ENode)  //either some thread has updated to marked(end_snap_enode) or added another edge
                                                                                                    // temp_tail_E->next is not marked(end_snap_enode))                         
                        temp_tail_snap_Enode := temp_tail_snap_Enode->next
                
                    if temp_snap_vertex_ptr ->next is end_snap_Vnode 
                        break
                    CAS(snap_vertex_ptr,temp_snap_vertex_ptr , temp_snap_vertex_ptr->next)
                    temp_snap_vertex_ptr := snap_vertex_ptr
                    CAS(tail_snap_Enode , temp_tail_snap_Enode , temp_snap_vertex_ptr->ehead)
                
                else //
                    snap_Enode := Snap_Enode(next_enode)


                    //add the  next node to snap
                    if (CAS(tail_snap_Enode->next , end_snap_ENode ,snap_Enode ))
                        CAS(tail_snap_Enode , temp_tail_snap_Enode , snap_Enode)
                    else
                        CAS(tail_snap_Enode , temp_tail_snap_Enode , temp_tail_snap_Enode->enext) //helping


    }


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

    ReportDeleteEdge(Node *victim,Vnode from)
        SC = (dereference) PSC
        If (SC.IsActive()) 
            addReport(Report(victim, DELETED,from),tid)

    ReportInsertEdge(Node* node,Vnode from)
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
        while(CAS(temp_tail_V->next , sentinelVNode , marked(sentinelVNode)))
            temp_tail_V := temp_tail_V->next
        
        //in case of edge we need to do update tail_E because temp_tail_E could be pointing to the last node
        // in the edge list in case tail_E could change to some other edgelist edge and cas may succeed
        temp_tail_E := tail_E
        while(CAS(tail_E , temp_tail_E , sentinelENode))
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

            while next_snap_enode is not end_snap_ENode and next_snap_enode.val < report->enode.val
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
                
                



                
