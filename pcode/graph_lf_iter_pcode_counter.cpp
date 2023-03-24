



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
}


class VertexReport{
    VNode * vnode // here only value can be stored...used to sort the reports based on vertex or associated vertex in case of edge
    action                              //insert or Delete
    VertexReport * nextReport
}

class EdgeReport{
    ENode * enode
    VNode * source
    action                              //insert or Delete
    EdgeReport * nextReport
}

class Snap_Vnode{ 
    Vnode *vnode
    Snap_VNode* vnext
    Snap_Enode *head_Enode   //head of edge linked list
    edge_status ; // {IDLE,ACTIVE,DONE }
                    //Default : IDLE , ACTIVE : some thread is processing the edge list , DONE : Some thread has completed processing
                    // IDLE -> ACTIVE -> DONE
                     
    iter_edge_status; //same as above but used during iteration operation
}

class Snap_Enode{
    Snap_Enode *enext
    Enode *enode
}


class Graph {
    VNode head_Vnode;

    Operation AddVertex (k);
    Operation RemoveVertex(k );
    Operation ContainsVertex(k);
    Operation AddEdge(k, l);
    Operation ContainsEdge(k, l);
    Operation RemoveEdge(k, l);
    operation locV(v);
    procedure locE(v, k);
    procedure ConVPlus (k, l);
    procedure locC(v, k);
    procedure ConCPlus (k, l);
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
    
    if (ce .l = l && !isMrkd (u) && !isMrkd(v) && !isMrkd (ce)) then
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


operation locV(v, k){
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



procedure locE(v, k){
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
    active = false   //indicates if the snap collect field is currently active
    vertex_reports[]
    edge_reports[]
    reconstruct_done = false
    iter_vcounter = 0
    reconstruct_vcounter = 0
    head_Vnode //head of linked list of collected Vnodes

    operation ReportDelete(Node *victim, nodeType{E,V})
    operation ReportInsert(Node* victim ,nodeType{E,V})
    operation TakeSnapshot()
    operation AcquireSnapCollector()
    operation CollectSnapshot()
    operation ReconstructionUsingReports()
    operation ReconstructionUsingReports_parallel()
    operation Iterator() 
    operation Iterator_parallel() 
    operation Deactivate();
    operation BlockFurtherReports();
    
}


operation ReportDelete(Node *victim, nodeType{E,V})
    SC = (dereference) PSC
    If (SC.IsActive()) 
        if(nodeType = V)
            report = VertexReport(victim, DELETE)
            temp = VertexReports[tid]
            if(cas(VertexReports[tid], temp, report))
                temp->next = report
        else
            report = EdgeReport(victim, DELETE)
            temp = EdgeReports[tid]
            if(cas(EdgeReports[tid], temp, report))
                temp->next = report

operation ReportInsert(Node* victim ,nodeType{E,V})
    SC = (dereference) PSC
    if (SC.IsActive())
        if(nodeType == V)
            if (node is not marked) 
                report = VertexReport(victim, INSERT)
                temp = VertexReports[tid]
                    if(cas(VertexReports[tid], temp, report))
                        temp->next <- report

        else
            if (node is not marked )
                report = EdgeReport(victim, DELETE)
                temp = EdgeReports[tid]
                    if(cas(EdgeReports[tid], temp, report))
                        temp->next <- report


operation TakeSnapshot()
    SC = AcquireSnapCollector()
    CollectSnapshot(SC)
    ReconstructUsingReports(SC)

operation AcquireSnapCollector()
    SC = (dereference) PSC 
    if (SC is not NULL and SC.IsActive()) 
        return SC 
    newSC = NewSnapCollector() 
    CAS(PSC, SC, newSC) 
    newSC = (dereference) PSC 
    return newSC 

operation CollectSnapshot()
    SC.iterator()        
    SC.Deactivate()
    SC.BlockFurtherReports()
            
            

operation ReconstructionUsingReports()
    V[] = SC.read_collected_vnodes() 
    v_reports[] = SC.vertex_reports //returns sorted vreports

    a VNode N belongs to Snapshot iff : 
        ((N has a reference in V[]) OR
        (N has INSERT Report in v_reports[]))
            AND
        (N has no DELETE Report in v_reports[])

    e_reports[] = SC.read_ereports() //returns sorted edge reports
    foreach VNode N of the Snapshot :
        E[] = SC.read_collected_enodes(N) //returns edge nodes having N as source

        a ENode M belongs to Snapshot iff :
            ((M has a reference in E[]) OR
            (M has INSERT Report in e_reports[]))
                AND
            (M has no DELETE Report in e_reports[] )
                AND
            (The DESTINATION Vnode of M belongs to snapshot)





operation ReconstructionUsingReports_parallel()
    V[] = SC.read_collected_vnodes() 
    v_reports[] = SC.read_vreports //returns sorted vreports

    a VNode N belongs to Snapshot iff : 
        ((N has a reference in V[]) OR
        (N has INSERT Report in v_reports[]))
            AND
        (N does not have a DELETE Report in v_reports[] )


    curr_V = SC.head_Vnode
    e_reports[] = SC.read_ereports() //returns sorted edge reports
    //ist iteration
    while curr_V != NULL and !SC.reconstruct_done:
        
        if(CAS(curr_V.edge_status , IDLE , ACTIVE))
            E[] = SC.read_collected_enodes(V)

            a ENode M belongs to Snapshot iff : 
                ((M has a reference in E[]) OR
                (M has INSERT Report in e_reports[]))
                    AND
                (M has no DELETE Report in e_reports[] )
                    AND
                (The DESTINATION Vnode of M belongs to snapshot)
                
            CAS(curr_V.edge_status , ACTIVE , DONE)
        
        curr_V = curr_V->vnext

    //2nd Iteration

    curr_V = SC.head_Vnode
    while curr_V != NULL and !SC.reconstruct_done:
        if(curr_V.edge_status = ACTIVE))

            E[] = SC.read_collected_enodes(V)

            a ENode M belongs to Snapshot iff : 
                ((M has a reference in E[]) OR
                (M has INSERT Report in e_reports[]))
                    AND
                (M has no DELETE Report in e_reports[] )
                    AND
                (The DESTINATION Vnode of M belongs to snapshot)
            
            CAS(curr_V.edge_status , ACTIVE , DONE)

        curr_V = curr_V->vnext

    SC.reconstruct_done = true


operation Iterator()
    curr_v = head vertex of the graph
    
    while curr_v != NULL and SC.isActive() :
        if curr_v is not marked :
            SC.collectVnode(curr)
        curr_v = curr_v.vnext

    SC.BlockFurtherVnodes();

    foreach V of the collected Vnodes :
        curr_e = head of the edge list of V
        while curr_e != NULL :
            if curr_e is not marked : 
                SC.collectEnode(V, curr_e)
            curr_e = curr_e.next
        SC.BlockFurtherEnodes(V)
    






operation Iterator_parallel()
    curr_V = head vertex of the graph
    
    while curr_V != NULL and SC.isActive() :
        if curr_V is not marked :
            SC.collectVnode(curr_V)
        curr_V = curr_V.vnext

    SC.BlockFurtherVnodes();

    curr_V = SC.head_Vnode // head of the linked List
    V_index = 0
    //1st iteration
    while SC.isactive() :
	loc_counter = iter_vcounter++
	while curr_V != NULL and V_index < loc_counter 
	curr_V = curr_V->vnext
	V_index++
	if curr_V == NULL 
	  		
	    	
        if(CAS(curr_V.iter_edge_status , IDLE , ACTIVE))
            
            curr_E = curr_V.head_Enode //head of the Edge list
            while curr_E != NULL :
                if curr_E is not marked : 
                    SC.collectEnode(curr_V, curr_E)
                curr_E = currE.next
            SC.BlockFurtherEnodes(curr_V)

            CAS(curr_V.iter_edge_status , ACTIVE , DONE)
        
    
    //2nd Iteration
    curr_V = SC.head_Vnode
    while curr_V != NULL and SC.isactive():
        if(curr_V.iter_edge_status = ACTIVE)

            curr_E = curr_V.head_Enode //head of the Edge list
            while curr_E != NULL :
                if curr_E is not marked : 
                    SC.collectEnode(curr_V, curr_E)
                curr_E = currE.next
            SC.BlockFurtherEnodes(curr_V)

            CAS(curr_V.iter_edge_status , ACTIVE , DONE)
        
        curr_V = curr_V->vnext



operation BlockFurtherReports() 

    for i in range(MAX_THREADS) : 
        block_rep = EdgeReport(NULL, BLOCK)
        temp = EdgeReports[tid]
        if(!cas(EdgeReports[tid], temp, report))//will fail only once
            temp =  EdgeReports[tid]
            cas(EdgeReports[tid], temp, report)
        block_rep->next = temp

        
        lock_rep = VertexReport(NULL, BLOCK)
        temp = VertexReports[tid]
        if(!cas(VertexReports[tid], temp, report))//will fail only once
            temp =  VertexReports[tid]
            cas(VertexReports[tid], temp, report)
        report->next = temp



   
        
            




      

        

        


        
    

        







                
