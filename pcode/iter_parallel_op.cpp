operation Iterator_parallel()
    curr_v = head vertex of the graph
    
    while curr_v != NULL and SC.isActive() :
        if curr_v is not marked :
            SC.collectVnode(curr)
        curr_v = curr_v.vnext

    SC.BlockFurtherVnodes();

    head_vnode = SC.read_collected_vnodes()
    active_set = {}

    //1st iteration
    while !idle_set.isEmpty() and SC.isactive() :
        V = idle_set.remove()
        
        active_set.add(V)

        curr_e = head of the edge list of V
        while curr_e != NULL :
            if curr_e is not marked : 
                SC.collectEnode(V, curr_e)
            curr_e = currE.next
        SC.BlockFurtherEnodes(V)

        active_set.remove(V)



//operation ReconstructionUsingReports_parallel()
//    V[] = SC.read_collected_vnodes() 
//    v_reports[] = SC.read_vreports() //returns sorted vreports

//    a VNode N belongs to Snapshot iff : 
//        ((N has a reference in V[]) OR
//        (N has INSERT Report in v_reports[]))
//            AND
//        (N does not have a DELETE Report in v_reports[] )




//    idle_set = {VNodes belonging to Snapshot}    
//    active_set = {}
//    e_reports[] = SC.read_ereports() //returns sorted edge reports
//    //ist iteration
//    while !idle_set.isEmpty() and !SC.reconstruct_done:
//        N = idle_set.remove()
//        active_set.add(V)
//        E[] = SC.read_collected_enodes(N)

//        a ENode M belongs to Snapshot iff : 
//            ((M has a reference in E[]) OR
//            (M has INSERT Report in e_reports[]))
//                AND
//            (M has no DELETE Report in e_reports[] )
//                AND
//            (The DESTINATION Vnode of M belongs to snapshot)
        
//        active_set.remove(N)

//    //2nd Iteration

//    while !active_set.isEmpty() and !SC.reconstruct_done:
//        N = active_set.fetch_next()

//        E[] = SC.read_collected_enodes(V)

//        a ENode M belongs to Snapshot iff : 
//            ((M has a reference in E[]) OR
//            (M has INSERT Report in e_reports[]))
//                AND
//            (M has no DELETE Report in e_reports[] )
//                AND
//            (The DESTINATION Vnode of M belongs to snapshot)
        
//        active_set.remove(N)

//    SC.reconstruct_done = true
