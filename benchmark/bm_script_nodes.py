import os
import sys
import shlex
import subprocess as sp
from subprocess import Popen
from datetime import datetime
from csv import writer
import random

now = datetime.now() # current date and time

date_time_obj =now.strftime('%H_%M_%S') 


threads = 104 
#######
#threads = 8
#######
algos = [ "report_resp","icdcn_resp" , "opodis_resp" ]

debug = False
main_file = "main.cpp"
iterations = 6
test_duration = "10" #no of sec before stop executions
init_vertices_map = { 
    6000 : "../input/datasets/p2p-Gnutella06",
    10000 : "../input/datasets/p2p-Gnutella10",
    22000 : "../input/datasets/p2p-Gnutella22",                    
    26000 : "../input/datasets/p2p-Gnutella26",              
    36000 : "../input/datasets/p2p-Gnutella36"
}
#######
#init_vertices = [10**4 * i for i in range(4,7)]
#######
#init_edges = [2 * i for i in init_vertices]

#files
maxt_output_file_fmt = '../output/{0}_op_' + date_time_obj +"_maxt_{1}" +  '.csv'
avgt_output_file_fmt = '../output/{0}_op_' + date_time_obj +"_avgt_{1}" + '.csv'
script_log_file = "../script_log/" + date_time_obj + ".txt"

#commands
cmd1 = "g++ -std=c++11 -pthread -O3 -o ../sources/{0}/a.out ../sources/{0}/" + main_file
cmd2 = "../sources/{0}/a.out ../log/{1} {2} {3} {4} {5} {6}"


# * 0->add vertex
# * 1->delete vertex
# * 2->add edge           
# * 3->delete edge
# * 4->contains edge
# * 5->contains vertex
# * 6->snapshot

dist_probs ={ 
            "loopup_int" : [3,2,3,2,45,45,0],
            "update_int" : [13,12,13,12,25,25,0]
            #"update_int_2" : [16,9,16,9,25,25,0],
            #"lookup_int_2" : [4,1,4,1,45,45,0]
            }

with open(script_log_file, 'w+') as log_f_object:
    for key in dist_probs.keys(): 
        print("\n\n\n\n\n\nProbablity Dist: "+ key +" " + str( dist_probs[key])  ,file = log_f_object,flush = True)
        
        for i in range(2,11,2):
            dist_prob = dist_probs[key].copy()
            print("\n\nSnapshot Dist: "+str(i)  ,file = log_f_object,flush = True)
            if(i != 0):
                dist_prob[6] = i
                dist_prob[4] -= i//2
                dist_prob[5] -= i//2
            
            maxt_output_file = maxt_output_file_fmt.format(key , "ss_" + str(i))
            avgt_output_file = avgt_output_file_fmt.format(key , "ss_" + str(i))
            with open( maxt_output_file, 'w+') as f_object:
                lst = ["Init Nodes" ]
                for algo in algos:
                    lst.append(algo)
                
                writer_object = writer(f_object)

                writer_object.writerow(lst)

            with open( avgt_output_file, 'w+') as f_object:
                lst = ["Init Nodes"  ]
                for algo in algos:
                    lst.append(algo)
                writer_object = writer(f_object)

                writer_object.writerow(lst)
            
            

            
            
            with open(maxt_output_file, 'a+') as f_object_max:
                with open(avgt_output_file, 'a+') as f_object_avg:
                    writer_object_max = writer(f_object_max)
                    writer_object_avg = writer(f_object_avg)

                    
                    
                    for algo in algos:
                        cmd = cmd1.format(algo)
                        proc = sp.Popen(cmd.split())
                        proc.wait()

                    number_of_triangles_parr = 0
                    for init_node_cnt in sorted(init_vertices_map.keys()):
                        init_edge_cnt = 2 * init_node_cnt
                        init_file = init_vertices_map[init_node_cnt]
                        max_lst = [init_file]
                        avg_lst = [init_file]
                        print("Init node cnt : " + str(init_node_cnt),file = log_f_object,flush = True)
                        
                        for algo in algos:
                            cmd = cmd2
                            for prob in dist_prob:
                                cmd += " " + str(prob)
                            
                            if debug:
                                cmd += " debug"
                            print("Algo : "+ algo,file = log_f_object,flush = True)
                            cmd = cmd.format(algo,date_time_obj,str(threads),test_duration,str(init_node_cnt),str(init_edge_cnt), init_file)
                            avg_time_taken_list = []
                            max_time_taken_list = []

                            #update parallel iterations
                            for i in range(iterations):
                                print("Iteration : "+ str(i),file = log_f_object,flush = True)
                                print(cmd)
                                proc = sp.Popen(cmd.split() ,stdout=sp.PIPE)
                                proc.wait()
                                std_output, std_err = proc.communicate()
                                print(std_output)
                                if std_err is not None:
                                    raise Exception(std_err)
                                avg_time , max_time = "",""
                                if len(std_output.decode().split()) > 1:
                                    avg_time, max_time, _ = std_output.decode().split('\n')
                                if not avg_time:
                                    lst.append("")#empty for that thread
                                    print("No o/p for init node cnt : " + str(init_node_cnt) + " and  Iteration :  " + str(i),file = log_f_object,flush = True)
                                    continue
                                print("Average Snapshot: " + str(avg_time),file = log_f_object,flush = True)
                                avg_time_taken_list.append(float(avg_time))

                                if not max_time:
                                    lst.append("")
                                    print("No max_time in o/p for init node cnt : " + str(init_node_cnt)+ " and  Iteration : " + str(i),file = log_f_object,flush = True)
                                    continue
                                print("Max Snapshot : " + str(max_time),file = log_f_object,flush = True)
                                max_time_taken_list.append(float(max_time))
                                print("\n",file= log_f_object,flush = True)
                                print(file= log_f_object,flush = True)
                                print(file= log_f_object,flush = True)
                                
                            avg_time_taken_mean = 0
                            if len(avg_time_taken_list)!=0:
                                if(len(avg_time_taken_list)) > 5:
                                    avg_time_taken_list = avg_time_taken_list[2:]
                                avg_time_taken_mean = int(sum(avg_time_taken_list)/len(avg_time_taken_list))
                            avg_lst.append(avg_time_taken_mean)
                            max_time_taken_mean = 0
                            if len(avg_time_taken_list)!=0:
                                if(len(max_time_taken_list)) > 5:
                                    max_time_taken_list = max_time_taken_list[2:]
                                max_time_taken_mean = int(sum(max_time_taken_list)/len(max_time_taken_list))
                            max_lst.append(max_time_taken_mean)
                            print()
                        print("\n\n",file= log_f_object,flush = True)
                        writer_object_max.writerow(max_lst)
                        writer_object_avg.writerow(avg_lst)
                        print(file= log_f_object,flush = True)








