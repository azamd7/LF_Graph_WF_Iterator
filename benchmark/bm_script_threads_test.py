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


threads = [13,26,52,78,104] 
#threads = [4,8]
algos = [ "report_DIA_tt_ss_update_test" , "icdcn_DIA_tt_ss_test" , "opodis_DIA_tt_ss_test" ]
#algos = ["report_DIA_tt_ss_update"]
debug = False
main_file = "main.cpp"
iterations = 6
test_duration = "10" #no of sec before stop executions
init_vertices = str(10**4)
init_edges = str(2 * (10**4))


#files
maxt_output_file_fmt = '../output/{0}_op_' + date_time_obj +"_maxt_{1}" +  '.csv'
avgt_output_file_fmt = '../output/{0}_op_' + date_time_obj +"_avgt_{1}" + '.csv'
script_log_file = "../script_log/" + date_time_obj + ".txt"
input_file = "../input/datasets/synth_10k_20k"

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
            }

with open(script_log_file, 'w+') as log_f_object:
    for key in dist_probs.keys(): 
        print("\n\n\n\n\n\nProbablity Dist: "+ key +" " + str( dist_probs[key])  ,file = log_f_object,flush = True)
        
        #for i in range(2,11,2):
        for i in [2,6,10]:
            dist_prob = dist_probs[key].copy()
            print("\n\nSnapshot Dist: "+str(i)  ,file = log_f_object,flush = True)
            if(i != 0):
                dist_prob[6] = i
                dist_prob[4] -= i//2
                dist_prob[5] -= i//2
            
            ss_maxt_output_file = maxt_output_file_fmt.format(key , "ss_" + str(i))
            ss_avgt_output_file = avgt_output_file_fmt.format(key , "ss_" + str(i))
            op_maxt_output_file = maxt_output_file_fmt.format(key , "op_" + str(i))
            op_avgt_output_file = avgt_output_file_fmt.format(key , "op_" + str(i))
            total_maxt_output_file = maxt_output_file_fmt.format(key , "total_" + str(i))
            total_avgt_output_file = avgt_output_file_fmt.format(key , "total_" + str(i))
            with open( ss_maxt_output_file, 'w+') as f_object:
                lst = ["Threads" ]
                for algo in algos:
                    lst.append(algo)
                
                writer_object = writer(f_object)

                writer_object.writerow(lst)

            with open( ss_avgt_output_file, 'w+') as f_object:
                lst = ["Threads"  ]
                for algo in algos:
                    lst.append(algo)
                writer_object = writer(f_object)

                writer_object.writerow(lst)
            
            with open( op_maxt_output_file, 'w+') as f_object:
                lst = ["Threads" ]
                for algo in algos:
                    lst.append(algo)
                
                writer_object = writer(f_object)

                writer_object.writerow(lst)

            with open( op_avgt_output_file, 'w+') as f_object:
                lst = ["Threads"  ]
                for algo in algos:
                    lst.append(algo)
                writer_object = writer(f_object)

                writer_object.writerow(lst)
            
            with open( total_maxt_output_file, 'w+') as f_object:
                lst = ["Threads" ]
                for algo in algos:
                    lst.append(algo)
                
                writer_object = writer(f_object)

                writer_object.writerow(lst)

            with open( total_avgt_output_file, 'w+') as f_object:
                lst = ["Threads"  ]
                for algo in algos:
                    lst.append(algo)
                writer_object = writer(f_object)

                writer_object.writerow(lst)
            
            

            with open(ss_maxt_output_file, 'a+') as ss_f_object_max:
                with open(ss_avgt_output_file, 'a+') as ss_f_object_avg:
                    with open(op_maxt_output_file, 'a+') as op_f_object_max:
                        with open(op_avgt_output_file, 'a+') as op_f_object_avg:
                            with open(total_maxt_output_file, 'a+') as total_f_object_max:
                                with open(total_avgt_output_file, 'a+') as total_f_object_avg:
                                    ss_writer_object_max = writer(ss_f_object_max)
                                    ss_writer_object_avg = writer(ss_f_object_avg)
                                    op_writer_object_max = writer(op_f_object_max)
                                    op_writer_object_avg = writer(op_f_object_avg)
                                    total_writer_object_max = writer(total_f_object_max)
                                    total_writer_object_avg = writer(total_f_object_avg)

                                    
                                    
                                    for algo in algos:
                                        cmd = cmd1.format(algo)
                                        proc = sp.Popen(cmd.split())
                                        proc.wait()

                                    number_of_triangles_parr = 0
                                    for thread_cnt in threads :
                                        ss_max_lst = [thread_cnt]
                                        ss_avg_lst = [thread_cnt]
                                        op_max_lst = [thread_cnt]
                                        op_avg_lst = [thread_cnt]
                                        total_max_lst = [thread_cnt]
                                        total_avg_lst = [thread_cnt]
                                        
                                        print("Thread cnt : " + str(thread_cnt),file = log_f_object,flush = True)
                                        
                                        for algo in algos:
                                            cmd = cmd2
                                            for prob in dist_prob:
                                                cmd += " " + str(prob)
                                            
                                            if debug:
                                                cmd += " debug"
                                            print("Algo : "+ algo,file = log_f_object,flush = True)
                                            cmd = cmd.format(algo,date_time_obj,str(thread_cnt),test_duration,init_vertices,init_edges, input_file)
                                            ss_avg_time_taken_list = []
                                            ss_max_time_taken_list = []
                                            op_avg_time_taken_list = []
                                            op_max_time_taken_list = []
                                            total_avg_time_taken_list = []
                                            total_max_time_taken_list = []
                                            

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
                                                    ss_avg_time, ss_max_time,op_avg_time,op_max_time,total_avg_time,total_max_time,_ = std_output.decode().split('\n')
                                                
                                                print("Average Snapshot: " + str(ss_avg_time),file = log_f_object,flush = True)
                                                ss_avg_time_taken_list.append(float(ss_avg_time))

                                                print("Max Snapshot : " + str(ss_max_time),file = log_f_object,flush = True)
                                                ss_max_time_taken_list.append(float(ss_max_time))
                                                
                                                print("Average Op: " + str(op_avg_time),file = log_f_object,flush = True)
                                                op_avg_time_taken_list.append(float(op_avg_time))

                                                print("Max OP : " + str(op_max_time),file = log_f_object,flush = True)
                                                op_max_time_taken_list.append(float(op_max_time))
                                                
                                                print("Average total: " + str(total_avg_time),file = log_f_object,flush = True)
                                                total_avg_time_taken_list.append(float(total_avg_time))

                                                print("Max total: " + str(total_max_time),file = log_f_object,flush = True)
                                                total_max_time_taken_list.append(float(total_max_time))
                                                print("\n",file= log_f_object,flush = True)
                                                print(file= log_f_object,flush = True)
                                                print(file= log_f_object,flush = True)
                                                
                                            avg_time_taken_mean = 0
                                            if len(ss_avg_time_taken_list)!=0:
                                                if(len(ss_avg_time_taken_list)) > 1:
                                                    ss_avg_time_taken_list = ss_avg_time_taken_list[2:]
                                                avg_time_taken_mean = int(sum(ss_avg_time_taken_list)/len(ss_avg_time_taken_list))
                                            ss_avg_lst.append(avg_time_taken_mean)
                                            max_time_taken_mean = 0
                                            if len(ss_max_time_taken_list)!=0:
                                                if(len(ss_max_time_taken_list)) > 1:
                                                    ss_max_time_taken_list = ss_max_time_taken_list[2:]
                                                max_time_taken_mean = int(sum(ss_max_time_taken_list)/len(ss_max_time_taken_list))
                                            ss_max_lst.append(max_time_taken_mean)
                                            
                                            avg_time_taken_mean = 0
                                            if len(op_avg_time_taken_list)!=0:
                                                if(len(op_avg_time_taken_list)) > 1:
                                                    op_avg_time_taken_list = op_avg_time_taken_list[2:]
                                                avg_time_taken_mean = int(sum(op_avg_time_taken_list)/len(op_avg_time_taken_list))
                                            op_avg_lst.append(avg_time_taken_mean)
                                            max_time_taken_mean = 0
                                            if len(op_max_time_taken_list)!=0:
                                                if(len(op_max_time_taken_list)) > 1:
                                                    op_max_time_taken_list = op_max_time_taken_list[2:]
                                                max_time_taken_mean = int(sum(op_max_time_taken_list)/len(op_max_time_taken_list))
                                            op_max_lst.append(max_time_taken_mean)
                                            
                                            avg_time_taken_mean = 0
                                            if len(total_avg_time_taken_list)!=0:
                                                if(len(total_avg_time_taken_list)) > 1:
                                                    total_avg_time_taken_list = total_avg_time_taken_list[2:]
                                                avg_time_taken_mean = int(sum(total_avg_time_taken_list)/len(total_avg_time_taken_list))
                                            total_avg_lst.append(avg_time_taken_mean)
                                            max_time_taken_mean = 0
                                            if len(total_max_time_taken_list)!=0:
                                                if(len(total_max_time_taken_list)) > 1:
                                                    total_max_time_taken_list = total_max_time_taken_list[2:]
                                                max_time_taken_mean = int(sum(total_max_time_taken_list)/len(total_max_time_taken_list))
                                            total_max_lst.append(max_time_taken_mean)
                                            print()
                                        print("\n\n",file= log_f_object,flush = True)
                                        ss_writer_object_max.writerow(ss_max_lst)
                                        ss_writer_object_avg.writerow(ss_avg_lst)
                                        op_writer_object_max.writerow(op_max_lst)
                                        op_writer_object_avg.writerow(op_avg_lst)
                                        total_writer_object_max.writerow(total_max_lst)
                                        total_writer_object_avg.writerow(total_avg_lst)
                                        print(file= log_f_object,flush = True)








