import numpy as np
import pandas as pd
import sys
import os
import json
import datetime

RESULTS_DIR_NAME = "/home/yqy/simulator-DARC-E/results/"
TASK_FILE_NAME = "task_times.csv"

def get_job_complation_time(result_folder):
    dir_path = RESULTS_DIR_NAME + result_folder
    os.chdir(dir_path)
    count = 0
    for entry in os.scandir(dir_path):
        if entry.is_dir():
            count += 1
    job_info = [[0, 0, 0, 0, 0] for _ in range(10000)]
    i = 0
    data_num = 0
    job_num= 0
    while i < count:
        file_path = dir_path + "sim_server_" + str(i) +"/"+ TASK_FILE_NAME
        print(file_path)
        task_file = open(file_path,"r")
        next(task_file)      
        for line in task_file:
            data_num = data_num + 1
            data = line.strip().split(",")
            job_id = int(data[10])
            if job_info[job_id][4] == 0 and job_info[job_id][3] == 0 and job_info[job_id][2] == 0 and job_info[job_id][1] == 0:
                job_info[job_id] = [job_id, int(data[11]), int(data[12]), int(data[2]), int(data[13])]
                job_num += 1
                # print(job_id, int(data[11]), int(data[12]), int(data[2]), int(data[13]))
            else:
                job_info[job_id][3] += int(data[2])
                if job_info[job_id][4] < int(data[13]):
                    job_info[job_id][4] = int(data[13])

        task_file.close()
        i = i+1
    # print(job_info)
    print("data_num:",data_num)
    print("job_num:",job_num)
    print("job_num:",count)
    return job_info



if __name__ == "__main__":

    result_ordered_folder = "sim_cluster_simulation_23-07-11_11:49:24_reuse_cluster_simulation_23-07-11_11:35:52_tasks_info__ordered_cluster_random/"
    result_folder = "sim_cluster_simulation_23-07-11_11:44:13_reuse_cluster_simulation_23-07-11_11:35:52_tasks_info__cluster_random/"

    job_time = get_job_complation_time(result_folder)
    job_time_ordered = get_job_complation_time(result_ordered_folder)
    search_str = "reuse_cluster_simulation"
    if len(result_ordered_folder) > len(result_folder):
        start_index = result_ordered_folder.find(search_str)
        substring = result_ordered_folder[start_index + len(search_str):]
        str1 = search_str + substring
        new_str = str1[:-1]
    else:
        start_index = result_folder.find(search_str)
        substring = result_folder[start_index + len(search_str):]
        str1 = search_str + substring
        new_str = str1[:-1]
    analysis_result_path = "/home/yqy/simulator-DARC-E/analysis_consistent_result/"+ datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S")+new_str+".csv"
    job_file = open(analysis_result_path,"w")
    # Write task information（需要修改只写入成功完成的任务）
    header = ["job_id","tasks_num_of_single_job","task_generation_time","total_time","complation_time","complation_time_ordered","shortened_completion_time"
              "job_id_order","tasks_num_of_single_job_order","task_generation_time_order","total_time_order"]
    job_file.write(','.join(header) + "\n")
    i = 0
    while i<10000: 
        stats = [job_time[i][0],job_time[i][1],job_time[i][2],job_time[i][3],job_time[i][4],
                job_time_ordered[i][4],job_time[i][4]-job_time_ordered[i][4],job_time_ordered[i][0],job_time_ordered[i][1],job_time_ordered[i][2],job_time_ordered[i][3],]
        stats = [str(x) for x in stats]
        job_file.write(','.join(stats) + "\n")
        i = i+1
    job_file.close()
    print("record tasks complete !")
    
    
