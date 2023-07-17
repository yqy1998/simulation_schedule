#!/usr/bin/env python
"""Creates a runs a simulation."""

import logging
import random
import os
import json
import math
import sys
import datetime
import pathlib
import xlsxwriter as xw
import copy

from simulation_state import SimulationState
from sim_thread import Thread
from tasks import Task
import progress_bar as progress
from sim_config import SimConfig

SINGLE_THREAD_SIM_NAME_FORMAT = "{}_{}"
MULTI_THREAD_SIM_NAME_FORMAT = "{}_{}_t{}"
RESULTS_DIR = "{}/results/"
META_LOG_FILE = "{}/results/meta_log"
CONFIG_LOG_DIR = "{}/config_records/"

type_micro = 100
type_mini = 1000
type_high = 5000


"""定义type level"""
def def_type(service_time):
    if service_time <= type_mini:
        return "mini"
    elif service_time <= type_high:
        return "mid"
    return "high"

###########用于复现同一任务不用调度策略下的值################
def write_tasks_into_file(run_name,tasks):

    dir_path = "/home/yqy/simulation_schedule/simulator-DARC-E/task_record/" 
    os.chdir(dir_path)
    task_file   = open("{}_tasks_info.csv".format(run_name), "w")

    # Write task information（需要修改只写入成功完成的任务）
    header = ["job_id","tasks_num_of_single_job","task_generation_time","arrival_time",
              "service_time"]
    task_file.write(','.join(header) + "\n")
    for task in tasks:
            stats = [task[0],task[1],task[2],task[3],task[4]]
            stats = [str(x) for x in stats]
            task_file.write(','.join(stats) + "\n")
    task_file.close()
    print("record tasks complete !")

def read_tasks_from_file(file_path,tasks):
    task_file = open(file_path,"r")
    next(task_file)
    for line in task_file:
        data = line.strip().split(",")
        tasks.append([int(data[0]),int(data[1]),int(data[2]),int(data[3]),int(data[4])])
    task_file.close()
    print("download task file complete !")

def Generate_cluster_tasks(cfg,tasks_list,task_distribution_para,task_delay_para):
    #生成的时间符合：exponential distribution指数分布
    #泊松分布是单位时间内独立事件发生次数的概率分布，指数分布是独立事件的时间间隔的概率分布。
    #泊松过程的到达率λ可以看作是单位时间内随机事件的平均发生次数。它的值越大，单位时间内随机事件的发生次数就越多。
    #avg_load = (busy_time/(cores * stats["End Time"]))：每个核心上每一纳秒花在工作的比例
    #平均服务时间：按照双峰分布的任务平均处理时长，如果load_thread_count=1，
    # 则就是1/AVERAGE_SERVICE_TIME，AVERAGE_SERVICE_TIME越大，生成的下一个任务的时间间隔越长，
    # 总体就是让任务在服务时间的基础上一个跟着一个到来
    #request_rate适合一个服务器，集群可以通过设置一个时间点的任务个数来完成
    request_rate = cfg.avg_system_load * cfg.load_thread_count / cfg.AVERAGE_SERVICE_TIME
    next_task_time = int(1/request_rate) if cfg.regular_arrivals else int(random.expovariate(request_rate))
    if cfg.bimodal_service_time:
            distribution = [task_distribution_para[0][0]] * task_distribution_para[0][1] + [task_distribution_para[1][0]] * task_distribution_para[1][1] 
    #job的个数
    i = 0
    task_arrive_distribution = [task_delay_para[0][0]]*task_delay_para[0][1] + [task_delay_para[1][0]]*task_delay_para[1][1]
    while (cfg.sim_duration is None or next_task_time < cfg.sim_duration) and \
                (cfg.num_tasks is None or i < cfg.num_tasks):
        #单个作业包含的任务个数
        num_of_single_job = random.randint(80,90)
        task_i = 0
        while task_i < num_of_single_job:
            service_time = None
            while service_time is None or service_time == 0:
                if cfg.constant_service_time:
                    service_time = cfg.AVERAGE_SERVICE_TIME
                elif cfg.bimodal_service_time:
                    service_time = random.choice(distribution)
                else:
                    #任务在100到10000之间，任务服务时间更加的广泛
                    # service_time = int(min(max(random.expovariate(1 / config.AVERAGE_SERVICE_TIME), 100), 10000))
                    service_time = int(random.expovariate(1 / cfg.AVERAGE_SERVICE_TIME))
                #一般网络的延迟是20ms以内，6G网络会达到微秒级延迟，到达时间 = 延迟 + 生成时间
                arrival_time = next_task_time + random.choice(task_arrive_distribution)
                task_generation_time = next_task_time
                tasks_list.append([i,num_of_single_job,task_generation_time,arrival_time,service_time])
                task_i = task_i + 1
        i = i + 1
        if cfg.regular_arrivals:
            next_task_time += int(1 / request_rate)
        else:
            next_task_time += int(random.expovariate(request_rate))
        if cfg.progress_bar and i % 100 == 0:
            progress.print_progress(next_task_time, cfg.sim_duration, decimals=3, length=50)


# logging.basicConfig(filename='out-drac.log', level=logging.DEBUG)

class Simulation:
    """Runs the simulation based on the simulation state."""

    def __init__(self, configuration, sim_dir_path):
        self.config = configuration
        self.state = SimulationState(configuration)
        self.sim_dir_path = sim_dir_path

    def run(self):
        """Run the simulation."""

        # Initialize data
        self.state.initialize_state(self.config)

        # A short duration may result in no tasks
        self.state.tasks_scheduled = len(self.state.tasks)
        if self.state.tasks_scheduled == 0:
            return

        # Start at first time stamp with an arrival
        task_number = 0
        self.state.timer.increment(self.state.tasks[0].arrival_time)

        allocation_number = 0
        reschedule_required = False

        if self.config.progress_bar:
            print("\nSimulation started")

        # 短请求队列号
        short_queue_number = 0
        # 中等请求队列号码
        mid_queue_number = 1
        # 长请求队列号
        long_queue_number = 2
        # 短请求服务时间设置
        micro_task_time = 500
        md_state = False
        ld_state = False
        cal_state_time = 0
        # Run for acceptable time or until all tasks are done
        Tasks = self.state.tasks
        while self.state.any_incomplete() and \
                (self.config.sim_duration is None or self.state.timer.get_time() < self.config.sim_duration):

            # If fast forwarding, find the time jump
            if self.config.fast_forward_enabled:
                next_arrival, next_alloc = self.find_next_arrival_and_alloc(task_number, allocation_number)
                time_jump, reschedule_required = self.find_time_jump(next_arrival, next_alloc,
                                                                     immediate_reschedule=reschedule_required)
                logging.debug("\n(jump: {}, rr: {})".format(time_jump, reschedule_required))
            task_distribute = [0, 0]
            task_distribute[0] = len(self.state.queues[0].queue)
            task_distribute[1] = len(self.state.queues[1].queue)
            # Put new task arrivals in queues
            while task_number < self.state.tasks_scheduled and \
                    Tasks[task_number].arrival_time <= self.state.timer.get_time():
                """
                预分配核心，重置核心分配
                """
                if Tasks[task_number].service_time <= micro_task_time:
                    task_distribute[0] = task_distribute[0] + 1
                else:
                    task_distribute[1] = task_distribute[1] + 1
                short_task = task_distribute[0]
                long_task = task_distribute[1]
                # 短请求当前队列长度
                short_reserved_cores = len(self.state.queues[short_queue_number].thread_ids)
                # 长请求当前队列长度
                long_reserved_cores = len(self.state.queues[long_queue_number].thread_ids)

                """
                各个类型队列的当前最大的延迟，采用头部第二个任务作为参照物
                """
                mini_delay = self.state.queues[short_queue_number].current_delay(second=True)
                mid_delay = self.state.queues[mid_queue_number].current_delay(second=True)
                long_delay = self.state.queues[long_queue_number].current_delay(second=True)
                # print(long_delay)

                # if mid_delay >= self.state.config.DELAY_THRESHOLD:
                #     self.get_cores(mid_queue_number)
                #     md_state = True
                # else:
                #     md_state = False
                #
                # if long_delay >= self.state.config.DELAY_THRESHOLD:
                #     self.get_cores(long_queue_number)
                #     ld_state = True
                # else:
                #     ld_state = False
                #
                # if mini_delay < self.config.AVERAGE_SERVICE_TIME * 10:
                #     pass
                # else:
                #     self.return_cores()
                    # cal_state_time = 0

                """
                任务分类分流
                """
                if Tasks[task_number].type == "mini":
                    chosen_queue = 0
                    self.state.queues[0].enqueue(Tasks[task_number], set_original=True)
                elif Tasks[task_number].type == "mid":
                    chosen_queue = 1
                    #在这边改了任务的一些参数
                    self.state.queues[1].enqueue(Tasks[task_number], set_original=True)
                elif Tasks[task_number].type == "high":
                    chosen_queue = 2
                    self.state.queues[2].enqueue(Tasks[task_number], set_original=True)

                if self.config.join_bounded_shortest_queue:
                    chosen_queue = self.state.main_queue
                    self.state.main_queue.enqueue(Tasks[task_number], set_original=False)

                elif self.config.enqueue_choice:
                    chosen_queue = self.choose_enqueue(self.config.ENQUEUE_CHOICES)
                    working_cores = self.state.currently_working_cores()
                    if len(working_cores) == 0:
                        self.state.tasks[task_number].source_core = self.state.queues[chosen_queue].get_core()
                    else:
                        self.state.tasks[task_number].source_core = random.choice(self.state.currently_working_cores())
                    source_core = self.state.tasks[task_number].source_core
                    if source_core != chosen_queue:
                        self.state.threads[source_core].enqueue_penalty += 1
                        self.state.queues[chosen_queue].awaiting_enqueue = True
                        self.state.tasks[task_number].to_enqueue = chosen_queue
                    self.state.queues[source_core].enqueue(self.state.tasks[task_number], set_original=True)

                # else:
                #     chosen_queue = random.choice(self.state.available_queues)
                #     self.state.queues[chosen_queue].enqueue(self.state.tasks[task_number], set_original=True)

                if self.config.fred_reallocation and \
                        self.state.threads[self.state.queues[chosen_queue].get_core()].is_busy():
                    self.state.threads[self.state.queues[chosen_queue].get_core()].fred_preempt = True

                logging.debug("[ARRIVAL]: {} onto queue {}".format(self.state.tasks[task_number], chosen_queue))
                task_number += 1

            # Reallocations
            # Continuously check for reallocations
            if self.config.parking_enabled and self.config.always_check_realloc and\
                    self.state.timer.get_time() - self.state.last_realloc_choice >= self.config.ALLOCATION_PAUSE:
                self.reallocate_threads()

            # Every x us, check for threads to park
            elif self.config.parking_enabled and not self.config.reallocation_replay and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.reallocate_threads()

            # Reallocation replay
            elif self.config.reallocation_replay:
                while allocation_number < self.state.reallocations and \
                        self.state.reallocation_schedule[allocation_number][0] <= self.state.timer.get_time():
                    if self.state.reallocation_schedule[allocation_number][1]:
                        self.state.deallocate_thread(self.find_deallocation())
                    else:
                        self.state.allocate_thread()
                    allocation_number += 1

            # No parking, but still record some stats at reallocation time
            elif not self.config.parking_enabled and self.config.record_allocations and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.state.add_realloc_time_check_in()

            # If recording queue lens but not parking, still do it on reallocs
            elif not self.config.parking_enabled and self.config.record_queue_lens and \
                    self.state.timer.get_time() % self.config.CORE_REALLOCATION_TIMER == 0:
                self.state.record_queue_lengths()

            # Schedule threads
            if self.config.fast_forward_enabled:
                self.fast_forward(time_jump)
            else:
                # Schedule threads
                for thread in self.state.threads:
                    thread.schedule()

                # Move forward in time
                self.state.timer.increment(1)

            # Log state (in debug mode)
            logging.debug("\nTime step: {}".format(self.state.timer))
            logging.debug("Thread status:")
            for thread in self.state.threads:
                logging.debug(str(thread) + " -- queue length of " + str(thread.queue.length()))

            # Print progress bar
            if self.config.progress_bar and self.state.timer.get_time() % 10000 == 0:
                progress.print_progress(self.state.timer.get_time(), self.config.sim_duration, length=50, decimals=3)

        # When the simulation is complete, record final stats
        self.state.add_final_stats()
    
    def choose_enqueue(self, num_choices):
        """Choose a queue to place a new task on by current queueing delay."""
        if num_choices > len(self.state.available_queues):
            num_choices = len(self.state.available_queues)
        choices = random.sample(self.state.available_queues, num_choices)

        delay = self.state.queues[choices[0]].length_by_service_time() if self.config.enqueue_by_st_sum \
            else self.state.queues[choices[0]].length(count_current=True)
        chosen_queue = choices[0]
        for choice in choices:
            if self.config.enqueue_by_st_sum:
                if self.state.queues[choice].length_by_service_time() < delay:
                    delay = self.state.queues[choice].length_by_service_time()
                    chosen_queue = choice
            elif self.state.queues[choice].length(count_current=True) < delay:
                delay = self.state.queues[choice].length(count_current=True)
                chosen_queue = choice

        return chosen_queue
    # """
    # 重分配核心
    # """
    # def get_cores(self, queue_id):
    #     pre_queue = len(self.state.queues[queue_id - 1].thread_ids)
    #     if pre_queue > 1:
    #         self.state.queues[queue_id].thread_ids.append(
    #             self.state.queues[queue_id - 1].thread_ids[len(self.state.queues[queue_id - 1].thread_ids) - 1])
    #         self.state.threads[len(self.state.queues[queue_id - 1].thread_ids) - 1].queue = self.state.queues[queue_id]
    #         self.state.queues[queue_id - 1].thread_ids.pop()
    #     elif queue_id - 1 >= 1:
    #         self.get_cores(queue_id - 1)

    # """
    # 还原核心分配
    # """
    # def return_cores(self):
    #     origin_map = self.state.config.mapping
    #     for i in range(self.state.config.num_queues):
    #         self.state.queues[i].thread_ids = []
    #     for i in range(self.state.config.num_threads):
    #         self.state.queues[origin_map[i]].thread_ids.append(i)
    #         self.state.threads[i].queue = self.state.queues[origin_map[i]]

    def reallocate_threads(self):
        """Reallocate threads according to policy defined in the configuration."""
        self.state.record_queue_lengths()
        if self.config.delay_range_enabled:
            self.reallocate_threads_delay_range()
        elif self.config.buffer_cores_enabled:
            self.reallocate_threads_buffer_cores()
        elif self.config.utilization_range_enabled:
            self.reallocate_threads_utilization()
        elif self.config.ideal_reallocation_enabled:
            self.reallocate_threads_ideal()
        elif self.config.fred_reallocation:
            # If all cores are parked, wake one up when a new task arrives (requires always checking reallocations)
            if len(self.state.parked_threads) == self.config.num_threads:
                self.state.allocate_thread()
        else:
            self.reallocate_threads_default()

    def reallocate_threads_default(self):
        """Reallocate threads. Grants a core if there is a queue whose head has been around for longer than the
        reallocation timer."""
        if self.state.any_queue_past_delay_threshold():
            self.state.allocate_thread()
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_ideal(self):
        """Reallocate threads. Grants a core for every queued task as it is able."""

        # Determine how many queued items there are
        queued_tasks = self.state.total_queue_occupancy()

        # Determine how many threads are non-productive (weaker constraint than buffer cores)
        non_productive_cores = self.state.currently_non_productive_cores()

        # Add/remove the difference
        if queued_tasks < len(non_productive_cores):
            while queued_tasks < len(non_productive_cores):
                thread = min(non_productive_cores)
                self.state.deallocate_thread(thread)
                non_productive_cores = self.state.currently_non_productive_cores()
        elif queued_tasks > len(non_productive_cores):
            while queued_tasks > len(non_productive_cores):
                self.state.allocate_thread()
                non_productive_cores = self.state.currently_non_productive_cores()
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_delay_range(self):
        """Reallocate cores according to delay range."""
        if self.config.delay_range_by_service_time:
            avg_delay = self.state.current_average_service_time_sum()
        else:
            avg_delay = self.state.current_average_queueing_delay()

        # If delay is high, add a core
        if avg_delay > self.config.REALLOCATION_THRESHOLD_MAX:
            self.state.allocate_thread()

        # If delay is low, remove a core
        elif avg_delay < self.config.REALLOCATION_THRESHOLD_MIN:
            # Only deallocate if there are buffer cores available
            if len(self.state.current_buffer_cores(check_work_available=True)) > 0:
                # Force clustering of parked cores by choosing min index
                thread = min(self.state.current_buffer_cores(check_work_available=True))
                self.state.deallocate_thread(thread)
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_utilization(self):
        """Reallocate cores according to utilization range."""
        utilization = self.state.current_utilization()

        # If high utilization, add a thread
        if utilization > self.config.UTILIZATION_MAX:
            self.state.allocate_thread()

        # If low utilization, revoke a core
        elif utilization < self.config.UTILIZATION_MIN and \
                len(self.state.current_buffer_cores(check_work_available=True)) > 0:
            thread = min(self.state.current_buffer_cores(check_work_available=True))
            self.state.deallocate_thread(thread)

        # Log that an allocation decision was considered
        else:
            self.state.add_reallocation(None)

    def reallocate_threads_buffer_cores(self):
        """Reallocate cores according to buffer core ranges."""
        allowed_buffer_cores = self.state.allowed_buffer_cores()
        current_buffer_cores = self.state.current_buffer_cores()
        num_current_buffer_cores = len(current_buffer_cores)

        # If not enough buffer cores, allocate a core
        if num_current_buffer_cores < allowed_buffer_cores[0]:
            while len(current_buffer_cores) < allowed_buffer_cores[0]:
                allocated = self.state.allocate_thread()
                if allocated is None:
                    break
                current_buffer_cores = self.state.current_buffer_cores()

        # If too many buffer cores, deallocate a thread
        elif num_current_buffer_cores > allowed_buffer_cores[1]:
            while len(current_buffer_cores) > allowed_buffer_cores[1]:
                thread = random.choice(current_buffer_cores)
                self.state.deallocate_thread(thread)
                current_buffer_cores = self.state.current_buffer_cores()
        else:
            self.state.add_reallocation(None)

    def find_deallocation(self):
        """Find a core to deallocate from all non-parked cores. Preference is given to idle or soon-to-be-idle cores."""
        free_threads = set(range(self.config.num_threads)).difference(self.state.parked_threads)
        min_time_left = None

        choice = None

        # Prefer threads that are idle, then the one that will be idle soonest
        for thread_id in free_threads:
            if not self.state.threads[thread_id].is_productive():
                choice = thread_id
                break
            else:
                if min_time_left is None or self.state.threads[thread_id].current_task.time_left < min_time_left:
                    min_time_left = self.state.threads[thread_id].current_task.time_left
                    choice = thread_id

        return choice

    def find_next_arrival_and_alloc(self, task_number, allocation_number):
        """Determine the next task arrival and allocation decision.
        :param task_number: Current index into tasks that have arrived.
        :param allocation_number: Current allocation index into schedule if in replay.
        """
        next_arrival = self.state.tasks[task_number].arrival_time if task_number < self.state.tasks_scheduled else None
        next_alloc = None

        if self.config.reallocation_replay and allocation_number < self.state.reallocations:
            next_alloc = self.state.reallocation_schedule[allocation_number][0]

        elif self.config.always_check_realloc:
            if self.config.delay_range_enabled:
                # Check when max threshold - current avg delay will happen
                # (min threshold cannot be violated during phase of otherwise inaction)
                # If by service time, until next arrival (/completion), this value cannot change
                if self.config.delay_range_by_service_time:
                    time_until_threshold_passed = 0
                else:
                    time_until_threshold_passed = self.config.REALLOCATION_THRESHOLD_MAX - \
                                                  int(self.state.current_average_queueing_delay())

            elif self.config.buffer_cores_enabled:
                # Buffer cores cannot change between other actions
                time_until_threshold_passed = 0

            elif self.config.ideal_reallocation_enabled:
                # In ideal case, only arrivals change this
                time_until_threshold_passed = 0

            else:
                current_delays = [x.current_delay() for x in self.state.queues]
                time_until_threshold_passed = self.config.ALLOCATION_THRESHOLD - max(current_delays)
            next_alloc = self.state.timer.get_time() + time_until_threshold_passed \
                if time_until_threshold_passed > 0 else None

        # If recording queue lens, do it at the reallocation intervals
        elif self.config.parking_enabled or self.config.record_queue_lens:
            next_alloc = (math.floor(
                self.state.timer.get_time() / self.config.CORE_REALLOCATION_TIMER) + 1) \
                         * self.config.CORE_REALLOCATION_TIMER

        # When recording single queue reallocs
        elif self.config.record_allocations:
            next_alloc = (math.floor(
                self.state.timer.get_time() / self.config.CORE_REALLOCATION_TIMER) + 1) \
                         * self.config.CORE_REALLOCATION_TIMER

        return next_arrival, next_alloc

    def find_time_jump(self, next_arrival, next_allocation=None, set_clock=True, immediate_reschedule=False):
        """Find the time step to the next significant event that requires directly running the simulation.
        :param next_arrival: Next task arrival.
        :param next_allocation: Next core allocation event.
        :param set_clock:
        :param immediate_reschedule: True if last time step required a jump of 1 for the next step.
        (ie. completing a task)
        """
        # Find the next task completion time
        completion_times = []
        for thread in self.state.threads:
            if thread.current_task is not None and not thread.current_task.is_idle:
                completion_times.append(thread.current_task.expected_completion_time())

                # If a task completed now but immediate reschedule missed (ex. service time of 1), next jump must be 1
                if thread.last_complete == self.state.timer.get_time():
                    immediate_reschedule = True

        next_completion_time = min(completion_times) if len(completion_times) > 0 else None

        # Find the next event of any type
        upcoming_events = [next_arrival, next_completion_time, next_allocation]
        if not any(upcoming_events):
            next_event = self.state.timer.get_time() + 1
        else:
            next_event = min([event for event in upcoming_events if event])

        # Set the time jump
        jump = next_event - self.state.timer.get_time()

        if jump == 0: # this can happen with 0-duration tasks - TODO: look into this more
            jump = 1

        # If immediate reschedule required, jump is 1
        # Another immediate reschedule is necessary if the time jump would have been 1 regardless
        if immediate_reschedule:
            reschedule_required = jump == 1 and next_completion_time == next_event
            jump = 1
        else:
            # TODO: (below) Not if it is a work steal task that isn't actually done (but how to determine this?)
            reschedule_required = next_completion_time == next_event

        # Move the clock
        if set_clock:
            self.state.timer.increment(jump)
        # reschedule_required = False
        return jump, reschedule_required

    def fast_forward(self, jump):
        """Fast forward through uneventful timesteps."""
        for thread in self.state.threads:
            thread.schedule(time_increment=jump)
        # self.state.timer.increment(jump)
        # Record all paired/unpaired time
        self.determine_pairings(jump)

    def determine_pairings(self, jump):
        """Determine how to pair cores for accounting of how well they are spending their time."""
        increment = jump if jump != 0 else 1
        paired = self.state.num_paired_cores()
        for thread in self.state.threads:
            if thread.classified_time_step:
                thread.classified_time_step = False
                if thread.preempted_classification:
                    thread.preempted_classification = False
                    # If you preempt a work steal spin, you get to use this exact cycle on the new task
                    if paired > 0:
                        thread.add_paired_time(increment - 1)
                        paired -= 1
                    else:
                        thread.add_unpaired_time(increment - 1)
            elif paired > 0:
                thread.add_paired_time(increment)
                paired -= 1
            else:
                thread.add_unpaired_time(increment)

    def save_stats(self,new_sim_name,i):
        """Save simulation date to file."""
        # Make files and directories
        name = self.config.name + str(i)
        new_dir_name = os.path.join(new_sim_name, "sim_{}/".format(name))
        #new_dir_name = RESULTS_DIR.format(self.sim_dir_path) + "sim_{}/".format(run_name)+ "sim_{}/".format(self.config.name)
        os.makedirs(os.path.dirname(new_dir_name))
        cpu_file = open("{}cpu_usage.csv".format(new_dir_name), "w")
        task_file = open("{}task_times.csv".format(new_dir_name), "w")
        meta_file = open("{}meta.json".format(new_dir_name), "w")
        stats_file = open("{}stats.json".format(new_dir_name), "w")

        # Write CPU information
        cpu_file.write(','.join(Thread.get_stat_headers(self.config)) + "\n")
        for thread in self.state.threads:
            cpu_file.write(','.join(thread.get_stats()) + "\n")
        cpu_file.close()

        # Write task information（需要修改只写入成功完成的任务）
        task_file.write(','.join(Task.get_stat_headers(self.config)) + "\n")
        for task in self.state.tasks:
            if task.complete == True:
                task_file.write(','.join(task.get_stats()) + "\n")
        task_file.close()

        # Save the configuration
        json.dump(self.config.__dict__, meta_file, indent=0)
        meta_file.close()

        # Save global stats
        json.dump(self.state.results(), stats_file, indent=0)
        stats_file.close()

        # If recording work steal stats, save
        if self.config.record_steals:
            ws_file = open("{}work_steal_stats.csv".format(new_dir_name), "w")
            ws_file.write("Local Thread,Remote Thread,Time Since Last Check,Queue Length,Check Count,Successful\n")
            for check in self.state.ws_checks:
                ws_file.write("{},{},{},{},{},{}\n".format(check[0], check[1], check[2], check[3], check[4], check[5]))
            ws_file.close()

        # If recording allocations, save
        if self.config.record_allocations:
            realloc_sched_file = open("{}realloc_schedule".format(new_dir_name), "w")
            realloc_sched_file.write(str(self.state.reallocation_schedule))
            realloc_sched_file.close()

        # If recording queue lengths, save
        if self.config.record_queue_lens:
            qlen_file = open("{}queue_lens.csv".format(new_dir_name), "w")
            for lens in self.state.queue_lens:
                qlen_file.write(",".join([str(x) for x in lens]) + "\n")
            qlen_file.close()


if __name__ == "__main__":
    run_name = SINGLE_THREAD_SIM_NAME_FORMAT.format("cluster_simulation",
                                                    datetime.datetime.now().strftime("%y-%m-%d_%H:%M:%S"))
###################读取配置文件#############################
    #服务器集群中服务器的个数
    server_num = 3
    #集群调度是否使用随机分配方法
    cluster_random_schedule = True
    #服务器集群power of  k  choice 
    enserver_choice = 3
    #服务器集群队列
    server_thread = []
    #是否优先完成序号在前的作业的任务，解决一致性问题
    job_id_ordered = True
    #是否记录本次生成的task集
    record_task = True
    #是否从task_file中读取task
    read_task_file = False
    #taskfile的路径
    task_file_path = "/home/yqy/simulation_schedule/simulator-DARC-E/task_record/cluster_simulation_23-07-13_16:43:51_tasks_info.csv"
    #用于获取此时的路径，用于存出仿真结果
    path_to_sim = os.path.relpath(pathlib.Path(__file__).resolve().parents[1], start=os.curdir)
    #打开配置文件，读取配置文件
    if os.path.isfile("/home/yqy/simulation_schedule/simulator-DARC-E/configs/config.json"):
        cfg_json = open("/home/yqy/simulation_schedule/simulator-DARC-E/configs/config.json", "r")
        cfg = json.load(cfg_json, object_hook=SimConfig.decode_object)
        #每个服务器的run_name不应该一样，
        cfg.name = "server_"
        cfg_json.close()
    else:
        print("Config file not found.")
        exit(1)
    #用于记录乱序的个数
    num_of_disorder = 0
#############################仿真参数初始化###############################
    #################Set tasks and arrival times（生成任务）1.随即生成；2.采用之前生成的task_file##############
    tasks = []
    ###########任务生成的两种逻辑##########
    if read_task_file == False:
        #####初始化任务参数####(双峰，三峰参数)
        task_distribution_para = []
        task_distribution_para.append([50,9])
        task_distribution_para.append([500,1])
        ######任务的延迟分布，是范围或者是+random
        task_delay_para = []
        task_delay_para.append([0,9])
        task_delay_para.append([100,1])
        i = 0
        while i < len(task_distribution_para):
            run_name = run_name + "-"+str(task_distribution_para[i][0]) + "_" + str(task_distribution_para[i][1])
            i = i + 1
        run_name = run_name + "-delay"
        i= 0
        while i < len(task_delay_para):
            run_name = run_name + "-"+str(task_delay_para[i][0]) + "_" + str(task_delay_para[i][1])
            i = i + 1
        tasks_list = []
        Generate_cluster_tasks(cfg,tasks_list,task_distribution_para,task_delay_para)
        ##############按照到达顺序给任务排序################
        tasks = sorted(tasks_list, key = lambda x: x[3])
        #记录此次仿真，用于下次仿真
        if record_task == True:
            write_tasks_into_file(run_name,tasks)
        ordered_name = ""
        cluster_schedule_name = ""
        if cluster_random_schedule == True:
            cluster_schedule_name = "_cluster_random"
        if job_id_ordered == True:
            ordered_name = "_ordered"
        run_name = run_name + ordered_name + cluster_schedule_name
    else:
        read_tasks_from_file(task_file_path,tasks)
        #用于记录是哪一项任务
        filename = task_file_path.rsplit('/', 1)[-1].rsplit('.', 1)[0]
        ordered_name = ""
        cluster_schedule_name = ""
        if cluster_random_schedule == True:
            cluster_schedule_name = "_cluster_random"
        if job_id_ordered == True:
            ordered_name = "_ordered"
        run_name = run_name + "_reuse_" + filename + "_"+ ordered_name + cluster_schedule_name
        print(run_name)
    ###########初始化服务器集群（包含任务）################
    i = 0
    while i < server_num:
        sim = Simulation(cfg, path_to_sim)
        sim.state.initialize_state(sim.config,tasks)
        server_thread.append(sim)
        i = i+1
################################开始仿真#########################################    
    #判断任务不为空
    tasks_scheduled = len(server_thread[0].state.tasks)
    if tasks_scheduled == 0:
        exit(1) 
    task_number = 0
    if server_thread[0].config.progress_bar:
        print("\nSimulation started")
    ###########仿真的时间按1纳秒循环,(所有服务器的配置一样)##########
    while(server_thread[0].config.sim_duration is None or server_thread[0].state.timer.get_time() < server_thread[0].config.sim_duration):
        ###########先将某一时刻的某一任务分配到服务器上，采用power of k choice ##########
        while task_number < tasks_scheduled  and server_thread[0].state.tasks[task_number].arrival_time <= server_thread[0].state.timer.get_time():
            if cluster_random_schedule == True:
                random_num = random.randint(0,server_num-1)
                chosen_server = server_thread[random_num]
            else:
                if enserver_choice > len(server_thread):
                    enserver_choice = len(server_thread)
                choices = random.sample(server_thread,enserver_choice)
                delay = choices[0].state.current_average_queueing_delay()
                chosen_server = choices[0]
                for choice in choices:
                    if choice.state.current_average_queueing_delay() < delay:
                        delay = choice.state.current_average_queueing_delay()
                        chosen_server = choice# power of k choice
            ##############请求进入服务器内部的队列##################
            # 短请求队列号
            short_queue_number = 0
            # 中等请求队列号码
            mid_queue_number = 1
            # 长请求队列号
            long_queue_number = 2     
            if server_thread[0].state.tasks[task_number].type == "mini":
                chosen_queue = 0
                if chosen_server.state.queues[0].enqueue(chosen_server.state.tasks[task_number], set_original=True , job_id_ordered = job_id_ordered) == True:
                    num_of_disorder = num_of_disorder+1
            elif server_thread[0].state.tasks[task_number].type == "mid":
                chosen_queue = 1
                #在这边改了任务的一些参数(需要重写，按照顺序插入队列合适的位置)
                if chosen_server.state.queues[1].enqueue(chosen_server.state.tasks[task_number], set_original=True, job_id_ordered = job_id_ordered) == True:
                    num_of_disorder = num_of_disorder+1
            elif server_thread[0].state.tasks[task_number].type == "high":
                chosen_queue = 2
                if chosen_server.state.queues[2].enqueue(chosen_server.state.tasks[task_number], set_original=True, job_id_ordered = job_id_ordered) == True:
                    num_of_disorder = num_of_disorder+1

            if chosen_server.config.join_bounded_shortest_queue:
                chosen_queue = chosen_server.state.main_queue
                chosen_server.state.main_queue.enqueue(chosen_server.state.tasks[task_number], set_original=False, job_id_ordered = job_id_ordered)
            task_number +=1    
        ############按照每个服务器内的循环执行################
        for server in server_thread:
            ###############Reallocations##################
            # Continuously check for reallocations
            if server.config.parking_enabled and server.config.always_check_realloc and\
                    server.state.timer.get_time() - server.state.last_realloc_choice >= server.config.ALLOCATION_PAUSE:
                server.reallocate_threads()

            # Every x us, check for threads to park
            elif server.config.parking_enabled and not server.config.reallocation_replay and \
                    server.state.timer.get_time() % server.config.CORE_REALLOCATION_TIMER == 0:
                server.reallocate_threads()

            # No parking, but still record some stats at reallocation time
            elif not server.config.parking_enabled and server.config.record_allocations and \
                    server.state.timer.get_time() % server.config.CORE_REALLOCATION_TIMER == 0:
                server.state.add_realloc_time_check_in()

            # If recording queue lens but not parking, still do it on reallocs
            elif not server.config.parking_enabled and server.config.record_queue_lens and \
                    server.state.timer.get_time() % server.config.CORE_REALLOCATION_TIMER == 0:
                server.state.record_queue_lengths()
            ##################Schedule threads##############
            for thread in server.state.threads:
                thread.schedule()

            #############每个服务器时间向前推进一纳秒Move forward in time #################
            server.state.timer.increment(1)
        if server_thread[0].config.progress_bar and server_thread[0].state.timer.get_time() % (server_thread[0].config.sim_duration/100) == 0:
            progress.print_progress(server_thread[0].state.timer.get_time(), server_thread[0].config.sim_duration, length=50, decimals=3)
    new_sim_name = RESULTS_DIR.format(server_thread[0].sim_dir_path) + "sim_{}/".format(run_name)
    os.makedirs(os.path.dirname(new_sim_name))
    i = 0
    while i < server_num:
        server_thread[i].state.add_final_stats()
        server_thread[i].save_stats(new_sim_name,i)
        i = i + 1
    print("num_of_disorder : %d",num_of_disorder)