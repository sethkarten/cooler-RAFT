#!/bin/bash

num_nodes=(3 10 30)
# latencies=(0 2 8)
latencies=(2)
# timeouts=(1 10 40 100)
timeouts=(1 40 100)
# max_failures=(1 4 10 14) 
max_failures=(1 4 14) 
# failure_intervals=(0 2 8 32)
failure_intervals=(8)
# client_intervals=(1 5 10)
client_intervals=(5)
counter=9000

max_jobs=9

output_dir="./outputs/experiment_logs"
mkdir -p $output_dir

active_jobs=0
for nodes in "${num_nodes[@]}"; do
    for latency in "${latencies[@]}"; do
        for timeout in "${timeouts[@]}"; do
            for failure in "${max_failures[@]}"; do
                for fail_interval in "${failure_intervals[@]}"; do
                    for client_interval in "${client_intervals[@]}"; do
                        if (( failure < ((nodes+1)/2) )); then
                            log_filename="${output_dir}/log_n${nodes}_l${latency}_t${timeout}_f${failure}_i${fail_interval}_i${client_interval}/"
                            mkdir -p $log_filename
                            echo "Running: ${nodes} nodes, ${latency}ms latency, ${timeout}s timeout, ${failure} failures, ${fail_interval}s fail interval , ${client_interval}s client interval"
                            python3 main.py --base_port=$counter --num_nodes=$nodes --interval=$timeout --filepath="$log_filename" --latency=$latency --max_failures=$failure --failure_interval=$fail_interval --client_interval=$client_interval &
                            # echo "Done"
                            ((counter+=100))
                            # sleep 5
                            ((active_jobs++))
                            if (( active_jobs >= max_jobs )); then
                                wait
                                ((active_jobs--))
                            fi
                        fi
                    done
                done
            done
        done
    done
done
wait
