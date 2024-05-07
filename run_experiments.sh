#!/bin/bash

# num_nodes=(3)
# latencies=(0)
# timeouts=(1)
# max_failures=(1)  
# failure_intervals=(0)

num_nodes=(3 10 30)
latencies=(0 2 8)
timeouts=(1 10 40 100)
max_failures=(1 4 10 14) 
failure_intervals=(0 2 8 32)

output_dir="./outputs/experiment_logs"
mkdir -p $output_dir

for nodes in "${num_nodes[@]}"; do
    for latency in "${latencies[@]}"; do
        for timeout in "${timeouts[@]}"; do
            for failure in "${max_failures[@]}"; do
                for fail_interval in "${failure_intervals[@]}"; do
                    if (( failure < ((nodes+1)/2) )); then
                        log_filename="${output_dir}/log_n${nodes}_l${latency}_t${timeout}_f${failure}_i${fail_interval}/"
                        mkdir -p $log_filename
                        echo "Running: ${nodes} nodes, ${latency}ms latency, ${timeout}s timeout, ${failure} failures, ${fail_interval}s fail interval"
                        python3 main.py --num_nodes=$nodes --interval=$timeout --filepath="$log_filename" --latency=$latency --max_failures=$failure --failure_interval=$fail_interval
                        echo "Done"
                        sleep 5
                    fi
                done
            done
        done
    done
done
