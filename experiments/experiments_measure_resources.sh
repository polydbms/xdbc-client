#!/bin/bash

# Ensure two container names are provided as arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <container_name1> <container_name2>"
    exit 1
fi

CONTAINER1="$1"
CONTAINER2="$2"

# Initialize variables to store CPU utilization and sample count
TOTAL_CPU_CONTAINER1=0
TOTAL_CPU_CONTAINER2=0
SAMPLE_COUNT=0

# Check if the start control file exists in /tmp
if [ -f /tmp/start_monitoring ]; then
    # Start monitoring resources
    echo "Monitoring started for containers: $CONTAINER1 and $CONTAINER2..."
else
    echo "Monitoring not started. Create /tmp/start_monitoring to begin monitoring."
    exit 1
fi

# Continuously monitor CPU utilization while the stop control file is not present
while [ ! -f /tmp/stop_monitoring ]; do
    # Get CPU utilization for both containers
    CPU_CONTAINER1=$(docker stats --no-stream --format "{{.CPUPerc}}" "$CONTAINER1" | tail -n 1)
    CPU_CONTAINER2=$(docker stats --no-stream --format "{{.CPUPerc}}" "$CONTAINER2" | tail -n 1)

    # Remove the '%' sign from CPU percentages
    CPU_CONTAINER1=${CPU_CONTAINER1%%%}
    CPU_CONTAINER2=${CPU_CONTAINER2%%%}

    # Check if CPU values are numeric (non-empty) before adding to totals
    if [[ "$CPU_CONTAINER1" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        TOTAL_CPU_CONTAINER1=$(awk "BEGIN {print $TOTAL_CPU_CONTAINER1 + $CPU_CONTAINER1}")
        SAMPLE_COUNT=$((SAMPLE_COUNT + 1))
    fi

    if [[ "$CPU_CONTAINER2" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        TOTAL_CPU_CONTAINER2=$(awk "BEGIN {print $TOTAL_CPU_CONTAINER2 + $CPU_CONTAINER2}")
    fi

    # Sleep for a specified interval (e.g., 1 second)
    sleep 1
done

# Calculate the average CPU utilization for both containers
AVERAGE_CPU_CONTAINER1=$(awk "BEGIN {if ($SAMPLE_COUNT > 0) print $TOTAL_CPU_CONTAINER1 / $SAMPLE_COUNT; else print 0}")
AVERAGE_CPU_CONTAINER2=$(awk "BEGIN {if ($SAMPLE_COUNT > 0) print $TOTAL_CPU_CONTAINER2 / $SAMPLE_COUNT; else print 0}")

# Output average CPU utilization in JSON format
echo '{
    "'"$CONTAINER1"'": {
        "average_cpu_usage": '"$AVERAGE_CPU_CONTAINER1"'
    },
    "'"$CONTAINER2"'": {
        "average_cpu_usage": '"$AVERAGE_CPU_CONTAINER2"'
    }
}' > /tmp/resource_metrics.json

# Clean up by removing the stop control file
rm -f /tmp/stop_monitoring

echo "Monitoring finished."
