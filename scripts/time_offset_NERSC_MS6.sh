#!/bin/bash

time_MS6=$(ssh mothership 'date +%s%3N')
time_NERSC=$(date +%s%3N)
time_offset=$(($time_NERSC - $time_MS6))
echo "Time offset between MS6 and NERSC is: ${time_offset}ms"