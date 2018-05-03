#!/usr/bin/env python3
import sys

# Expected input (ordered by key):
#   location-date-time(to the hour) <tab> wait-time
# Examples:
# Aldergrove:BC;2017-06-30_19     3
# Aldergrove:BC;2017-06-30_19     4
# Aldergrove:BC;2017-06-30_19     10
# Aldergrove:BC;2017-06-30_20     8
# Aldergrove:BC;2017-06-30_20     13
# Aldergrove:BC;2017-06-30_20     11
# Aldergrove:BC;2017-06-30_21     14
# Aldergrove:BC;2017-06-30_21     4
# Aldergrove:BC;2017-06-30_21     0

# Expected output (grouped by key)
# Aldergrove:BC;2017-06-30_19     6
# Aldergrove:BC;2017-06-30_20     11
# Aldergrove:BC;2017-06-30_21     6

# Note: We expect the keys come grouped together, in that the 'sort'/'shuffle' is done automatically
#   as a core requirement of the map reduce framework.
# We need to keep track of and handle state changes.
# i.e., When the key changes (i.e., location-date-time), we need to reset
# our counter, and write out the average wait time in that hour we've calculated

prevLocationDateHour = None
waitTime = None
observations_this_hour = None

for line in sys.stdin:

    line = line.strip()
    locationDateHour, minutes = line.split("\t")

    # if this is the first iteration
    if not prevLocationDateHour: 
        prevLocationDateHour = locationDateHour
        waitTime = 0
        observations_this_hour = 0

    # if they're the same, log it
    if locationDateHour == prevLocationDateHour:
        waitTime += int(minutes)
        observations_this_hour += 1
    else:
        # state change (previous line was k=x, this line is k=y)
        avgWaitTime = round(float(waitTime)/observations_this_hour)
        print(prevLocationDateHour, '\t', avgWaitTime)
        prevLocationDateHour = locationDateHour
        waitTime = int(minutes)
        observations_this_hour = 1
        
# this is to catch the final counts after all records have been received.
avgWaitTime = round(float(waitTime)/observations_this_hour)
print("%s\t%s" % (prevLocationDateHour,avgWaitTime))
