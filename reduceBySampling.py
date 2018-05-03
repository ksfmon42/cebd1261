#!/usr/bin/env python3
import sys

# This program picks and returns from the stdin stream the first line
#   of each location and hour combination along with the wait time
#   and discard the others.  The input is expected to be sorted in
#   the following way.

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
# Aldergrove:BC;2017-06-30_19     3
# Aldergrove:BC;2017-06-30_20     8
# Aldergrove:BC;2017-06-30_21     14

# Note: We expect the keys come grouped together, in that the 'sort'/'shuffle' is done automatically
#   as a core requirement of the map reduce framework.
# We need to keep track of and handle state changes.
# i.e., When the key changes (i.e., location-date-time), we need to reset
# our counter, and write out the average wait time in that hour we've calculated

prevLocationDateHour = None
samplewaitTime = None

for line in sys.stdin:

    line = line.strip()
    locationDateHour, sampleWaitTime = line.split("\t")

    # if this is the first iteration
    if not prevLocationDateHour: 
        prevLocationDateHour = locationDateHour
        print("%s\t%s" % (locationDateHour,sampleWaitTime))
        continue

    if locationDateHour == prevLocationDateHour:
      continue
    else:
      print("%s\t%s" % (locationDateHour,sampleWaitTime))
      prevLocationDateHour = locationDateHour


      
