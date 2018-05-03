#!/usr/bin/env python3
import sys
import re

# TODOs
#   Logging does not work in the MapReduce streaming framework and still to be addressed
#   Add more comments
firstLine=True
logfile=open("Logfile", "w")
withWordMinute = re.compile("(\d+)_*minute", re.IGNORECASE)
justDigits = re.compile("(\d+) *$", re.IGNORECASE)
for line in sys.stdin:
    #Skip first line of headings
    if not firstLine:
      line = line.strip()
      try:
        office, location1, location2, dateTime, commercial, waitTime = line.split(",")
      except:
        try:
          office, location1, dateTime, commercial, waitTime = line.split(",")
          if office == "Thousand Islands Bridge": 
            location1 = '"Lansdowne'
            location2 = 'ON"'
          else:
            raise
        except:
          message="Issue with line: %s\n" % line
          logfile.write(message) 
          continue

      location = (location1.strip(" ")+":"+location2.strip(" ")).strip('"')
      location=location.replace(" ","_")
      location=location.replace("._",".")
      dateTime=dateTime.replace(" ","_")
      waitTime=waitTime.replace(" ","_")
 
      if not justDigits.match(str(waitTime)):
        if re.search('missed_entry|temporarily_closed|not_applicable|closed', str(waitTime), re.IGNORECASE):
          waitTime="NaN"
          # In fact skip outputing this line all together for NaNs
          continue

        elif re.search('No_delay', str(waitTime), re.IGNORECASE):
          waitTime=0

        elif withWordMinute.match(str(waitTime)): 
          waitTime = withWordMinute.match(str(waitTime)).group(1)

        else:
          # Cases which have not been encountered before, log it.
          message="Line pattern not encountered before: %s\n" % line
          logfile.write(message)
          continue

#      print(location, dateTime, waitTime)

      # Remove the minute part to map all observations to the hour
      dateTimeNoMinutes = str(dateTime).split(":")[0]

      # Form key, value pair for map reduce framework
      results = [location+";"+dateTimeNoMinutes, str(waitTime)]
      print("\t".join(results))
    else:
      firstLine=False

logfile.close()

