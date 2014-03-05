import sys
import getopt
import redis
import searches
import json

def purge(timeArray,continuous, multiplier):
	retArray=[]
	if len(timeArray)>1:
		currentStart=timeArray[0][0]
		currentEnd=timeArray[0][1]
		for point in timeArray:
			if (point[0]-currentEnd)<=(continuous*multiplier):
				currentEnd = point[1]
			else:
				c=[]
				c.append(currentStart)
				c.append(currentEnd)
				retArray.append(c)
				currentStart=point[0]
				currentEnd = point[1]

		c=[]
		c.append(currentStart)
		c.append(currentEnd)
		retArray.append(c)
	elif len(timeArray)==1:
		retArray=timeArray		
	return(retArray)

def getCoverage(r, key, startTime, endTime,continuous, multiplier):
	if startTime>endTime:
		return {"status":"Error, startTime must occur before endTime", "values":"null"}
	#Find first start before startTime
	#Find first start after endTime

	#Tighten this up...replace the +inf and -inf with something. Can't be startTime and endTime.
	allOfThem = r.zrangebyscore(key, '-inf', '+inf',  withscores=True)
	if (len(allOfThem) == 0):
		return {"status":"Good", "values":[]}
	beforePos = searches.binary_search(allOfThem, startTime,-1)
	afterPos = searches.binary_search(allOfThem, endTime,1)
	beforeStartTime = allOfThem[beforePos][1] 
	afterEndTime = allOfThem[afterPos][1]
	results=[]
	started = False
	finished = False
	for x in range(beforePos,afterPos+1):
		startCand = None
		endCand = None
		payload = json.loads(allOfThem[x][0])  #This gets us payload["endTime"]
		thisStartTime = allOfThem[x][1]
		thisEndTime = payload["endTime"]
		if not started:	#We've got to start somewhere. Three cases:
			#Case s1: startTime is between the boundaries
			if (startTime>=thisStartTime) and (startTime<=thisEndTime):
				started=True
				startCand = startTime
			#Case s2: startTime is before the start boundary
			elif (startTime<=thisStartTime):
				started=True
				startCand = thisStartTime
			#Case s3: startTime is after the end boundary...This is a don't care case, will be handled next iteration
			#elif (startTime>=thisEndTime):

			if started:	#Check for ending
				#Case e1: end times between boundaries
				#if s1 above, S1 -> Ts -> Te -> E1
				#if s2 above, Ts -> S1 -> Te -> E1
				if (endTime>=thisStartTime) and (endTime<=thisEndTime):
					finished=True
					endCand = endTime
				#Case e2: end times after end boundary
				#if s1 above, S1 -> Ts -> E1 -> Te
				#if s2 above, Ts -> S1 -> E1 -> Te
				elif endTime>=thisEndTime:
					endCand = thisEndTime
				#Case e3: end times before start boundary...No data 
				elif endTime<=thisStartTime:
					finished = True
			if (startCand is not None) and (endCand is not None):
				tempArray=[]
				tempArray.append(startCand)
				tempArray.append(endCand)
				results.append(tempArray)
			if finished:
				break
		else:	#We are started, need to finish
			#Case e1: end times between boundaries. End it, this does it for us.
			if (endTime>=thisStartTime) and (endTime<=thisEndTime):
				finished=True		
				startCand = thisStartTime 
				endCand = endTime
			#Case e2: end times before start boundary. End it, last full interval does it for us.
			elif endTime<thisStartTime:
				finished = True
			#Case e3: end times after end boundary. Add this interval and keep going.
			elif endTime>=thisEndTime:
				startCand = thisStartTime 
				endCand = thisEndTime

			if (startCand is not None) and (endCand is not None):
				tempArray=[]
				tempArray.append(startCand)
				tempArray.append(endCand)
				results.append(tempArray)
			if finished:
				break

	return {"status":"Good","values":purge(results,continuous, multiplier)}


