import redis
import struct

#getFeatureData.getData(<reddis connection>,<patient_id>,<startTime>,<endTime>)

#This will return a byte array of the feature data between <startTime> and <endTime> for <patient_id>
#This function is intended to be called with the <startTime>,<endTime> pairs gathered from getCoverage.

def getData(r, key, startTime, endTime):
	#A more JSON-ified return, if needed
	#if startTime>=endTime:
	#	return {"status":"Error, startTime must occur before endTime", "values":"null"}
	goodVals = r.zrangebyscore(key, startTime, endTime)
	#if (len(goodVals) == 0):
	#	return {"status":"Good", "values":[]}		
	#return {"status":"Good","values":goodVals}
	s='%sd' % (len(goodVals[0])/struct.calcsize('d'))
	return goodVals

