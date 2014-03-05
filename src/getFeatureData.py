import redis
import struct

def getData(r, key, startTime, endTime):
	#if startTime>=endTime:
	#	return {"status":"Error, startTime must occur before endTime", "values":"null"}
	goodVals = r.zrangebyscore(key, startTime, endTime)
	#if (len(goodVals) == 0):
	#	return {"status":"Good", "values":[]}		
	#return {"status":"Good","values":goodVals}
	s='%sd' % (len(goodVals[0])/struct.calcsize('d'))
	return goodVals

