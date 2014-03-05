import csv
import exceptions
import json
import redis
import sys
import getopt
import datetime
import conversions
import logging
import time
import struct

def writePipedRedis(pipe,key, score, member,lifetime):
	pipe.zadd(key, score, member)
	pipe.expire(key, lifetime) 

def writeRedis(r,key, score, member,lifetime):
	r.zadd(key, score, member)
	r.expire(key, lifetime) 

def loadFile(path, key, measure,redisConnection,usePiped, pipe, config, logger, startRow=0):
	logger.info("Loading file: "+path)
	logger.info("Starting at row: "+str(startRow))
	if (usePiped == True):
		logger.info("Use Pipe: True")
	else:
		logger.info("Use Pipe: False")
	ifile  = open(path, "rb")
	reader = csv.reader(ifile,delimiter=',')
	headers=[]
	noVal = float(config["noVal"])
	excludeList=config["excludeList"]
	includeList=config["includeList"]
	passThroughs=config["passThroughList"]
	excludePattern=config["excludePattern"]
	redisLifetime=int(config["redisLifetime"])

	submitRate = int(config["submitRate"])
	submitRecords = int(config["submitRecords"])
	submitId = config["submitId"]
	
	pipeSize = int(config["pipeSize"])
	timeOffset = config["continuous"] * config["multiplier"]
	currentStart = 0
	currentTime = 0
	lastTime = 0
	counter = 0
	indexCounter=0
	dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime)  or isinstance(obj, datetime.date) else None

	#On the first row...
	row=reader.next()
	try:
		header[0] = int(row[0])
	except:
		for x in range(0,len(row)):
			thisEntry=dict()
			thisEntry["label"] = str(row[x])
			if ((row[x] in excludeList) or (row[x][0] in excludePattern)):
				thisEntry["use"] = False
			elif (row[x] in includeList):
				thisEntry["use"] = True
			elif (row[x] in passThroughs):
				thisEntry["use"] = True
			else:
				thisEntry["use"] = False
			headers.append(thisEntry)
	#Skip rows, if needed
	for x in range(1,startRow):
		reader.next()
	
	#Now process...
	for row in reader:
		counter = counter +1
		vector=[noVal]*len(includeList)
		thisKey = 0
		for x in range(0,len(row)):
			thisVal = conversions.toNum(row[x],config["noVal"])
			if (thisVal <> noVal):
				if headers[x]["use"]:
					if (headers[x]["label"]==key):
						if (submitId=="0"):
							thisKey = thisVal
						else:
							thisKey = submitId	
					elif (headers[x]["label"]==measure):
						thisMeasure = thisVal
					if (headers[x]["label"] in includeList):
						vector[includeList.index(headers[x]["label"])]= thisVal
					#This block builds the coverage index...
					if (headers[x]["label"] == config["timestamp"]):
						currentTime = thisVal
						if (lastTime == 0):
							#We are just starting
							startTime = currentTime
							lastTime = currentTime					
						elif ((currentTime-lastTime) <= timeOffset):
							#We are still in a continuous block
							lastTime = currentTime
						else:
							#We are in a new interval. startTime and lastTime define a block...
							#s= '{"creationTime":'+json.dumps(datetime.datetime.now(), default=dthandler)+',"endTime":%.8f}' %(lastTime)	
							s= '{"endTime":%.8f}' %(lastTime)	
							indexCounter = indexCounter+1
							#Write the index
							if usePiped:
								#NOTE: Upon number of records = buffer, pipe gets executed below.
								# Pipe contains both coverage index requests AND data
								writePipedRedis(pipe,str(thisKey)+"-Ind", startTime, s,redisLifetime)   
							else:
								writeRedis(redisConnection,str(thisKey)+"-Ind", startTime, s,redisLifetime)	
							#Start a new interval
							startTime = currentTime
							lastTime = currentTime							
		packed = struct.pack('%sd' % len(vector), *vector)
		if usePiped:
			writePipedRedis(pipe,thisKey, thisMeasure, packed,redisLifetime)
			if ((counter % pipeSize) == 0):
				logger.info("\t"+str(counter)+ " rows loaded via pipe")
				#NOTE: This dumps data to both the data collection AND the coverage index....
				pipe.execute()
		else:
			if ((counter % 1000) == 0):
				logger.info("\t"+str(counter)+ " rows loaded")
			writeRedis(redisConnection,thisKey, thisMeasure, packed,redisLifetime)
		packed = None
		buf2 = None
		vector= None
	
		if (submitRate<>0):
			if (counter == submitRecords):
				break
		
	logger.info("Done loading data. "+str(counter) +" rows loaded")

	#Close out the coverage index
	#s= '{"creationTime":'+json.dumps(datetime.datetime.now(), default=dthandler)+',"endTime":%.8f}' %(lastTime)
	s= '{"endTime":%.8f}' %(lastTime)		
	if usePiped:
		writePipedRedis(pipe,str(thisKey)+"-Ind", startTime, s,redisLifetime)
		#Dump anything left in the pipe.
		pipe.execute()
	else:
		writeRedis(redisConnection,str(thisKey)+"-Ind", startTime, s,redisLifetime)		
	logger.info("Done loading coverage index. "+str(indexCounter) +" continuous intervals loaded")


