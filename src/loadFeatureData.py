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

blockcounter = 0
blockDatacounter = 0

def writePipedRedis(pipe,key, score, member,lifetime):
	pipe.zadd(key, score, member)
	if lifetime >0:
		pipe.expire(key, lifetime) 

def writeRedis(r,key, score, member,lifetime):
	r.zadd(key, score, member)
	if lifetime >0:
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

def writeCoverage(theseTimes,pipe,usePiped,pipeSize,redisConnection,thisKey,logger):
	global blockcounter
	print("Coverage Counter: "+str(blockcounter))
	startTime=theseTimes[0]
	maxTime=len(theseTimes)
	if "." in str(theseTimes[maxTime-1]):
		s= '{"endTime":%.8f}' %(theseTimes[maxTime-1])
	else:
		s= '{"endTime":%i}' %(theseTimes[maxTime-1])
	#print("Continuous: "+str(theseTimes)+"  StartTime: "+str(startTime)+"  S: "+s)
	if usePiped:
		writePipedRedis(pipe,str(thisKey)+"-Ind", startTime, s, 0)
		if ((blockcounter % pipeSize) == 0): 
			logger.info("\t"+str(blockcounter)+ " coverage rows loaded via pipe")
			pipe.execute()
	else:
		logger.info("\t"+str(blockcounter)+ " coverage rows loaded")
	 	writeRedis(redisConnection,str(thisKey)+"-Ind", startTime, s, 0)

def writeData(theseTimes,vector,pipe,usePiped,pipeSize,redisConnection,thisKey,logger):
	global blockDatacounter
	print("Data Counter: "+str(blockDatacounter))
	startTime=theseTimes[0]
	packed = struct.pack('%sd' % len(vector), *vector)
	if usePiped:
		writePipedRedis(pipe,str(thisKey), startTime, packed, 0)
		if ((blockDatacounter % pipeSize) == 0): 
			logger.info("\t"+str(blockDatacounter)+ " data rows loaded via pipe")
			pipe.execute()
	else:
		logger.info("\t"+str(blockcounter)+ " data rows loaded")
	 	writeRedis(redisConnection,str(thisKey), startTime, packed, 0)

def writeNRows(redisConnection,pipe,usePiped,pipeSize,reader, num, timeMultiplier,contThreshold,logger,headers,key,submitId,includeList,measure):
	global blockcounter
	global blockDatacounter
	previous = 0
	noVal=-1.0
	theseTimes=[]
	vector=[]
	for x in range(0,num):
		row = reader.next()
		for x in range(0,len(row)):
			thisVal = conversions.toNum(row[x],noVal)
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
						vector.append(thisVal)
		blockcounter = blockcounter + 1
		blockDatacounter = blockDatacounter + 1
		thisKey = int(row[0])
		if timeMultiplier>1:
			thisTime = int(float(row[1])*int(timeMultiplier))
		else:
			thisTime = row[1]
		if (previous == 0) or ((thisTime-previous)<=contThreshold):
			theseTimes.append(thisTime)
		else:
			if len(theseTimes)>0:
				writeCoverage(theseTimes,pipe,usePiped,pipeSize,redisConnection,thisKey,logger)
				writeData(theseTimes,vector,pipe,usePiped,pipeSize,redisConnection,thisKey,logger)
				theseTimes=[]
				vector=[]
		previous = thisTime
	if len(theseTimes)>0:
		writeCoverage(theseTimes,pipe,usePiped,pipeSize,redisConnection,thisKey,logger)
		writeData(theseTimes,vector,pipe,usePiped,pipeSize,redisConnection,thisKey,logger)
	
def loadFileBlocks(path, key, measure,redisConnection,usePiped, pipe, config, logger, startRow=0):
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
	timeMultiplier=int(config["timeMultiplier"])
	includeList=config["includeList"]
	passThroughs=config["passThroughList"]
	excludePattern=config["excludePattern"]
	redisLifetime=int(config["redisLifetime"])
	
	if "." in str(config["continuous"]):
		contThreshold=float(config["continuous"])*int(config["multiplier"])
	else:
		contThreshold=int(config["continuous"])*int(config["multiplier"])
	submitRate = int(config["submitRate"])
	submitRecords = int(config["submitRecords"])
	submitId = config["submitId"]
	
	pipeSize = int(config["pipeSize"])
	timeOffset = config["continuous"] * config["multiplier"]
	currentStart = 0
	currentTime = 0
	lastTime = 0
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
	done = False
	while not done:
		try:
			for x in range(1,6):
				writeNRows(redisConnection,pipe,usePiped,pipeSize,reader,x,timeMultiplier, contThreshold,logger,headers,key,submitId,includeList,measure)
		except:
			done = True

	if usePiped:
		pipe.execute()
