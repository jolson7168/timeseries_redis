import redis
import sys
import getopt
import logging
import time
import loadFeatureData
import getCoverage
import getFeatureData
import csv
import json
from redis.sentinel import Sentinel

#globals
config = {}
logger = logging.getLogger('loadData')

def initLog():
	logger = logging.getLogger('loadData')
	timeStr = time.strftime("%d%m%Y%H%M%S")
	hdlr = logging.FileHandler(config["logFileLoadData"]+timeStr+".log")
	formatter = logging.Formatter(config["logFormat"],config["logTimeFormat"])
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr) 
	logger.setLevel(logging.INFO)
	return logger

def getSentinelConnection(sentinelHosts):
	sentinel = Sentinel(sentinelHosts, socket_timeout=0.1)
	master = sentinel.discover_master('mymaster')
	r = redis.StrictRedis(master[0], port=int(master[1]), db=int(config["redisDb"]))
	return r

def getConnection():
	r = redis.StrictRedis(config["redisHost"], config["redisPort"], db=int(config["redisDb"]))
	return r

#Start Main Here
def main(argv):
 	try:
      		opts, args = getopt.getopt(argv,"hc:",["configfile="])
	except getopt.GetoptError:
		print ('loadTestData.py -c <configfile>')
      		sys.exit(2)
	for opt, arg in opts:
      		if opt == '-h':
         		print ('loadTestData.py -c <configfile>')
         		sys.exit()
		elif opt in ("-c", "--configfile"):
			configFile=arg
			try:
   				with open(configFile): pass
			except IOError:
   				print ('Configuration file: '+configFile+' not found')
				sys.exit(2)
	execfile(configFile, config)
	logger=initLog()
	logger.info('Starting Run  ========================================')

	submitRate = int(config["submitRate"])
	submitRecords = int(config["submitRecords"])
	fromRow = int(config["startRow"])
	hosts = config["sentinelHosts"]
	sentinelHosts = []
	for addr in hosts:
		tup=(str(addr[0]),int(addr[1]))
		sentinelHosts.append(tup)
	usePiped = False
	if (config["redisUsePiped"] == "True"):
		usePiped = True
	useSentinel = False
	if (config["redisUseSentinel"] == "True"):
		useSentinel = True
	if (submitRate==0):
		if useSentinel:
			r = getSentinelConnection(sentinelHosts)
		else:
			r = getConnection()		
		pipe = r.pipeline(not usePiped)
		loadFeatureData.loadFile(config["dataFilePath"],config["redisKey"],config["redisMeasure"],r,usePiped,pipe,config, logger,fromRow)
	else:
		done=False		
		while not done:	
			if useSentinel:
				r = getSentinelConnection(sentinelHosts)
			else:
				r = getConnection()
			pipe = r.pipeline(not usePiped)		
			loadFeatureData.loadFile(config["dataFilePath"],config["redisKey"],config["redisMeasure"],r,usePiped,pipe,config, logger,fromRow)
			logger.info('Sleeping for '+str(submitRate)+' seconds...')
			time.sleep(submitRate)
			fromRow = fromRow+submitRecords
			r=None
if __name__ == "__main__":
	main(sys.argv[1:])
