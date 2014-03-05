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

	r = redis.StrictRedis(config["redisHost"], port=int(config["redisPort"]), db=int(config["redisDb"]))
	usePiped = False
	if (config["redisUsePiped"] == "True"):
		usePiped = True
	pipe = r.pipeline(not usePiped)
	submitRate = int(config["submitRate"])
	submitRecords = int(config["submitRecords"])
	fromRow = int(config["startRow"])
	if (submitRate==0):
		loadFeatureData.loadFile(config["dataFilePath"],config["redisKey"],config["redisMeasure"],r,usePiped,pipe,config, logger,fromRow)
	else:
		done=False		
		while not done:
			loadFeatureData.loadFile(config["dataFilePath"],config["redisKey"],config["redisMeasure"],r,usePiped,pipe,config, logger,fromRow)
			logger.info('Sleeping for '+str(submitRate)+' seconds...')
			time.sleep(submitRate)
			fromRow = fromRow+submitRecords

if __name__ == "__main__":
	main(sys.argv[1:])
