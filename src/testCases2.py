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
logger = logging.getLogger('testCases')

def initLog():
	logger = logging.getLogger('testCases')
	timeStr = time.strftime("%d%m%Y%H%M%S")
	hdlr = logging.FileHandler(config["logFile"]+timeStr+".log")
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
		print ('testCases.py -c <configfile>')
      		sys.exit(2)
	for opt, arg in opts:
      		if opt == '-h':
         		print ('testCases.py -c <configfile>')
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

	#if needed...
	loadFeatureData.loadFileBlocks(config["dataFilePath"],config["redisKey"],config["redisMeasure"],r,usePiped,pipe,config, logger)

#	ifile  = open(config["testCaseFilePath"], "rb")
#	reader = csv.reader(ifile,delimiter='|')	
#	for row in reader:
#		testcase=row[0]
#		key=row[1]+"-Ind"
#		startTime=float(row[2])
#		endTime=float(row[3])
#		s = str(row[4])
#		expected = json.loads(s)
#		results = getCoverage.getCoverage(r,key,startTime,endTime,config["continuous"],int(config["multiplier"]))
#		if (results == expected):
#			logger.info(testcase+": "+"PASSED!")
#		else: 
#			logger.info(testcase+": "+"FAILED!")
#			logger.info("        Expected: "+json.dumps(expected))
#			logger.info("        Got:      "+json.dumps(results))
#
#	logger.info('Finished Run  ========================================')
if __name__ == "__main__":
	main(sys.argv[1:])

