# Redis Atalaya - Redis Enterprise Cluster
# Redis Enterprise Failover Test - Monitoring Script

### Import section ###
import time
import redis
from redis import Redis
import curses
from datetime import datetime
import argparse
import sys

### Global Config ###
pollIntervalSec=0.5
pollIterations=0
endpointStatus="OK"
hcCommand="ROLE"
lastEpCheck=datetime.now()
time.sleep(0.01)
lastEpError=datetime.now()
endpointStatus='OK'
clessidra="-\|/"
#### Command line argument parsing

parser = argparse.ArgumentParser(prog='ra-rec',description='Test Failover Time on Redis Enterprise Server Endpoints')
parser.add_argument("-e","--endpoint", metavar='<endpoint_id>',default='10000',help='endpoint id')
parser.add_argument("-d","--domain", metavar='<domain>',default='claudio.cs.redislabs.com',help='Domain suffix for the endpoint')
parser.add_argument("-p","--password", metavar='<password>',default='',help='Password of the endpoint')
args = parser.parse_args()

if len(sys.argv)==1:
    parser.print_help()

epHost="redis-" + args.endpoint + "." + args.domain
epPort=args.endpoint        
epPassword=args.password
epAddress=(epHost,epPort)
#print(str(epAddress))



### Functions ###

# helper function for logging
def log(class_id,msg):
	global pollIterations
	msg_classes={0:'[INFO]    ',1:'[WARNING] ',2:'[ERROR]   '}
	print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' ' + msg_classes.get(class_id,'[UNKNOWN]') + clessidra[pollIterations % 4] + ' ' + msg)

# This function is called *ONLY* when last check returned an error
def getEpDowntime(t1,t2):
	return((t2-t1).total_seconds())

# This function does the health check of the endpoint
def healthCheckEndpoint(address):
	global endpointStatus,lastEpCheck,lastEpError
	try:
		r = redis.Redis(host=epHost,port=epPort,password=epPassword,socket_timeout=0.1)
		#role=r.execute_command('ROLE')[0].decode("utf-8")
		role=r.execute_command('info')['run_id']
		if endpointStatus != 'OK':
			log(0,"[ENDPOINT UP] Recovered after " + str((lastEpError-lastEpCheck).total_seconds()))
		log(0,"Endpoint " + str(epAddress) + " Healthy:  run_id " + str(role))
		lastEpCheck=datetime.now()
		endpointStatus='OK'
		return("OK")
	except Exception as errorInstance:
		log(2,"[ENDPOINT DOWN] " + str(errorInstance))
		lastEpError=datetime.now()
		endpointStatus='ERROR'
		return("ERROR")
	return("UNDEFINED")





### MAIN SECTION ###
# Create a poll loop that checks the endpoint and shows of any problems
# This is the main poll loop
endpointStatus='ERROR'
while 1>0:
	pollIterations+=1
	time.sleep(pollIntervalSec)
	endpointStatus=healthCheckEndpoint(epAddress)
	if endpointStatus == 'ERROR':
		#print('[*]' * round(float(getEndpointDowntime())) + '[ ]' * max(1,(15 - round(float(getMasterDowntime())))))
		pass

