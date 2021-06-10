# nanny - redis replication cluster monitor (c) 2021-05-30

# NOTE: We use the word Cluster, ReplicationCluster, Group, ReplicationGroup interchangeably
# They all denote one single group of Redis servers that have Master/Replica relationship
# The name *MUST* match a "master" name in Sentinel configuration

from redis.sentinel import Sentinel
import time
import redis
from redis import Redis
import curses
from datetime import datetime

# Using class mostly for scope reasons, no class specific features used.
class Nanny:
	def __init__(self):
		self.ClusterName='myReplicationCluster'
		self.SentinelsList=[('172.16.1.147', 26379),('172.16.10.171',26379),('172.16.0.233',26379)]
		self.CurrentSentinelIndex=0
		self.CurrentMaster=[('1.1.1.1',1111)]
		self.CurrentSentinel=[('1.1.1.1', 1111)]
		self.MasterStatus='OK'
		self.LastMasterCheck=datetime.now()
		self.LastMasterOK=datetime.now()
		self.LastMasterERROR='0000-00-00'
		self.PollIntervalSec=1
		self.PollLastRun='0000-00-00'
		self.SentinelsUpdateIntervalSec=10
		self.SentinelsUpdateLastRun='0000-00-00'
		self.MaxDowntime=0
		self.CurrentDowntime=0	


######## GLOBAL CONFIGURATION ########

gIterations=0
gSentinelIterationMultiplier=30

__ERROR='ERROR'
__OK='OK'
__NOTMASTER='NOTMASTER'

#####################################

def log(class_id,msg):
	msg_classes={0:'[INFO]    ',1:'[WARNING] ',2:'[ERROR]   '}
	print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + ' ' + msg_classes.get(class_id,'[UNKNOWN]') + msg)

####### FUNCTIONS ########

# Returns the current Master given the Cluster/Group name
def getMaster(pClusterName):
# Returns value is a tuple of two values: ip,port
	numOfCurrentSentinels = len(n.SentinelsList)
	failedSentinels=0
	# We need to scan all Sentinels we have configured because some of them could be down, so in case of problem connecting to one Sentinel we skip to the next
	for s in n.SentinelsList:
		try:
			log(0,"Using Sentinel at " + str(s) + " out of total " +str(numOfCurrentSentinels) + " instances")
			sentinel = Sentinel([s], socket_timeout=0.1)
			connectionTest = sentinel.discover_slaves(pClusterName)
			return sentinel.discover_master(pClusterName)
		except Exception as errorInstance:
			log(2,str(errorInstance))
			if 'No master found' in str(errorInstance):
				#("No Master Found")
				pass
			else:
				log(2,"[SENTINEL DOWN]")
				failedSentinels+=1
				print("Number of failing Sentinels " , failedSentinels, " out of " , numOfCurrentSentinels, " instances")
			if failedSentinels == numOfCurrentSentinels:
				print("!!! None of the Sentinels is reachable, will try in 5 seconds")
				time.sleep(5)
				return [('nosentinels',-1)]
	return [('none',-1)]

# Check if Master is responsive and 'additionally' double checks if it's actually the Master
def healthCheckMaster(pCurrentMaster):
# Returns a literal with: 'ok','error','not-master'
	# does it answer? YES=OK, NO=ERROR
	# if YES, is it MASTER? YES=OK, NO=NOT-MASTER
	try:
		r = redis.Redis(host=n.CurrentMaster[0],port=n.CurrentMaster[1],password='somePass')
		role=r.execute_command('ROLE')[0].decode("utf-8")
		if role != 'master':
			log(1,"This is not a Master! It's a " + role)
			n.MasterStatus='ERROR'
			n.LastMasterError=datetime.now()
			# This is a Slave for some reason, maybe some failover happened, so we ask again the Sentinels who is the current Master
			n.CurrentMaster=getNewMaster
		else:
			# If it was not 'OK' before it means it just recoverd from a failure
			# So we will calculate the downtime between NOW and the previous last OK check
			if n.MasterStatus != 'OK':
				log(0,"[MASTER UP] Recovered the Master after " + getMasterDowntime())
			n.MasterStatus='OK'
			n.LastMasterCheck=datetime.now()
			n.LastMasterOK=datetime.now()
			return 'OK'
	except:
		n.MasterStatus='ERROR'
		n.LastMasterError=datetime.now()
		log(2,"[MASTER DOWN] Connection Error!   Master " + str(n.CurrentMaster) + " unreachable for " + getMasterDowntime())
		
		### try to get a new Master
		getNewMaster=getMaster(n.ClusterName)
		if getNewMaster != n.CurrentMaster:
			if getNewMaster != [('none',-1)]:
				n.CurrentMaster = getNewMaster
				log(0,"A New Master has been found at " + str(getNewMaster))
		return 'ERROR'

# This function is called *ONLY* when last check returned an error
def getMasterDowntime():
	return(str((n.LastMasterError - n.LastMasterCheck).total_seconds()))

def sentinelDiscovery(pSentinelsList):
# NO OP for now
# ask one of the sentinels for the updated list of sentinels and in case updates it (remove/add)
# loop in sentinels list
	log(0,"-"*20 + "sentinel_discovery_begin" + "-"*20)
	log(0,"Discovering Sentinels (every " + str(gSentinelIterationMultiplier) + " master check polls)  === ONLY informative for now, no merge ===")
	latestSentinels=[]
	for s in n.SentinelsList:
		try:
			#Xsentinel = Sentinel([s], socket_timeout=0.1)
			Xsentinel = Redis(s[0],s[1])
			log(0," -- discovery performed by " + s[0] + ":" + str(s[1]) + " -- ")
			listSentinel=Xsentinel.execute_command('SENTINEL SENTINELS myReplicationCluster')
			for k in listSentinel:
				sKeys=k[0::2]
				sValues=k[1::2]
				dictSwap= dict(zip(sKeys, sValues))
				dictSentinel = {k.decode(): v.decode() for k,v in dictSwap.items()}
				log(0," -- discovered Sentinel -- " + dictSentinel["ip"] + ":" + dictSentinel["port"] + " Last Hello (ms) " + dictSentinel["last-hello-message"])
				latestSentinels.append(dictSentinel)
				# To complete this I should merge configured_Sentinels U discovered_Sentinels

			log(0,"-"*21 + "sentinel_discovery_end" + "-"*21)
			return 1
		except Exception as errorInstance:			
			log(2,str(errorInstance))		
	log(0,"-"*40)
	return 1

	
def setMaxDowntime(pCurrentDowntime):
	n.MaxDowntime=max(n.MaxDowntime,pCurrentDowntime)

#### MAIN ####

## initialization

n = Nanny()
#read command line parameters and set class variables accordingly
print("*-" *40)
print("Nannyi (c) 2020-05")
print("-*" *40)
print("Starting 'nanny' with the following parameters: ")
print("Main poll time (sec): ",n.PollIntervalSec)
print("New Sentinels discovery: every ",gSentinelIterationMultiplier," polls")
print("Initial Sentinels list: ", n.SentinelsList)
print("-*" *40)
# Update sentinelsList
sentinelDiscovery(n.SentinelsList)
# Get Master
n.CurrentMaster=getMaster(n.ClusterName)
if n.CurrentMaster[0] != 'none':
	log(0,"[MASTER UP] Found Master at " + str(n.CurrentMaster[0]) + " : " + str(n.CurrentMaster[1]))
else:
	log(0,"Couldn't find a Master")

# This is the main poll loop
while 1>0:
	gIterations+=1
	time.sleep(n.PollIntervalSec)
	n.MasterStatus=healthCheckMaster(n.CurrentMaster)
	if n.MasterStatus == 'ERROR':
		print('[*]' * round(float(getMasterDowntime())) + '[ ]' * max(1,(15 - round(float(getMasterDowntime())))))
		pass
	## Every gSentinelIteraionMultiplier main poll loops we also refresh the sentinels list
	if gIterations % gSentinelIterationMultiplier == 0:
		sentinelDiscovery(n.SentinelsList)	
	if gIterations % 10 == 0:
		log(0,"Current Master is at: " + str(n.CurrentMaster) + " (polling every " + str(n.PollIntervalSec) + " seconds)"  )

