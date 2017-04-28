import psutil
import requests
import sys
import time
import os

cpu_load = 0.0
listNode = []
host = None
port = None
workerid = sys.argv[1]
pid = os.getpid()

# read list of node from external file and register it
def init():
	global host
	global port
	context = None
	countWorker = 0
	item = open("ServerList.txt", "r")
	for line in item :
		if len(line) > 0:
			if line[0] == '#':
				if context == None :
					context = "Node"
				elif context == "Node":
					context = "Worker"
			elif line[0] == '>':
				address = line[1:]
				args = address.split(':')
				if context == "Node" :
					listNode.append(address.split('\n')[0])
				elif context == "Worker" :
					countWorker = countWorker + 1
					if countWorker == int(workerid) + 1:
						host = args[0] + ':' + args[1]
						port = args[2].split('\n')[0]

# testing get cpu load
def getCPULoad():
	return psutil.Process(pid).cpu_percent(interval=0.1)

# send cpu load to all node
def sendCPULoad():
	cpu_load = getCPULoad();
	for item in listNode:
		r = requests.get(item + '/' + host + '/' + port + '/cpuload/' + str(cpu_load) + '/' + workerid)
		print(r.url)

if len(sys.argv) != 2:
	print("Usage : python daemon.py [worker id from 0 - 8]")
else :
	init()
	while 1:
		sendCPULoad()
		time.sleep(2) # wait 2 seconds before sending again