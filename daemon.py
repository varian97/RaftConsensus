import psutil
import requests
import sys
import time
import os

cpu_load = 0.0
listNode = []
host = sys.argv[1]
port = sys.argv[2]
pid = os.getpid()

# read list of node from external file and register it
def init():
	item = open("node.txt", "r")
	for line in item:
		listNode.append(line.split('\n')[0])

# testing get cpu load
def getCPULoad():
	return psutil.Process(pid).cpu_percent(interval=0.1)

# send cpu load to all node
def sendCPULoad():
	cpu_load = getCPULoad();
	for item in listNode:
		r = requests.get(item + '/' + host + '/' + port + '/' + str(cpu_load))
		print(r.url)

init()
while 1:
	sendCPULoad()
	time.sleep(2) # wait 2 seconds before sending again