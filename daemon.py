import psutil
import requests
import sys
import time

cpu_load = 10.0
listNode = []
host = sys.argv[1]
port = sys.argv[2]

# read list of node from external file and register it
def init():
	item = open("node.txt", "r")
	for line in item:
		listNode.append(line)

# testing get cpu load
def getCPULoad():
	print(psutil.cpu_percent(interval=0.1))

# send cpu load to all node
def sendCPULoad():
	for item in listNode:
		payload = {'load' : cpu_load}
		r = requests.get(item, params=payload)
		print(r.url)

init()
while 1:
	sendCPULoad()
	time.sleep(1) # wait 1 seconds before sending again