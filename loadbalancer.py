import sys, time, _thread, random, requests
import json
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

#const static
IP = 0
PORT = 1
ON = 1
OFF = 0
SERVER_FILE = "ServerList.txt"
NEW_LINE = '\n'
STD_HEARTBEAT = None
STD_DAEMON_TIMEOUT = None

#const from file
N_NODE = None
N_WORKER = None
DATA_FILE = None
ID = None

#address dict (ID->[IP,PORT])
WORKER_DICT = {}
NODE_DICT = {}

#RAFT stored variables
LOAD_DICT = {}
STATUS_DICT ={}
TERM = None

#RAFT runtime variables
IS_ELECTION = False
IS_LEADER = False
TIMEOUT = 0

#Election variables
UPVOTE = 0
DOWNVOTE = 0
VOTE_TERM = 0

#Leader variables
TOP_DICT = {}
COMMIT_DICT = {}
DAEMON_TIMEOUT = {}
HEARTBEAT = None

def init() :
	global N_NODE
	global N_WORKER
	global DATA_FILE
	global WORKER_DICT
	global NODE_DICT
	global LOAD_DICT
	global STATUS_DICT
	
	DATA_FILE = "Data_" + str(ID) + ".txt"
	context = None
	file = open(SERVER_FILE, "r")
	for line in file:
		if len(line) > 0:
			if line[0] == '#':
				if context == None :
					context = "Node"
					N_NODE = int(line.split(':')[1].split(NEW_LINE)[0])
					count = 0
				elif context == "Node":
					context = "Worker"
					count = 0
					N_WORKER = int(line.split(':')[1].split(NEW_LINE)[0])
			elif line[0] == '>':
				address = line[1:]
				args = address.split(':')
				if context == "Node" and count < N_NODE:
					NODE_DICT[count] = [args[0]+":"+args[1], args[2].split(NEW_LINE)[0]]
					count += 1
				elif context == "Worker" and count < N_WORKER:
					WORKER_DICT[count] = [args[0]+":"+args[1], args[2].split(NEW_LINE)[0]]
					LOAD_DICT[count] = 0
					STATUS_DICT[count] = OFF
					count += 1
	

def daemonTimer():
	past = time.clock()
	while IS_LEADER:
		now = time.clock()
		for i in range(N_WORKER):
			DAEMON_TIMEOUT[i] -= now - past
			if DAEMON_TIMEOUT[i] < 0:
				STATUS_DICT[i] = OFF
		past = now	
		
def nodeTimer():
	global TERM
	global TIMEOUT
	global IS_ELECTION
	global UPVOTE
	global DOWNVOTE
	global VOTE_TERM
	while 1:
		past = time.clock()
		TIMEOUT = random.uniform(2.0, 4.0)
		while not IS_LEADER and TIMEOUT > 0:
			now = time.clock()
			TIMEOUT -= now - past
			past = now	
		if not IS_LEADER:
			TERM += 1
			IS_ELECTION = True
			UPVOTE = 1
			DOWNVOTE = 0
			VOTE_TERM = TERM
			for i in range (N_NODE):
				if i != ID:
					print ("send vote request to node " + str(i) + " for term " + str(TERM))
					print(NODE_DICT[i][IP] + ":" + NODE_DICT[i][PORT]+"/voteRequest/"+str(ID)+"/"+str(TERM)+"/"+str(COMMIT_DICT[ID]))
					try:
						r = requests.get(NODE_DICT[i][IP] + ":" + NODE_DICT[i][PORT]+"/voteRequest/"+str(ID)+"/"+str(TERM)+"/"+str(COMMIT_DICT[ID]))
					except:
						print("request fail for node "+str(i))
					
		while IS_LEADER:
			for i in range (N_NODE):
				if i != ID:
					print ("send data to node " + str(i))
			time.sleep(STD_HEARTBEAT)

#class for http connection between node2node and node2worker
class ListenerHandler(BaseHTTPRequestHandler):			
	
	def do_GET(self):
		try:
			args = self.path.split('/')
			if len(args) < 2:
				raise Exception()
			self.send_response(200)
			self.end_headers()
			# handle each request based on its type
			if len(args) == 2:
				n = int(args[1])
				print("request to worker")
			else:
				n = int(args[3])
				if n == "voteRequest":
					print("send your vote here")
					if (TERM < args[5] and COMMIT_DICT[ID] <= args[6]):
						r = requests.get(NODE_DICT[int(args[4])][IP] + ":" + NODE_DICT[int(args[4])][PORT]+"/upvote/"+str(ID)+"/"+str(TERM))
						TERM = args[5]
						IS_LEADER = False
						IS_ELECTION = False
						VOTE_TERM = TERM
					else:
						r = requests.get(NODE_DICT[int(args[4])][IP] + ":" + NODE_DICT[int(args[4])][PORT]+"/downvote/"+str(ID)+"/"+str(TERM))
				elif n == "upvote":
					print("increment upvote")
				elif n == "downvote":
					print("increment downvote")
				elif n == "data":
					print("check local and reply")
					if not IS_LEADER:
						# check if commit equals or not
						countCommit = 0
						leaderCommit = args[4]
						f = open(DATA_FILE, 'r')
						for line in f:
							countCommit = countCommit + 1

						if countCommit == leaderCommit - 1:
							# parsing the data from url into local load dict
							LOAD_DICT = json.loads(args[5])
							for key in LOAD_DICT:
								STATUS_DICT[key] = ON

							# copy data from temp dict into storage
							commit_data = open(DATA_FILE, 'w')
							for key, value in LOAD_DICT.items():
								#format file commit : worker_id | cpuload | status
								commit_data.write(key + '|' + value + '|' + STATUS_DICT[key] + NEW_LINE)

							# send commit to leader
							try:
								id_leader = args[2]
								r = requests.get(NODE_DICT[id_leader][IP] + ":" + NODE_DICT[id_leader][PORT] + "/positive")
							except:
								print("Send positive response failed for node "+str(ID))
						elif countCommit < leaderCommit - 1:
							# send negative
							id_leader = args[2]
							try :
								r = requests.get(NODE_DICT[id_leader][IP] + ":" + NODE_DICT[id_leader][PORT] + "/negative/" + countCommit + "/" + str(ID))
							except :
								print("Send negative response failed for node "+str(ID))

				elif n == "positive":
					print("increment till majority")
				elif n == "negative":
					print("reply with prev data")
				elif n == "cpuload":
					# process the cpu load if the current node is a leader
					if IS_LEADER:
						# collect the data
						fromHost = args[1]
						fromPort = args[2]
						cpuload = args[4]
						workerid = args[5]

						if workerid in WORKER_DICT:
							print("worker with id %d is found" % (workerid))
							print("from host : " + fromHost + ":" + fromPort + " with the cpu load = " + cpuload)
							LOAD_DICT[workerid] = [cpuload]
							STATUS_DICT[workerid] = ON

							#REPLICATES DATA TO OTHER NODE
							for i in range(N_NODE):
								if i != ID:
									try:
										cpuloadJSON = json.dumps(LOAD_DICT)
										r = requests.get(NODE_DICT[i][IP] + ":" + NODE_DICT[i][PORT]+"/data/"+str(ID)+"/"+str(TERM)+"/"+str(sorted(COMMIT_DICT.keys())[-1] + 1) + "/", params=cpuloadJSON)
										print(r.url)
									except:
										print("replication fail for node "+str(i))

						else:
							print("Sorry the worker %d is not defined..." % (workerid))
			
		except Exception as ex:
			self.send_response(500)
			self.end_headers()
			print(ex)
					
					
if (len(sys.argv) != 2):
	print ("Please use ID (0 <= ID < number of node) as argv")
else:
	ID = int(sys.argv[1])
	TERM = 0
	COMMIT_DICT[ID] = 0
	init()
	
	#start timer here
	try:
	   _thread.start_new_thread(nodeTimer, ())
	except:
	   print ("Error: unable to start thread")
	
	#start server here
	server = HTTPServer(("", int(NODE_DICT[ID][PORT])), ListenerHandler)
	server.serve_forever()