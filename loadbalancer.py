import sys, time

#const static
IP = 0
PORT = 1
ON = 1
OFF = 0
SERVER_FILE = "ServerList.txt"
NEW_LINE = '\n'
STD_HEARTBEAT = None

#const from file
N_NODE = None
N_WORKER = None
DATA_FILE = None
ID = None

#dict
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
					NODE_DICT[count] = [args[IP], args[PORT].split(NEW_LINE)[0]]
					count += 1
				elif context == "Worker" and count < N_WORKER:
					WORKER_DICT[count] = [args[IP], args[PORT].split(NEW_LINE)[0]]
					LOAD_DICT[count] = 0
					STATUS_DICT[count] = OFF
					count += 1

def timer():
	while 1:
		past = time()
		if not IS_LEADER:
			#init TIMEOUT
			while not IS_LEADER and TIMEOUT > 0:
				now = time()
				TIMEOUT -= now - past
				past = now	
			if not IS_LEADER:
				TERM += 1
				IS_ELECTION = True
				UPVOTE = 1
				DOWNVOTE = 0
				#send vote request here
		else:
			HEARTBEAT = STD_HEARTBEAT
			while IS_LEADER and HEARTBEAT > 0:
				now = time()
				HEARTBEAT -= now-past
				past = now
			if IS_LEADER:
				print ("g")
				#broadcast array to all
	

if (len(sys.argv) != 2):
	print ("Please use ID (0 <= ID < number of node) as argv")
else:
	ID = sys.argv[1]
	init()
	print (NODE_DICT)
	print (WORKER_DICT)

