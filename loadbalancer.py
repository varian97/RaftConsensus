import sys, time
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
DAEMON_TIMEOUT = {}
HEARTBEAT = None


class NodeHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        try:
            args = self.path.split('/')
            if len(args) < 2:
                raise Exception()
            self.send_response(200)
            self.end_headers()
			if len(args) == 2:
				n = int(args[1])
				print("request to worker")
			else:
				n = int(args[3])
				if n == "voteRequest":
					print("send your vote here")
				elif n == "upvote":
					print("increment upvote")
				elif n == "downvote":
					print("increment downvote")
				elif n == "data":
					print("check local and reply")
				elif n == "positive":
					print("increment till majority")
				elif n == "negative":
					print("reply with prev data")
				elif n == "load":
					print("add to array")
			
            self.wfile.write(str(self.calc(n)).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)


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
					N_NODE = int(line.split('|')[1].split(NEW_LINE)[0])
					count = 0
				elif context == "Node":
					context = "Worker"
					count = 0
					N_WORKER = int(line.split('|')[1].split(NEW_LINE)[0])
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

def daemonTimer():
	past = clock()
	while IS_LEADER:
		now = clock()
		for i in range(N_WORKER)
			DAEMON_TIMEOUT[i] -= now - past
			if DAEMON_TIMEOUT[i] < 0:
				STATUS_DICT[i] = OFF
		past = now	
		
def nodeTimer():
	while 1:
		past = clock()
		#init random TIMEOUT
		while not IS_LEADER and TIMEOUT > 0:
			now = clock()
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
					print ("send vote request to node " + str(i))
		while IS_LEADER:
			for i in range (N_NODE):
						if i != ID:
							print ("send data to node " + str(i))
			time.sleep(STD_HEARTBEAT)
				
	

if (len(sys.argv) != 2):
	print ("Please use ID (0 <= ID < number of node) as argv")
else:
	ID = sys.argv[1]
	init()
	#start timer here
	#start server here

