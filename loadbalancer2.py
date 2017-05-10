import sys, time, _thread, random, requests, math, json
import urllib.parse as urlparse
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

# const static
IP = 0
PORT = 1
ON = 1
OFF = 0
LOAD = 0
STATUS = 1
DATA_TERM = 0
DATA_INDEX = 1
SERVER_FILE = "ServerList.txt"
NEW_LINE = '\n'
STD_HEARTBEAT = 2
STD_DAEMON_TIMEOUT = None
TIMEOUT_RANGE = 5

# const from file
N_NODE = 0
N_WORKER = 0
DATA_FILE = ""
ID = 0

# address dict (ID->[IP,PORT])
WORKER_DICT = {}
NODE_DICT = {}
WORKER_ADDR = {}

# RAFT stored variables
DATA_DICT = {}
META_DATA = {}
TERM = 0
INDEX = None

# RAFT runtime variables
IS_ELECTION = False
IS_LEADER = False
TIMEOUT = 0

# Election variables
UP_VOTE = 0
DOWN_VOTE = 0
VOTE_TERM = 0

# Leader variables
HEARTBEAT = 0
DAEMON_TIMEOUT = {}
COMMIT_COUNTER = {}


def init():
    global N_NODE
    global N_WORKER
    global DATA_FILE
    global WORKER_DICT
    global NODE_DICT
    global HEARTBEAT
    global ID
    global TERM
    global INDEX

    DATA_FILE = "Data_" + str(ID) + ".txt"
    context = None
    file = open(SERVER_FILE, "r")
    datafile = open(DATA_FILE, "a+").readlines()
    count = 0
    for line in file:
        if len(line) > 0:
            if line[0] == '#':
                if context is None:
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
                    NODE_DICT[count] = [args[0] + ":" + args[1], args[2].split(NEW_LINE)[0]]
                    count += 1
                elif context == "Worker" and count < N_WORKER:
                    WORKER_DICT[count] = [args[0] + ":" + args[1], args[2].split(NEW_LINE)[0]]
                    count += 1
    TERM = getLastTerm()
    INDEX = getLastCommit()

    f = open("WorkerList.txt", "r")
    i = 0
    for line in f:
    	WORKER_ADDR[i] = line
    	i+=1


def daemonTimer():
    global DATA_DICT
    global DAEMON_TIMEOUT
    past = time.clock()
    while IS_LEADER:
        now = time.clock()
        for i in range(N_WORKER):
            if DAEMON_TIMEOUT[i] > 0:
                DAEMON_TIMEOUT[i] -= now - past
            if DAEMON_TIMEOUT[i] <= 0:
                DATA_DICT[i][STATUS] = OFF
        past = now


def nodeTimer():
    global TERM
    global TIMEOUT
    global IS_ELECTION
    global UPVOTE
    global DOWNVOTE
    global VOTE_TERM
    global IS_LEADER

    while 1:
        # if ID == 0:
        #	IS_LEADER = True
        past = time.clock()
        TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)
        while not IS_LEADER and TIMEOUT > 0:
            now = time.clock()
            TIMEOUT -= now - past
            past = now
        if not IS_LEADER:
            UPVOTE = 1
            DOWNVOTE = 0
            IS_ELECTION = True
            TERM += 1
            VOTE_TERM = TERM
            for i in range(N_NODE):
                if i != ID:
                    try:
                        # print ("send vote request to node " + str(i) + " for term " + str(TERM))
                        r = requests.get("{node_ip}:{node_port:d}/voteRequest/{node_id:d}/{term:d}/{commit}".format(
                            node_ip=NODE_DICT[i][IP], node_port=NODE_DICT[i][PORT], node_id=ID, term=TERM,
                            commit=getLastCommit()), timeout=0.01)
                    except:
                        pass
        while IS_LEADER:
            print("# BROADCAST")
            for i in range(N_NODE):
                if i != ID:
                    sendCommit(i, 0)
                    print("send commit to node {node_id}".format(node_id=i))
            HEARTBEAT = STD_HEARTBEAT
            past = time.clock()
            while IS_LEADER and HEARTBEAT > 0:
                now = time.clock()
                HEARTBEAT -= now - past
                past = now
            pass


def getLastTerm():
    f = open(DATA_FILE, 'r').readlines()
    return int(f[0].split('*')[0])


def getLastCommit():
    f = open(DATA_FILE, 'r').readlines()
    return int(f[0].split('*')[1])


def readData():
    global META_DATA
    global DATA_DICT
    f = open(DATA_FILE, 'r').readlines()
    dataString = f[0].split('*')[2].split('\n')[0]
    META_DATA[DATA_TERM] = getLastTerm()
    META_DATA[DATA_INDEX] = getLastCommit()
    for item in dataString.split('&'):
        values = item.split('|')
        DATA_DICT[int(values[0])] = [float(values[1]), int(values[2])]


def commitData():
    text = str(META_DATA[DATA_TERM]) + "*" + str(META_DATA[DATA_INDEX]) + "*"
    for i in range(N_WORKER):
        if (i != 0):
            text += '&'
        text += str(i) + "|" + str(DATA_DICT[i][LOAD]) + "|" + str(DATA_DICT[i][STATUS])
    f = open(DATA_FILE, 'w')
    f.write(text)
    f.close()


def getMinIdx():
	DATA_DICT = {}
	f = open(DATA_FILE, 'r').readlines()
	dataString = f[0].split('*')[2].split('\n')[0]
	for item in dataString.split('&'):
		values = item.split('|')
		DATA_DICT[int(values[0])] = [float(values[1]),int(values[2])]
	idx = -1
	for i in range(N_WORKER):
		if idx < 0:
			if DATA_DICT[i][STATUS] == ON:
				idx = i
		else:
			if DATA_DICT[i][STATUS] == ON and DATA_DICT[i][LOAD] < DATA_DICT[idx][LOAD]:
				idx = i
	return idx

#class for http connection between node2node and node2worker
class ListenerHandler(BaseHTTPRequestHandler):			
	
	def do_GET(self):
		global TERM
		global IS_LEADER
		global IS_ELECTION
		global VOTE_TERM
		global UPVOTE
		global DOWNVOTE
		global INDEX
		global DATA_DICT
		global META_DATA
		global DAEMON_TIMEOUT
		
		try:
			self.send_response(200)
			self.end_headers()
			args = self.path.split('/')
			if len(args) < 2:
				raise Exception()

			# handle each request based on its type
			if len(args) == 2:

				# send request from client to worker
				n = int(args[1])
				if getMinIdx() >= 0:
					r = requests.get(WORKER_ADDR[getMinIdx()].split('\n')[0] + "/" + args[1], timeout = 0.5)
					print(r.url)
					self.wfile.write(str(int(r.text)).encode('utf-8'))
					'''print("INI CETAK URL ")
					url = WORKER_ADDR[getMinIdx()] + "/" + args[1]
					print(url)
					self.send_response(301)
					self.send_header('Location', url)
					self.end_headers()'''
				else:
					self.wfile.write(str("No active worker").encode('utf-8'))
				#print the result
				
			else:
				n = args[1]
				if n == "voteRequest":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					if (TERM < int(args[3])):
						try:
							#print("send upvote to node" +args[2]+" for term "+args[3])
							r = requests.get(NODE_DICT[int(args[2])][IP] + ":" + str(NODE_DICT[int(args[2])][PORT])+"/upvote/"+str(ID)+"/"+args[3], timeout = 0.01)
						except:
							pass
						if IS_LEADER:
							print("Node " + str(ID) + " berhenti menjadi leader")
						IS_LEADER = False
						IS_ELECTION = False
						TERM = int(args[3])
						VOTE_TERM = TERM
					else:
						try:
							#print("send downvote to node" +args[2]+" for term "+args[3])
							r = requests.get(NODE_DICT[int(args[2])][IP] + ":" + str(NODE_DICT[int(args[2])][PORT])+"/downvote/"+str(ID)+"/"+args[3], timeout = 0.01)
						except:
							pass
						
				elif n == "upvote":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					if int(args[3]) == TERM and IS_ELECTION:
						UPVOTE+=1
						#print ("Receive upvote from "+args[2]+" for term "+args[3])
						if UPVOTE > math.floor(N_NODE/2): #harusnya ad validasi siapa yg udah vote, tp aku males, ntar aj
							print ("Node "+str(ID)+" menjadi leader")
							
							IS_ELECTION = False
							TERM = int (args[3])
							VOTE_TERM = TERM
							readData()
							IS_LEADER = True

							for i in range (N_WORKER):
								DAEMON_TIMEOUT[i] = STD_DAEMON_TIMEOUT
							
							try:
								_thread.start_new_thread(daemonTimer, ())
							except:
								print("Error: unable to start thread")
							#set semua daemon_timer ke std_daemon_timeout
							#jalankan timer		
							
				elif n == "downvote":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					if int(args[3]) == TERM and IS_ELECTION:
						DOWNVOTE+=1
						#print ("Receive downvote from "+args[2]+" for term "+args[3])
						if DOWNVOTE > math.floor(N_NODE/2):
							print ("Node "+str(ID)+" gagal menjadi leader")
							IS_LEADER = False
							IS_ELECTION = False
					pass

				elif n == "data":
					
					print("===================check local and reply=============================")
					if not IS_LEADER:
						if(int(args[3]) >= TERM):
							TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
							TERM = int(args[3])
							META_DATA[DATA_INDEX] = int(args[4].split('?')[0])
							META_DATA[DATA_TERM] = int(args[3])
							parsed = urlparse.urlparse(self.path)
							for i in range(N_WORKER):
								DATA_DICT[i][LOAD] = float(urlparse.parse_qs(parsed.query)[str(i)][0])
								DATA_DICT[i][STATUS] = int(urlparse.parse_qs(parsed.query)[str(i)][1])

							try:
								id_leader = int(args[2])
								print("NODE FOLLOWER : SENDING POSITIVE BACK TO THE LEADER")
								r = requests.get("{}:{}/positive/{}/{}/{}".format(NODE_DICT[id_leader][IP], str(NODE_DICT[id_leader][PORT]), args[3], args[4].split('?')[0], str(ID), timeout=0.01))
							except :
								pass
						else:
							print("NODE FOLLOWER : SENDING NEGATIVE BACK TO THE LEADER")
							# send negative
							id_leader = int(args[2])
							try :
								r = requests.get("{}:{}/negative/{}/{}/{}".format(NODE_DICT[id_leader][IP], str(NODE_DICT[id_leader][PORT]), str(TERM), str(getLastCommit()), str(ID), timeout=0.01))
							except :
								pass

								#print(e)
						
					pass

				elif n == "positive":
					COMMIT_COUNTER[int(args[3])] += 1
					if COMMIT_COUNTER[int(args[3])] > N_NODE/2:
						if (META_DATA[DATA_TERM] == int(args[2]) and META_DATA[DATA_INDEX] == int(args[3])):
							commitData()
					pass
				elif n == "negative":
					if TERM < int(args[2]) and getLastCommit() <= int(args[3]):
						TERM = int(args[2])
						IS_LEADER = False
					pass

				elif n == "commit":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					if not IS_LEADER:
						print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + args[4].split('?')[0])
						if(int(args[4].split('?')[0]) >= META_DATA[DATA_INDEX]):
							
							META_DATA[DATA_INDEX] = int(args[4].split('?')[0])
							META_DATA[DATA_TERM] = int(args[3])
							parsed = urlparse.urlparse(self.path)
							for i in range(N_WORKER):
								DATA_DICT[i][LOAD] = float(urlparse.parse_qs(parsed.query)[str(i)][0])
								DATA_DICT[i][STATUS] = int(urlparse.parse_qs(parsed.query)[str(i)][1])
							commitData()

							try:
								id_leader = int(args[2])
								print("NODE FOLLOWER : SENDING POSITIVE BACK TO THE LEADER")
								r = requests.get("{}:{}/positive/{}/{}/{}".format(NODE_DICT[id_leader][IP], str(NODE_DICT[id_leader][PORT]), args[3], args[4].split('?')[0], str(ID), timeout=0.01))
							except :
								pass
							
						else:
							print("NODE FOLLOWER : SENDING NEGATIVE COMMIT")
							id_leader = int(args[2])
							try :
								r = requests.get("{}:{}/negativecommit/{}/{}/{}".format(NODE_DICT[id_leader][IP], str(NODE_DICT[id_leader][PORT]), str(TERM), str(getLastCommit()), str(ID), timeout=0.01))
							except :
								pass
				elif n == "cpuload":
					print(IS_LEADER)
					# process the cpu load if the current node is a leader
					if IS_LEADER:
						# collect the data
						cpuload = args[2]
						workerid = args[3]

						print("NODE LEADER : DAPET DARI DAEMON ID KE %s" % workerid)

						if int(workerid) in WORKER_DICT:
							#print("worker with id %d is found" % (int(workerid)))
							#print("from host : " + fromHost + ":" + fromPort + " with the cpu load = " + cpuload)
							DAEMON_TIMEOUT[int(workerid)] = STD_DAEMON_TIMEOUT
							DATA_DICT[int(workerid)][LOAD] = cpuload
							DATA_DICT[int(workerid)][STATUS] = ON
							INDEX += 1
							META_DATA[DATA_INDEX] = INDEX
							META_DATA[DATA_TERM] = TERM
							COMMIT_COUNTER[INDEX] = 1
							DAEMON_TIMEOUT[int(workerid)] = STD_DAEMON_TIMEOUT
							for i in range(N_NODE):
								if i != ID:
									try:
										_thread.start_new_thread(sendCPULoad, (i,0))

									except Exception as e:
										print(e)
										pass
						else:
							print("Sorry the worker %s is not defined..." % (workerid))
					pass
			
		except Exception as ex:
			self.send_response(500)
			self.end_headers()
			print(ex)

def sendCPULoad(i, j):
    try:
        payload = {}
        for j in range(N_WORKER):
            payload[j] = DATA_DICT[j]

        r = requests.get(
            "{node_ip}:{node_port}/data/{node_id}/{term}/{index}".format(node_ip=NODE_DICT[i][IP],
                                                                         node_port=NODE_DICT[i][PORT], node_id=ID,
                                                                          term=META_DATA[DATA_TERM], index=META_DATA[DATA_INDEX]),
            params=payload, timeout=0.01)
    except Exception as e:
        pass


def sendCommit(i, j):
    try:
        f = open(DATA_FILE, 'r').readlines()
        META_DATA[DATA_TERM] = int(f[0].split('*')[0])
        META_DATA[DATA_INDEX] = int(f[0].split('*')[1])
        payload = {}
        dataLines = f[0].split('*')[2].split('&')
        for values in dataLines:
            payload[int(values.split('|')[0])] = [float(values.split('|')[1]), int(values.split('|')[2])]
        for j in range(N_WORKER):
            payload[j] = DATA_DICT[j]

        r = requests.get(
            "{node_ip}:{node_port}/commit/{node_id}/{term}/{index}".format(node_ip=NODE_DICT[i][IP], node_port=NODE_DICT[i][PORT],
                                                                           node_id=ID,
                                                                                   term=META_DATA[DATA_TERM], index=META_DATA[DATA_INDEX]),params=payload,
            timeout=0.01)
    except Exception as e:
        pass


def print_usage():
    print('Usage : {command} [node_id]'.format(command=sys.argv[0]))
    print('Please use ID (0 <= ID < number of node)')
    sys.exit(-1)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage()
    else:
        try:
            # initialize ID, TERM, COMMIT_DICTIONARY
            ID = int(sys.argv[1])
            init()

            # start timer here
            try:
                _thread.start_new_thread(nodeTimer, ())
            except:
                print("Error: unable to start thread")
            # start server here
            server = HTTPServer(("", int(NODE_DICT[ID][PORT])), ListenerHandler)
            server.serve_forever()
        except KeyboardInterrupt:
            print("# Load Balancer Shutdown")
            sys.exit(0)
