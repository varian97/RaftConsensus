import sys, time, _thread, random, requests, math, json
import urllib.parse as urlparse
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler

# const static
IP = 0
PORT = 1
ON = 1
OFF = 0
SERVER_FILE = "ServerList.txt"
NEW_LINE = '\n'
STD_HEARTBEAT = 2
STD_DAEMON_TIMEOUT = None
TIMEOUT_RANGE = 2

# const from file
N_NODE = 0
N_WORKER = 0
DATA_FILE = None
ID = None

# address dict (ID->[IP,PORT])
WORKER_DICT = {}
NODE_DICT = {}

# RAFT stored variables
LOAD_DICT = {}
STATUS_DICT = {}
TERM = None

# RAFT runtime variables
IS_ELECTION = False
IS_LEADER = False
TIMEOUT = 0

# Election variables
UP_VOTE = 0
DOWN_VOTE = 0
VOTE_TERM = 0

# Leader variables
TOP_DICT = {}
COMMIT_DICT = {}
DAEMON_TIMEOUT = {}
HEARTBEAT = None
COMMIT_COUNTER = {}

def init():
    global N_NODE
    global N_WORKER
    global DATA_FILE
    global WORKER_DICT
    global NODE_DICT
    global LOAD_DICT
    global STATUS_DICT
    global COMMIT_DICT
    global ID
    global TERM

    DATA_FILE = "Data_" + str(ID) + ".txt"
    context = None
    file = open(SERVER_FILE, "r")
    datafile = open(DATA_FILE, "a+")
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
                    LOAD_DICT[count] = 0
                    STATUS_DICT[count] = OFF
                    count += 1
    
    #Mungkin dipindah ke listener url



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
	global HEARTBEAT
	global IS_LEADER
	global TOP_DICT

	#if ID == 0:
	#		IS_LEADER = True

	while 1:
		past = time.clock()
		TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
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
			for i in range (N_NODE):
				if i != ID:
					try:
						#print ("send vote request to node " + str(i) + " for term " + str(TERM))
						r = requests.get(NODE_DICT[i][IP] + ":" + str(NODE_DICT[i][PORT])+"/voteRequest/"+str(ID)+"/"+str(TERM)+"/"+str(COMMIT_DICT[ID]), timeout = 0.01)
					except:
						pass
		while IS_LEADER:
			print ("LEADER")
			for i in range (N_NODE):		
				if i != ID:
					sendCommit(NODE_DICT[i][IP], NODE_DICT[i][PORT], TOP_DICT[i])
					print ("send commit to node " + str(i))
			HEARTBEAT = STD_HEARTBEAT
			past = time.clock()
			while IS_LEADER  and HEARTBEAT > 0:
				now = time.clock()
				HEARTBEAT -= now - past
				past = now
		

def replace_line(file_name, line_num, text):
    lines = open(file_name, 'r').readlines()
    lines[line_num] = text
    out = open(file_name, 'w')
    out.writelines(lines)
    out.close()

#class for http connection between node2node and node2worker
class ListenerHandler(BaseHTTPRequestHandler):			
	
	def do_GET(self):
		global TERM
		global IS_LEADER
		global IS_ELECTION
		global VOTE_TERM
		global UPVOTE
		global DOWNVOTE
		global LOAD_DICT
		global STATUS_DICT
		global COMMIT_DICT

		
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
				minLoad = 0
				for i in range(N_WORKER):
					if LOAD_DICT[i] < LOAD_DICT[minLoad]:
						minLoad = i
				r = requests.get(WORKER_DICT[minLoad][IP] + ":" + str(WORKER_DICT[minLoad][PORT])+"/"+args[2], timeout = 0.01)

				#print the result
				self.wfile.write(str(int(r.text)).encode('utf-8'))
			else:
				n = args[1]
				if n == "voteRequest":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					if (TERM < int(args[3]) and COMMIT_DICT[ID] <= int(args[4]) and VOTE_TERM < int(args[3])):
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
							#set semua top jadi sama dengan master
							#set semua commit jadi 0


							#isi temporary status dengan last commit
							lastcommit = ""
							count = 0
							f = open(DATA_FILE, "r")
							for line in f:
								if (line[0] != '!') and len(line) > 0:
									lastcommit = line
									count += 1

							for i in range(N_NODE):
								if i != ID:
									COMMIT_DICT[i] = 0								
								else:
									COMMIT_DICT[i] = count
								TOP_DICT[i] = count+1

							if len(lastcommit) > 0:
								arr0 = lastcommit.split('*')
								TERM = int(arr0[0])
								arr1 = arr0[1].split('&')

								for item in arr1:
									arr2 = item.split('|')
									LOAD_DICT[int(arr2[0])] = float(arr2[1])
									STATUS_DICT[int(arr2[0])] = int(arr2[2])
							else:
								for i in range(N_NODE):
									LOAD_DICT[i] = 0
									STATUS_DICT[i] = OFF
							IS_LEADER = True
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

				elif n == "data":
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					print("===================check local and reply=============================")
					if not IS_LEADER:
						# check if commit equals or not
						totalLine = 0
						countCommit = 0
						leaderCommit = int(args[4].split('?')[0])
						f = open(DATA_FILE, 'r')
						for line in f:
							if (line[0] != '#') and len(line) > 0:
								countCommit = countCommit + 1
							totalLine = totalLine + 1

						#leaderCommit = countCommit + 1

						print("count commit = %s, leader commit = %s" % (str(countCommit), str(leaderCommit)))

						while countCommit > leaderCommit-1:
							i = 0
							payload = ""
							for line in f:
								if i != countCommit-1:
									i+=1
								else:
									payload = line
							replace_line(DATA_FILE, countCommit-1, '#'+ payload)
							countCommit-=1

						if countCommit == leaderCommit - 1:
							# parsing the data from url into local load dict
							parsed = urlparse.urlparse(self.path)
							#print(int(urlparse.parse_qs(parsed.query)['1'][0]))
							for i in range(N_WORKER):
								LOAD_DICT[i] = float(urlparse.parse_qs(parsed.query)[str(i)][0])
								STATUS_DICT[i] = int(urlparse.parse_qs(parsed.query)[str(i)][1])
								pass



							# copy data from temp dict into storage
							payload = "!" + str(TERM) + '*'

							for key, value in LOAD_DICT.items():
								#format file commit : worker_id | cpuload | status
								if key != 0:
									payload += '&'
								payload += '{}|{}|{}'.format(str(key), str(value), str(STATUS_DICT[key]))

							payload += '\n'

							if(leaderCommit-1 < totalLine):
								replace_line(DATA_FILE, leaderCommit-1, payload)
							else:
								commit_data = open(DATA_FILE, 'w')
								commit_data.write(payload)
								commit_data.close()

							# send commit to leader
							try:
								id_leader = int(args[2])
								print("NODE FOLLOWER : SENDING POSITIVE BACK TO THE LEADER")
								r = requests.get("{}:{}/positive/{}/{}".format(NODE_DICT[id_leader][IP], str(NODE_DICT[id_leader][PORT]), countCommit+1 , str(ID), timeout=0.01))
							except :
								pass
								#print(e)

						elif countCommit < leaderCommit - 1:
							print("NODE FOLLOWER : SENDING NEGATIVE BACK TO THE LEADER")
							# send negative
							id_leader = int(args[2])
							try :
								r = requests.get(NODE_DICT[id_leader][IP] + ":" + str(NODE_DICT[id_leader][PORT]) + "/negative/" + str(countCommit+1) + "/" + str(ID), timeout=0.01)
							except :
								pass
								#print(e)

				elif n == "positive":
					if IS_LEADER:
						print("GET POSITIVE")
						COMMIT_COUNTER[int(args[2])] += 1
						COMMIT_DICT[int(args[3])] = int(args[2])
						TOP_DICT[int(args[3])] = int(args[2]) + 1
						if(COMMIT_COUNTER[int(args[2])] > (N_NODE / 2)):
							f = open(DATA_FILE, 'r')
							i = 0
							payload = ""
							for line in f:
								if i != int(args[3]):
									i+=1
								else:
									payload = line
							if payload[0] == '!':
								replace_line(DATA_FILE, int(args[3]), payload.split('!')[1])

							COMMIT_DICT[ID] = int(args[2])
							TOP_DICT[ID] = int(args[2]) + 1

				elif n == "negative":
					if IS_LEADER:
						print("GET NEGATIVE")
						TOP_DICT[int(args[3])] -= 1

				elif n == "commit":
					print("COMMIT")
					TIMEOUT = random.uniform(TIMEOUT_RANGE, 2*TIMEOUT_RANGE)
					countCommit = 0
					f = open(DATA_FILE, "r")
					for line in f:
						if (line[0] != '!') and len(line) > 0:
							countCommit = countCommit + 1

					commitLine = int(args[3])
					id_leader = int(args[4])

					print(str(countCommit)+ "========="+ str(commitLine-1))
					if countCommit == commitLine - 1:
						i = 0
						payload = ""
						for line in f:
							if i != countCommit:
								i+=1
							else:
								payload = line
						if len(payload) == 0:
							replace_line(DATA_FILE, countCommit, args[2].replace('%7C', '|') + '\n')
						elif payload[0] == '!':
							replace_line(DATA_FILE, countCommit, payload.split('!')[1])
						print("POSITIVE COMMIT")
						r = requests.get(NODE_DICT[id_leader][IP] + ":" + str(NODE_DICT[id_leader][PORT]) + "/positivecommit/" + args[3] + "/" + str(ID), timeout=0.01)

					elif countCommit < commitLine - 1:
						try :
							r = requests.get(NODE_DICT[id_leader][IP] + ":" + str(NODE_DICT[id_leader][PORT]) + "/negativecommit/" + args[3] + "/" + str(ID), timeout=0.01)
						except TypeError as e:
							pass

				elif n == "positivecommit":
					print ("Leader get positive commit from id = %s" % args[3])
					if COMMIT_DICT[int(args[3])] == int(args[2]) - 1:
						COMMIT_DICT[int(args[3])] = int(args[2])
						TOP_DICT[int(args[3])] = int(args[2]) + 1
						COMMIT_COUNTER[int(args[3])] += 1
						if COMMIT_COUNTER[int(args[3])] > N_NODE/2 and COMMIT_DICT[ID] == int(args[2]) - 1:
							COMMIT_DICT[ID] += 1

				elif n == "negativecommit":
					print ("Leader get negative commit")
					TOP_DICT[int(args[3])] = int(args[2])					

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
							LOAD_DICT[int(workerid)] = cpuload
							STATUS_DICT[int(workerid)] = ON

							"""payload = "!" + str(TERM) + '*'

							for key, value in LOAD_DICT.items():
								#format file commit : worker_id | cpuload | status
								if key != 0:
									payload += '&'
								payload += '{}|{}|{}'.format(str(key), str(value), str(STATUS_DICT[key]))

							payload += '\n'

							replace_line(DATA_FILE, COMMIT_DICT[ID], payload)"""

							COMMIT_COUNTER[TOP_DICT[ID]] = 1
							TOP_DICT[ID] += 1
							for i in range(N_NODE):
								if i != ID:
									try:
										_thread.start_new_thread(sendCPULoad, (NODE_DICT[i][IP], NODE_DICT[i][PORT]))
									except:
										pass
						else:
							print("Sorry the worker %s is not defined..." % (workerid))
			
		except Exception as ex:
			self.send_response(500)
			self.end_headers()
			print(ex)

def sendCPULoad(ip, port):
	try:
		payload = {}
		for i in range(N_WORKER):
			payload[i] = [LOAD_DICT[i], STATUS_DICT[i]]
		r = requests.get(ip + ":" + port +"/data/"+str(ID)+"/"+str(TERM) + "/" + str(COMMIT_DICT[ID] + 1), params=payload, timeout=0.01)
	except:
		pass

def sendCommit(ip, port, commitLine):
	f = open(DATA_FILE, 'r')
	i = 0
	payload = ""
	for line in f:
		if i != commitLine-1:
			i+=1
		else:
			payload = line

	try:
		r = requests.get(ip + ":" + port + "/commit/" +  payload + "/" + str(commitLine) + "/" + str(ID), timeout=0.01)
	except :
		#print(e)
		pass

def print_usage():
    print('Usage : %s [node_id]' % sys.argv[0])
    print('Please use ID (0 <= ID < number of node)')
    sys.exit(-1)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage()
    else:
        # initialize ID, TERM, COMMIT_DICTIONARY
        ID = int(sys.argv[1])
        TERM = 0
        COMMIT_DICT[ID] = 0
        init()



        # start timer here
        try:
            _thread.start_new_thread(nodeTimer, ())
        except:
            print("Error: unable to start thread")

        # start server here
        server = HTTPServer(("", int(NODE_DICT[ID][PORT])), ListenerHandler)
        server.serve_forever()
