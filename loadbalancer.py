import sys, time, _thread, random, requests, math
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
SUBMITTER_COUNTER = 0

# const from file
N_NODE = 0
N_WORKER = 0
DATA_FILE = None
ID = None

# temp data for workload
TEMP_WORKER_DICT = {}

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


def init():
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
    while 1:
        past = time.clock()
        TIMEOUT = random.uniform(2.0, 4.0)
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
                        print("send vote request to node " + str(i) + " for term " + str(TERM))
                        r = requests.get(
                            NODE_DICT[i][IP] + ":" + str(NODE_DICT[i][PORT]) + "/voteRequest/" + str(ID) + "/" + str(
                                TERM) + "/" + str(COMMIT_DICT[ID]), timeout=0.01)
                    except:
                        pass
        while IS_LEADER:
            for i in range(N_NODE):
                if i != ID:
                    print("send data to node " + str(i))
            HEARTBEAT = STD_HEARTBEAT
            past = time.clock()
            while IS_LEADER and HEARTBEAT > 0:
                now = time.clock()
                HEARTBEAT -= now - past
                past = now


# class for http connection between node2node and node2worker
class ListenerHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global TERM
        global IS_LEADER
        global IS_ELECTION
        global VOTE_TERM
        global UPVOTE
        global DOWNVOTE

        try:
            self.send_response(200)
            self.end_headers()
            args = self.path.split('/')
            if len(args) < 2:
                raise Exception()
            # handle each request based on its type
            if len(args) == 2:
                n = int(args[1])
                print("request to worker")
            else:
                n = args[1]
                if n == "voteRequest":
                    TIMEOUT = random.uniform(2.0, 4.0)
                    if (TERM < int(args[3]) and COMMIT_DICT[ID] <= int(args[4]) and VOTE_TERM < int(args[3])):
                        try:
                            print("send upvote to node" + args[2] + " for term " + args[3])
                            r = requests.get(NODE_DICT[int(args[2])][IP] + ":" + str(
                                NODE_DICT[int(args[2])][PORT]) + "/upvote/" + str(ID) + "/" + args[3], timeout=0.01)
                        except:
                            pass

                        IS_LEADER = False
                        IS_ELECTION = False
                        TERM = int(args[3])
                        VOTE_TERM = TERM
                    else:
                        try:
                            print("send downvote to node" + args[2] + " for term " + args[3])
                            r = requests.get(NODE_DICT[int(args[2])][IP] + ":" + str(
                                NODE_DICT[int(args[2])][PORT]) + "/downvote/" + str(ID) + "/" + args[3], timeout=0.01)
                        except:
                            pass

                elif n == "upvote":
                    TIMEOUT = random.uniform(2.0, 4.0)
                    if int(args[3]) == TERM and IS_ELECTION:
                        UPVOTE += 1
                        print("Receive upvote from " + args[2] + " for term " + args[3])
                        if UPVOTE > math.floor(
                                        N_NODE / 2):  # harusnya ad validasi siapa yg udah vote, tp aku males, ntar aj
                            print("Node " + str(ID) + " menjadi leader")
                            IS_LEADER = True
                            IS_ELECTION = False
                            TERM = int(args[3])
                            VOTE_TERM = TERM
                            # set semua top jadi sama dengan master
                            # set semua commit jadi 0
                            # isi temporary data dengan last commit
                            # isi temporary status dengan last commit
                            # set semua daemon_timer ke std_daemon_timeout
                            # jalankan timer



                elif n == "downvote":
                    TIMEOUT = random.uniform(2.0, 4.0)
                    if int(args[3]) == TERM and IS_ELECTION:
                        DOWNVOTE += 1
                        print("Receive downvote from " + args[2] + " for term " + args[3])
                        if DOWNVOTE > math.floor(N_NODE / 2):
                            print("Node " + str(ID) + " gagal menjadi leader")
                            IS_LEADER = False
                            IS_ELECTION = False
                elif n == "data":
                    print("check local and reply")
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
                            TEMPWORKER_DICT[workerid] = [fromHost, fromPort, cpuload, workerid]
                            if SUBMITTER_COUNTER >= N_WORKER:
                                SUBMITTER_COUNTER = 0
                                # copy data from temp dict into main dict

                        else:
                            print("Sorry the worker %d is not defined..." % (workerid))

        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)


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
