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
DATA_FILE = ""
ID = 0

# address dict (ID->[IP,PORT])
WORKER_DICT = {}
NODE_DICT = {}

# RAFT stored variables
LOAD_DICT = {}
STATUS_DICT = {}
TERM = 0

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

    DATA_FILE = "Data_{node_id:d}.txt".format(node_id=ID)
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

                    # Mungkin dipindah ke listener url


def daemon_timer():
    past = time.clock()
    while IS_LEADER:
        now = time.clock()
        for i in range(N_WORKER):
            DAEMON_TIMEOUT[i] -= now - past
            if DAEMON_TIMEOUT[i] < 0:
                STATUS_DICT[i] = OFF
        past = now


def node_timer():
    global TERM
    global TIMEOUT
    global IS_ELECTION
    global UPVOTE
    global DOWNVOTE
    global VOTE_TERM
    global HEARTBEAT
    global IS_LEADER
    global TOP_DICT

    # if ID == 0:
    #		IS_LEADER = True

    while 1:
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
                            commit=COMMIT_DICT[ID]), timeout=0.01)
                    except:
                        pass
        while IS_LEADER:
            print("LEADER")
            for i in range(N_NODE):
                if i != ID:
                    send_commit(NODE_DICT[i][IP], NODE_DICT[i][PORT], TOP_DICT[i] - 1)
                    print("send commit to node {node_id}".format(node_id=i))
            HEARTBEAT = STD_HEARTBEAT
            past = time.clock()
            while IS_LEADER and HEARTBEAT > 0:
                now = time.clock()
                HEARTBEAT -= now - past
                past = now


def replace_line(file_name, line_num, text):
    lines = open(file_name, 'r').readlines()
    lines[line_num] = text
    out = open(file_name, 'w')
    out.writelines(lines)
    out.close()


# class for http connection between node2node and node2worker
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
        global TIMEOUT

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
                        try:
                            r = requests.get(
                                "{worker_ip}:{worker_port}/{number}".format(worker_ip=WORKER_DICT[minLoad][IP],
                                                                            worker_port=WORKER_DICT[minLoad][
                                                                                PORT], number=args[2]), timeout=0.01)
                            # print the result
                            self.wfile.write(str(int(r.text)).encode('utf-8'))
                        except:
                            pass
                    else:
                        n = args[1]

            if n == "voteRequest":
                node_dest = int(args[2])
                term_request = int(args[3])
                commit_current = int(args[4])
                TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)

                if TERM < term_request and COMMIT_DICT[ID] <= commit_current and VOTE_TERM < term_request:
                    try:
                        # print("send upvote to node" +args[2]+" for term "+args[3])
                        r = requests.get("{node_ip}:{node_port}/upvote/{node_id}/{term}".format(
                            node_ip=NODE_DICT[node_dest][IP], node_port=NODE_DICT[node_dest][PORT],
                            node_id=ID, term=term_request), timeout=0.01)
                    except:
                        pass

                if IS_LEADER:
                    print("Node {node_id} berhenti menjadi leader".format(node_id=ID))
                    IS_LEADER = False
                    IS_ELECTION = False
                    TERM = term_request
                    VOTE_TERM = TERM
                else:
                    try:
                        # print("send downvote to node" +args[2]+" for term "+args[3])
                        r = requests.get("{node_ip}:{node_port}/downvote/{node_id}/{term}".format(
                            node_ip=NODE_DICT[node_dest][IP], node_port=NODE_DICT[node_dest][PORT],
                            node_id=ID, term=term_request)
                            , timeout=0.01)
                    except:
                        pass
            elif n == "upvote":
                term_request = int(args[3])
                TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)
                if term_request == TERM and IS_ELECTION:
                    UPVOTE += 1
                    # print ("Receive upvote from "+args[2]+" for term "+args[3])
                    if UPVOTE > math.floor(N_NODE / 2):
                        # harusnya ad validasi siapa yg udah vote, tp aku males, ntar aj
                        print("Node {node_id} menjadi leader".format(node_id=ID))

                        IS_ELECTION = False
                        TERM = term_request
                        VOTE_TERM = TERM
                        # set semua top jadi sama dengan master
                        # set semua commit jadi 0
                        # isi temporary status dengan last commit
                        lastcommit = ""
                        count = 0
                        f = open(DATA_FILE, "r")
                        for line in f:
                            if (line[0] != '#') and len(line) > 0:
                                lastcommit = line
                                count += 1

                        for i in range(N_NODE):
                            if i != ID:
                                COMMIT_DICT[i] = 0
                            else:
                                COMMIT_DICT[i] = count
                            TOP_DICT[i] = count + 1

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
                        # set semua daemon_timer ke std_daemon_timeout
                        # jalankan timer

            elif n == "downvote":
                term_now = int(args[3])
                TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)
                if term_now == TERM and IS_ELECTION:
                    DOWNVOTE += 1
                    # print ("Receive downvote from "+args[2]+" for term "+args[3])
                    if DOWNVOTE > math.floor(N_NODE / 2):
                        print("Node {node_id} gagal menjadi leader".format(node_id=ID))
                        IS_LEADER = False
                        IS_ELECTION = False

            elif n == "data":
                TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)
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

                        # leaderCommit = countCommit + 1

                    print("# count commit = {commit_count}, leader commit = {leader_commit}".format(
                        commit_count=countCommit, leader_commit=leaderCommit))

                    while countCommit > leaderCommit - 1:
                        i = 0
                        payload = ""
                        for line in f:
                            if i != countCommit - 1:
                                i += 1
                            else:
                                payload = line
                        replace_line(DATA_FILE, countCommit - 1, '#' + payload)
                        countCommit -= 1

                    if countCommit == leaderCommit - 1:
                        # parsing the data from url into local load dict
                        parsed = urlparse.urlparse(self.path)
                        # print(int(urlparse.parse_qs(parsed.query)['1'][0]))
                        for i in range(N_WORKER):
                            LOAD_DICT[i] = float(urlparse.parse_qs(parsed.query)[str(i)][0])
                            STATUS_DICT[i] = int(urlparse.parse_qs(parsed.query)[str(i)][1])
                            pass

                        # copy data from temp dict into storage
                        payload = "#{term}*".format(term=TERM)

                        for key, value in LOAD_DICT.items():
                            # format file commit : worker_id | cpuload | status
                            if key != 0:
                                payload += '&'
                            payload += '{key}|{value}|{status}'.format(key=key, value=value,
                                                                       status=STATUS_DICT[key])

                        payload += '\n'

                        if (leaderCommit - 1 < totalLine):
                            replace_line(DATA_FILE, leaderCommit - 1, payload)
                        else:
                            commit_data = open(DATA_FILE, 'w')
                            commit_data.write(payload)
                            commit_data.close()

                        # send commit to leader
                        try:
                            id_leader = int(args[2])
                            print("# NODE FOLLOWER : SENDING POSITIVE BACK TO THE LEADER")
                            r = requests.get("{node_ip}:{node_port}/positive/{commit_count}/{node_id}".format(
                                node_ip=NODE_DICT[id_leader][IP],
                                node_port=NODE_DICT[id_leader][PORT],
                                commit_count=countCommit + 1, node_id=ID), timeout=0.01)
                        except:
                            pass
                            # print(e)

                    elif countCommit < leaderCommit - 1:
                        print("# NODE FOLLOWER : SENDING NEGATIVE BACK TO THE LEADER")
                        # send negative
                        id_leader = int(args[2])
                        try:
                            r = requests.get("{node_ip}:{node_port}/negative/{commit_count}/{node_id}".format(
                                node_ip=NODE_DICT[id_leader][IP], node_port=NODE_DICT[id_leader][PORT],
                                commit_count=countCommit + 1, node_id=ID),
                                timeout=0.01)
                        except:
                            pass
                            # print(e)

            elif n == "positive":
                if IS_LEADER:
                    commit_id = int(args[2])
                    node_id = int(args[3])
                    print("# GET POSITIVE")
                    COMMIT_COUNTER[commit_id] += 1
                    COMMIT_DICT[node_id] = commit_id
                    TOP_DICT[node_id] = commit_id + 1
                    if COMMIT_COUNTER[commit_id] > (N_NODE / 2):
                        f = open(DATA_FILE, 'r')
                        i = 0
                        payload = ""
                        for line in f:
                            if i != int(args[3]):
                                i += 1
                            else:
                                payload = line
                        if payload[0] == '#':
                            replace_line(DATA_FILE, int(args[3]), payload.split('#')[1])

                        COMMIT_DICT[ID] = commit_id
                        TOP_DICT[ID] = commit_id + 1

            elif n == "negative":
                if IS_LEADER:
                    print("# GET NEGATIVE")
                    TOP_DICT[int(args[3])] -= 1

            elif n == "commit":
                print("# COMMIT")
                TIMEOUT = random.uniform(TIMEOUT_RANGE, 2 * TIMEOUT_RANGE)
                countCommit = 0
                f = open(DATA_FILE, "r")
                for line in f:
                    if (line[0] != '#') and len(line) > 0:
                        countCommit = countCommit + 1
                commitLine = int(args[3])
                print(str(countCommit) + "=========" + str(commitLine - 1))
                id_leader = int(args[4])
                if countCommit == commitLine - 1:
                    i = 0
                    payload = ""
                    for line in f:
                        if i != countCommit:
                            i += 1
                        else:
                            payload = line
                    if len(payload) == 0:
                        replace_line(DATA_FILE, countCommit, args[2].replace('%27', '|') + '\n')
                    elif payload[0] == '#':
                        replace_line(DATA_FILE, countCommit, payload.split('#')[1])
                    print("## POSITIVE COMMIT")
                    try:
                        r = requests.get("{node_ip}:{node_port}/positivecommit/{commit}/{node_id}".format(
                            node_ip=NODE_DICT[id_leader][IP], node_port=NODE_DICT[id_leader][PORT], commit=args[3],
                            node_id=ID), timeout=0.01)
                    except:
                        pass
                elif countCommit < commitLine - 1:
                    try:
                        r = requests.get("{node_ip}:{node_port}/negativecommit/{commit}/{node_id}".format(
                            node_ip=NODE_DICT[id_leader][IP], node_port=NODE_DICT[id_leader][PORT], commit=args[3],
                            node_id=ID), timeout=0.01)
                    except:
                        pass

            elif n == "positivecommit":
                print("# Leader get positive commit")
                node_id = int(args[3])
                commit_line = int(args[2])
                if COMMIT_DICT[node_id] == commit_line - 1:
                    COMMIT_DICT[node_id] = commit_line
                    TOP_DICT[node_id] = commit_line + 1
                    COMMIT_COUNTER[node_id] += 1
                    if COMMIT_COUNTER[node_id] > N_NODE / 2 and COMMIT_DICT[ID] == commit_line - 1:
                        COMMIT_DICT[ID] += 1

            elif n == "negativecommit":
                print("# Leader get negative commit")
                TOP_DICT[int(args[3])] = int(args[2])

            elif n == "cpuload":
                print(IS_LEADER)
                # process the cpu load if the current node is a leader
                if IS_LEADER:
                    # collect the data
                    cpu_load = float(args[2])
                    worker_id = int(args[3])

                    print("# NODE LEADER : DAPET DARI DAEMON ID KE {id}".format(id=worker_id))

                    if worker_id in WORKER_DICT:
                        # print("worker with id %d is found" % (int(workerid)))
                        # print("from host : " + fromHost + ":" + fromPort + " with the cpu load = " + cpuload)
                        LOAD_DICT[worker_id] = cpu_load
                        STATUS_DICT[worker_id] = ON

                        payload = "#{term}*".format(term=TERM)

                        for key, value in LOAD_DICT.items():
                            # format file commit : worker_id | cpuload | status
                            if key != 0:
                                payload += '&'
                            payload += '{key} | {value} | {status}'.format(key=key, value=value,
                                                                           status=STATUS_DICT[key])

                        payload += '\n'

                        replace_line(DATA_FILE, COMMIT_DICT[ID], payload)

                        # REPLICATES DATA TO OTHER NODE
                        """for i in range(N_NODE):
                            if i != ID:
                                try:
                                    cpuloadJSON = json.dumps(LOAD_DICT)
                                    print("==== cpu load has been sent to another node %s ====" % str(i))
                                    r = requests.get(NODE_DICT[i][IP] + ":" + str(NODE_DICT[i][PORT]) +"/data/"+str(ID)+"/"+str(TERM) + "/" + str(COMMIT_DICT[ID] + 1), params=LOAD_DICT, timeout=0.01)
                                except NameError as e:
                                    #print("replication fail for node "+str(i))
                                    #print ("Unexpected error:", sys.exc_info()[0])
                                    print(e)"""
                        COMMIT_COUNTER[COMMIT_DICT[ID] + 1] = 1
                        TOP_DICT[ID] += 1
                        for i in range(N_NODE):
                            if i != ID:
                                try:
                                    _thread.start_new_thread(send_cpu_load, (NODE_DICT[i][IP], NODE_DICT[i][PORT]))
                                    # print("SEND SUCCEED TO NODE %s" % str(i))
                                except:
                                    pass
                                    # print("Error: unable to start thread")
                    else:
                        print("Sorry the worker {id} is not defined.".format(id=worker_id))

        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)


def send_cpu_load(ip, port):
    try:
        payload = {}
        for i in range(N_WORKER):
            payload[i] = [LOAD_DICT[i], STATUS_DICT[i]]
        r = requests.get(
            "{node_ip}:{node_port}/data/{node_id}/{term}/{commit}".format(node_ip=ip, node_port=port, node_id=ID,
                                                                          term=TERM, commit=COMMIT_DICT[ID] + 1),
            params=payload, timeout=0.01)
    except:
        pass


def send_commit(ip, port, commit_line):
    f = open(DATA_FILE, 'r')
    i = 0
    payload = ""
    for line in f:
        if i != commit_line - 1:
            i += 1
        else:
            payload = line

    try:
        r = requests.get(
            "{node_ip}:{node_port}/commit/{payload_msg}/{commit}/{node_id}".format(node_ip=ip, node_port=port,
                                                                                   payload_msg=payload,
                                                                                   commit=COMMIT_DICT[ID], node_id=ID),
            timeout=0.01)
    except Exception:
        # print(e)
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
            TERM = 0
            COMMIT_DICT[ID] = 0
            init()

            # start timer here
            try:
                _thread.start_new_thread(node_timer, ())
            except:
                print("# Error: unable to start thread")

            # start server here
            server = HTTPServer(("", int(NODE_DICT[ID][PORT])), ListenerHandler)
            server.serve_forever()
        except KeyboardInterrupt:
            print("# Load Balancer Shutdown")
            sys.exit(0)
