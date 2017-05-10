#!/usr/bin/env python
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import sys

#PORT = 13337


class WorkerHandler(BaseHTTPRequestHandler):
    def prime(self, n):
        i = 2
        while i * i <= n:
            if n % i == 0:
                return False
            i += 1
        return True

    def calc(self, n):
        p = 1
        while n > 0:
            p += 1
            if self.prime(p):
                n -= 1
        return p

    def do_GET(self):
        try:
            print(self.path)
            args = self.path.split('/')
            if len(args) != 2:
                raise Exception()
            n = int(args[1])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(str(self.calc(n)).encode('utf-8'))
        except Exception as ex:
            self.send_response(500)
            self.end_headers()
            print(ex)

ID = int(sys.argv[1])
i = 0
PORT = None

f = open("WorkerList.txt", "r")
for line in f:
    if i == ID:
        PORT = int(line.split(":")[2])
    i+=1

server = HTTPServer(("", PORT), WorkerHandler)
server.serve_forever()
