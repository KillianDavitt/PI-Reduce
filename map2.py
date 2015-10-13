import json
import select
from socket import *
import sys
import threading
import time

MASTER_PORT = 8060
WORKER_PORT = 3061

BROADCAST_TIMEOUT = 1
BROADCAST_INTERVAL = 0.1
BROADCAST_IP = "127.0.0.1"
BROADCAST_MSG = "Broadcast"
REGISTER_MSG = "Register"

WORKER_TIMEOUT = BROADCAST_TIMEOUT + 1

MAP_PREFIX = "MAP_PREFIX"
WORKERS_PREFIX = "WORKERS_PREFIX"

def Socket(host, port):
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
  sock.bind((host, port))
  return sock

class Master:

  def __init__(self, map_path):
    self.sock = Socket(gethostbyname(gethostname()), MASTER_PORT)
    print self.sock.getsockname()

    broadcast_thread = threading.Thread(target=self.Broadcast)
    broadcast_thread.start()
    self.workers = []
    self.RegisterWorkers()
    broadcast_thread.join()

    print str(len(self.workers)), "workers found."

    # Send map function.
    with open(map_path, "r") as f:
      map_f = f.read()
    for worker in self.workers:
      self.sock.sendto(MAP_PREFIX + map_f, worker)
      self.sock.sendto(WORKERS_PREFIX + json.dumps(self.workers), worker)

  def RegisterWorkers(self):
    read, _, _ = select.select([self.sock], [], [], BROADCAST_TIMEOUT)
    for sock in read:
      data, addr = sock.recvfrom(1024)
      if data == REGISTER_MSG:
        print "Found one worker at", addr
        self.workers.append(addr)

  def Broadcast(self):
    time_expired = 0
    while time_expired < BROADCAST_TIMEOUT:
      self.sock.sendto(BROADCAST_MSG, (BROADCAST_IP, WORKER_PORT))
      print "Broadcasting..."
      time.sleep(BROADCAST_INTERVAL)
      time_expired += BROADCAST_INTERVAL

class Worker:

  def __init__(self):
    self.sock = Socket(BROADCAST_IP, WORKER_PORT)
    print "Waiting for broadcast from Master"
    self.Register()
    print "Found Master at", self.master
    self.ReceiveData()
    print "Received data"
    print "map_f", self.map_f
    print "workers", self.workers

  def Register(self):
    while True:
      data, addr = self.sock.recvfrom(1024)
      if data == BROADCAST_MSG:
        self.master = addr
        break
    self.sock.sendto(REGISTER_MSG, self.master)

  def ReceiveData(self):
    start_time = time.time()
    while time.time() < start_time + WORKER_TIMEOUT:
      read, _, _ = select.select([self.sock], [], [], 0.1)
      for sock in read:
        data, addr = sock.recvfrom(65535)
        if data.startswith(MAP_PREFIX):
          self.map_f = data[len(MAP_PREFIX):]
        elif data.startswith(WORKERS_PREFIX):
          self.workers = json.loads(data[len(WORKERS_PREFIX):])

if __name__ == "__main__":
  if len(sys.argv) <= 1:
    print "Not enough arguments"
  elif sys.argv[1].lower() == "master":
    print "Running as Master"
    Master(sys.argv[2])
  elif sys.argv[1].lower() == "worker":
    print "Running as Worker"
    Worker()
  else:
    print "Ambiguous"