import json
import select
from socket import *
import sys
import threading
import time

MASTER_PORT = 8060
WORKER_PORT = 3061

BROADCAST_IP = "127.0.0.1"
BROADCAST_MSG = "Broadcast"
REGISTER_TIMEOUT = 1
REGISTER_MSG = "Register"
CONFIRMATION_MSG = "Confirmation"

DATA_TIMEOUT = REGISTER_TIMEOUT + 1

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
    self.sock = Socket(BROADCAST_IP, MASTER_PORT)
    print "At:", self.sock.getsockname()

    # Discover all Workers.
    self.sock.sendto(BROADCAST_MSG, (BROADCAST_IP, WORKER_PORT))
    self.workers = set() # Allow no duplicates.
    self.RegisterWorkers()
    print str(len(self.workers)), "workers found."

    # Read in data to send to Workers.
    with open(map_path, "r") as f:
      map_f = f.read()

    # Send all data to Workers.
    for worker in self.workers:
      self.sock.sendto(MAP_PREFIX + map_f, worker)
      self.sock.sendto(WORKERS_PREFIX + json.dumps(list(self.workers)), worker)

    # Wait for all workers to finish.

    # Signal all workers to start Reduce.

  def RegisterWorkers(self):
    start_time = time.time()
    while time.time() < start_time + REGISTER_TIMEOUT:
      read, _, _ = select.select([self.sock], [], [], 0.1)
      for sock in read:
        data, addr = sock.recvfrom(1024)
        if data == REGISTER_MSG:
          print "Found worker at", addr
          self.workers.add(addr)
          self.sock.sendto(CONFIRMATION_MSG, addr)

class Worker:

  def __init__(self):
    self.sock = Socket(BROADCAST_IP, WORKER_PORT)
    print "Waiting for broadcast from Master."

    # Contact Master and receive confirmation.
    self.Register()

    # Receive all needed data from Master.
    self.map_f = None
    self.workers = None
    self.ReceiveData()
    print "workers:", self.workers
    print "len(map_f):", len(self.map_f)

  def Register(self):
    registered = False
    while not registered:
      data, addr = self.sock.recvfrom(1024)
      if data == BROADCAST_MSG:
        print "Registering with master at", addr
        self.master = addr
        self.sock.sendto(REGISTER_MSG, self.master)
        sent = True
      elif data == CONFIRMATION_MSG:
        print "Registration complete"
        registered = True

  def ReceiveData(self):
    start_time = time.time()
    # Until all data received or timed-out.
    while time.time() < start_time + DATA_TIMEOUT and not all([self.workers, self.map_f]):
      read, _, _ = select.select([self.sock], [], [], 0.1)
      for sock in read:
        data, addr = sock.recvfrom(65535)
        if data.startswith(MAP_PREFIX):
          self.map_f = data[len(MAP_PREFIX):]
        elif data.startswith(WORKERS_PREFIX):
          self.workers = json.loads(data[len(WORKERS_PREFIX):])

if __name__ == "__main__":
  if len(sys.argv) <= 1:
    print "Not enough arguments."
  elif sys.argv[1].lower() == "master":
    print "Running as Master."
    Master(sys.argv[2])
  elif sys.argv[1].lower() == "worker":
    print "Running as Worker."
    Worker()
  else:
    print "Invalid options."