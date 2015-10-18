import json
import select
from socket import *
import sys
import threading
import time

MASTER_PORT = 8002
WORKER_PORT = 8001

BROADCAST_MSG = "Broadcast"
REGISTER_TIMEOUT = 1
REGISTER_MSG = "Register"
CONFIRMATION_MSG = "Confirmation"

DATA_TIMEOUT = REGISTER_TIMEOUT + 1

MAP_PREFIX = "MAP_PREFIX"
WORKERS_PREFIX = "WORKERS_PREFIX"
LINES_PREFIX = "LINES_PREFIX"

def Socket(host, port):
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
  sock.bind((host, port))
  return sock


class Master:

  def __init__(self, map_f, part_f, reduce_f, lines, broadcast_ip):
    self.sock = Socket(broadcast_ip, MASTER_PORT)
    print "At:", self.sock.getsockname()

    # Broadcast then register all Workers.
    self.sock.sendto(BROADCAST_MSG, (broadcast_ip, WORKER_PORT))
    self.workers = set() # Allow no duplicates.
    self.RegisterWorkers()
    print str(len(self.workers)), "workers found."
    if not len(self.workers):
      print "Exiting"
      return

    amount_lines = len(lines)
    print "amount_lines: " + str(amount_lines)
    amount_workers = len(self.workers)
    print "amount_workers: " + str(amount_workers)
    lines_per_worker = amount_lines / amount_workers
    print "lines_per_worker", lines_per_worker
    line_remainders = amount_lines % amount_workers

    # Send all data to Workers.
    lines_index = 0
    for worker in self.workers:
      self.sock.sendto(WORKERS_PREFIX + json.dumps(list(self.workers)), worker)
      self.sock.sendto(MAP_PREFIX + map_f, worker)
      #Instruct worker to open tcp port
      self.sock.sendto(LINES_PREFIX, worker)
      #Wait for worker to open port
      time.sleep(1)
      s = socket()
      #Connect to worker tcp port
      s.connect(worker)
      print "lines_index: " + str(lines_index)
      lines_end_index = lines_index + lines_per_worker - 1
      if line_remainders:
        lines_end_index += 1
        line_remainders -= 1
      print "lines_end_index: " + str(lines_end_index)
      for line in lines[lines_index:lines_end_index]:
        s.send(line)
      #Send socket shutdown signal to Worker
      s.shutdown(SHUT_WR)
      s.close()
      lines_index = lines_end_index + 1
    # send partition_f TODO
    # send reduce_f TODO

    # Wait for all workers to finish.

    # Signal all workers to start Reduce.

  def RegisterWorkers(self):
    """Register any Workers that reply to broadcast within REGISTER_TIMEOUT.
    """
	master_ip = "192.168.0.25"
    self.sock = Socket(master_ip, MASTER_PORT)
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

  def __init__(self, broadcast_ip):
    self.sock = Socket(broadcast_ip, WORKER_PORT)
    print "Waiting for broadcast from Master."

    # Contact Master and receive confirmation.
    self.master = None
    self.Register()
    # Receive all needed data from Master.
    self.map_f = None
    self.workers = None
    self.lines = []
    self.ReceiveData()

    # print "workers:", self.workers
    # print "len(map_f):", len(self.map_f)
    # print "len(lines):", len(self.lines)
    # print self.lines

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
    while time.time() < start_time + DATA_TIMEOUT:
      data, addr = self.sock.recvfrom(65535)

      if data.startswith(MAP_PREFIX):
        print "MAP"
        self.map_f = data[len(MAP_PREFIX):]
      elif data.startswith(WORKERS_PREFIX):
        print "WORKERS"
        self.workers = json.loads(data[len(WORKERS_PREFIX):])
      elif data.startswith(LINES_PREFIX):
        print "LINE"
        #Start tcp Server when 'LINE' prefix is recieved
        s = socket(AF_INET, SOCK_STREAM)
        #bind the port to the address of the Master but on WORKER_PORT
        s.bind((addr[0], WORKER_PORT))
        s.listen(4)
        while True:
          c, addr = s.accept()     # Establish connection with client.
          print 'Got connection from', addr
          print "Receiving..."
          l = c.recv(1024)
          while (l):
            print "Receiving..."
            self.lines.append(l)
            l = c.recv(1024)
          print "Done Receiving"
          #Close connection to client
          c.close()





