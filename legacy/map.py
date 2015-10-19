from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, SO_BROADCAST, gethostbyname, gethostname
from sys import argv
from threading import Thread
from time import sleep

BROADCAST_TIMEOUT = 5
BROADCAST_INTERVAL = 0.1
BROADCAST_IP = "127.0.0.1"
PORT = 8002
BROADCAST_MSG = "MasterBroadcast"

REGISTER_MSG = "Registered"

WORKER_PORT = 3001

def Socket():
  sock = socket(AF_INET, SOCK_DGRAM)
  sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
  sock.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
  sock.bind((gethostbyname(gethostname()), P))
  return sock

class Master:

  def Run(self):
    self.sock = Socket()
    print self.sock.getsockname()

    self.workers = []
    Thread(target=self.RegisterWorkers()).start()

    self._Broadcast()

  def RegisterWorkers(self):
    data, addr = self.sock.recvfrom(1024)
    if data == REGISTER_MSG:
      self.workers += addr
      print addr
    else:
      print "no data"

  def _Broadcast(self):
    """Sends UDP broadcast packets to discover any Workers."""
    time_expired = 0
    while time_expired < BROADCAST_TIMEOUT:
      print "Broadcasting..."
      self.sock.sendto(BROADCAST_MSG, (BROADCAST_IP, BROADCAST_PORT))
      sleep(BROADCAST_INTERVAL)
      time_expired += BROADCAST_INTERVAL

class Worker:

  def Run(self):
    print "Waiting for broadcast from Master"
    self.DiscoverMaster()
    print "Discovered master at", self.master_addr
    # TODO(Maki) Send packet to Master

  def DiscoverMaster(self):
    sock = socket(AF_INET, SOCK_DGRAM)
    sock.bind((BROADCAST_IP, BROADCAST_PORT))
    while True:
      data, addr = sock.recvfrom(1024)
      if data == BROADCAST_MSG:
        self.master_addr = addr
        break

if __name__ == "__main__":
  if len(argv) <= 1:
    print "Not enough arguments"
  elif argv[1].lower() == "master":
    print "Running as Master"
    Master().Run()
  elif argv[1].lower() == "worker":
    print "Running as Worker"
    Worker().Run()
  else:
    print "Ambiguous"
