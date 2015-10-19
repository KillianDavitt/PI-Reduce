import json
import logging
import socket
import sys
import time
import threading

WORKER_PORT = 8001
MASTER_PORT = 8002
REGISTRATION_TIMEOUT = 1
BROADCAST_MSG = "BROADCAST!"
REGISTER_MSG = "PLS REGISTER ME"
CONFIRMATION_MSG = "CONFIRMED!"
MAP_FINISHED_MSG = "MAP FINISHED!"
START_REDUCING_MSG = "STTSRASGUHIJK"
MAP_PREFIX = "MAP_PREFIX"
WORKERS_PREFIX = "WORKERS_PREFIX"

class Master:

  def __init__(self, broadcast_ip, map_f, pairs):
    """
    Args:
      broadcast_ip: a string, the IP address to broadcast on
      pairs: a list of tuples where the tuples are key value pairs
    """
    ip = socket.gethostbyname(socket.gethostname())
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, MASTER_PORT)) # Listen on (ip, MASTER_PORT)
    sock.sendto(BROADCAST_MSG, (broadcast_ip, WORKER_PORT)) # Send to (broadcast_ip, WORKER_PORT)

    # Register workers.
    workers = []
    start_time = time.time()
    while time.time() < start_time + REGISTRATION_TIMEOUT:
      time_left = start_time + REGISTRATION_TIMEOUT - time.time()
      sock.settimeout(abs(time_left))
      try:
        data, addr = sock.recvfrom(1024)
        logging.debug(data)
        if data == REGISTER_MSG:
          logging.info("Registering %s", addr)
          workers.append(addr)
          sock.sendto(CONFIRMATION_MSG, addr)
      except socket.timeout:
        logging.info("Socket timed out")

    # Quit if no workers.
    if not workers:
      logging.warn("No workers found.")
      return

    # Send functions to workers.
    for worker in workers:
      sock.sendto(MAP_PREFIX + map_f, worker)
      sock.sendto(WORKERS_PREFIX + json.dumps(worker), worker)

    # Send data to workers.
    pairs_per_worker = len(pairs) / len(workers)
    remainder_pairs = len(pairs) % len(workers)
    start_index = 0
    for worker in workers:
      connected = False
      while not connected:
        try:
          sock = socket.create_connection(worker)
          connected = True
          logging.info("connected")
        except socket.error:
          logging.warn("Could not connect")
      end_index = start_index + pairs_per_worker
      if remainder_pairs:
        end_index += 1
        remainder_pairs -= 1
      sock.send(json.dumps(pairs[start_index:end_index]))
      sock.close()

    # Wait for workers to finish.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, MASTER_PORT)) # Listen on (ip, MASTER_PORT)
    workers_finished = 0
    while workers_finished < len(workers):
      data, addr = sock.recvfrom(1024)
      if data == MAP_FINISHED_MSG:
        workers_finished += 1
        logging.info("Worker finished.")
    logging.info("Workers finished mapping!")

    # Notify workers to start reducing.
    for worker in workers:
      sock.sendto(START_REDUCING_MSG, worker)

class Worker:

  def __init__(self, broadcast_ip):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((broadcast_ip, WORKER_PORT)) # Listen on (broadcast_ip, WORKER_PORT)

    # Register.
    registered = False
    while not registered:
      data, addr = sock.recvfrom(1024)
      logging.debug(data)
      if data == BROADCAST_MSG:
        logging.debug("Registering with master at " + str(addr))
        master_addr = addr
        sock.sendto(REGISTER_MSG, master_addr)
        sent = True
      elif data == CONFIRMATION_MSG:
        print "Registration complete"
        registered = True

    # Receive functions.
    map_data = None
    self.workers = None
    while not all([map_data, self.workers]):
      data, addr = sock.recvfrom(65535)
      if data.startswith(MAP_PREFIX):
        map_data = data[len(MAP_PREFIX):]
      elif data.startswith(WORKERS_PREFIX):
        self.workers = json.loads(data[len(WORKERS_PREFIX):])
    logging.info(self.workers)
    sock.close()

    # Receive data.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((broadcast_ip, WORKER_PORT))
    logging.info("bound!")
    sock.listen(1)
    logging.info("listened!")
    conn, addr = sock.accept()
    logging.info("connected!")
    logging.info(addr)
    data = ""
    while True:
      received_data = conn.recv(65535)
      if not received_data:
        break
      data += received_data
    pairs = json.loads(data)

    # Apply map function.
    exec(map_data)
    mapped_pairs = []
    for key, value in pairs:
      for new_pair in map_f(key, value):
        mapped_pairs.append(new_pair)
    print len(pairs)

    # Notify master that mapping is finished.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((broadcast_ip, WORKER_PORT)) # Listen on (broadcast_ip, WORKER_PORT)
    sock.sendto(MAP_FINISHED_MSG, master_addr)
    logging.info("Sent finished msg")

    # Wait until all workers are ready to start reducing.
    data, addr = sock.recvfrom(1024)
    if data == START_REDUCING_MSG:
      logging.info("Ready to reduce!")

    # Ready to receive data.
    receieve_thread = threading.Thread(target=ReceiveData)
    receive_thread.start()
    receieve_thread.join()

  def ReceiveData(self):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((broadcast_ip, WORKER_PORT))
    logging.info("bound!")
    sock.listen(len(workers))

