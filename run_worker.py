import argparse
import logging
from map_reduce import Worker

parser = argparse.ArgumentParser()
parser.add_argument("--broadcast_ip", required=True)

if __name__ == "__main__":
  logging.basicConfig(level=0)
  args = parser.parse_args()
  Worker(args.broadcast_ip)
