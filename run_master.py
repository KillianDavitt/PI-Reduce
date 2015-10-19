import argparse
import logging
from map_reduce import Master

parser = argparse.ArgumentParser()
parser.add_argument("--map", required=True)
parser.add_argument("--data", required=True)
parser.add_argument("--broadcast_ip", required=True)

if __name__ == "__main__":
  logging.basicConfig(level=0)
  args = parser.parse_args()
  with open(args.map, "r") as f:
    map_f = f.read()
  with open(args.data, "r") as f:
    pairs = [(k, v) for k, v in enumerate(f.read().split("\n"))]
  logging.info(pairs)
  Master(args.broadcast_ip, map_f, pairs)
