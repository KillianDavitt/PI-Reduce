import argparse
from map2 import Master

parser = argparse.ArgumentParser()
parser.add_argument("--map", required=True)
parser.add_argument("--partition", required=True)
parser.add_argument("--reduce", required=True)
parser.add_argument("--values", required=True)
parser.add_argument("--broadcast_ip", required=True)

if __name__ == "__main__":
  args = parser.parse_args()
  with open(args.map, "r") as f:
    map_f = f.read()
  with open(args.partition, "r") as f:
    part_f = f.read()
  with open(args.reduce, "r") as f:
    reduce_f = f.read()
  with open(args.values, "r") as f:
    lines = f.read().split("\n")
  Master(map_f, part_f, reduce_f, lines, args.broadcast_ip)
