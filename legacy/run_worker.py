import argparse
from map2 import Worker

parser = argparse.ArgumentParser()
parser.add_argument("--broadcast_ip", required=True)

if __name__ == "__main__":
  args = parser.parse_args()
  Worker(args.broadcast_ip)