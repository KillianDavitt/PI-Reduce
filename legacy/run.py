def RunMaster():
  print "Running as Master."
  if len(sys.argv < 5):
    print "Not enough arguments"
    return
  with open(sys.argv[1], "r") as f:
    map_f = f.read()
  with open(sys.argv[2], "r") as f:
    part_f = f.read()
  with open(sys.argv[3], "r") as f:
    reduce_f = f.read()
  with open(sys.argv[4], "r") as f:
    data = [(line.split()[0], line.split()[1]) for line in f.read.split("\n")]


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print "Not enough arguments."
  elif sys.argv[1].lower() == "master":
    RunMaster()
  elif sys.argv[1].lower() == "worker":
    RunWorker()
  else:
    print "Invalid options."