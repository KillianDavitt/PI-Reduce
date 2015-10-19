def map_f(_, line):
  counts = {}
  for word in line.split():
    if word in counts:
      counts[word] += 1
    else:
      counts[word] = 1
  result = []
  for word, count in counts.iteritems():
    result.append((word, count))
  return result

if __name__ == "__main__":
  print map_f(None, "I am I")