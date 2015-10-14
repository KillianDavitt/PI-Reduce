def map(_, line):
  words = line.split()
  count = {}
  for word in words:
    if word not in count:
      count[word] = 1
    else:
      count[word] += 1

def reduce(word, counts):
  return (word, sum(counts))