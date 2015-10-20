package main

import (
  "crypto/md5"
  "fmt"
  "io/ioutil"
  "os"
  "strconv"
  "strings"
)

var FINISHED_SIG = StringTuple{strconv.Itoa(0), strconv.Itoa(0)}

type StringTuple struct {
  s0 string
  s1 string
}

func merge(a []StringTuple, b []StringTuple) []StringTuple {
  var r = make([]StringTuple, len(a) + len(b))
  var i = 0
  var j = 0
  for i < len(a) && j < len(b) {
    val_ai, _ := strconv.Atoi(a[i].s1)
    val_bj, _ := strconv.Atoi(b[j].s1)
    if val_ai <= val_bj {
      r[i+j] = a[i]
      i++
    } else {
      r[i+j] = b[j]
      j++
    }
  }
  for i < len(a) { r[i+j] = a[i]; i++ }
  for j < len(b) { r[i+j] = b[j]; j++ }
  return r
}

func Mergesort(items []StringTuple) []StringTuple {
  if len(items) < 2 {
    return items
  }
  var middle = len(items) / 2
  var a = Mergesort(items[:middle])
  var b = Mergesort(items[middle:])
  return merge(a, b)
}

type Mapper struct {
  id int
  data []StringTuple
  reducer_channels []chan StringTuple
  map_f func(StringTuple) []StringTuple
  dist_f func(StringTuple, int) int
}

func (mapper *Mapper) Run() {

  // Map
  mapped_data := make([]StringTuple, 0)
  tenth_index := len(mapper.data) / 10
  for i := 0; i < len(mapper.data); i++ {
    map_out := mapper.map_f(mapper.data[i])
    for j := 0; j < len(map_out); j++ {
      mapped_data = append(mapped_data, map_out[j])
    }
    if i % tenth_index == 0 {
      fmt.Printf("Mapper %d: %03d%%\n", mapper.id, (i / tenth_index) * 10)
    }
  }

  // Shuffle
  for i := range mapped_data {
    tuple := mapped_data[i]
    reducer_id := dist_f(tuple, len(mapper.reducer_channels))
    mapper.reducer_channels[reducer_id] <- StringTuple{tuple.s0, tuple.s1}
  }
  for i := 0; i < len(mapper.reducer_channels); i++ {
    mapper.reducer_channels[i] <- FINISHED_SIG
  }
}

type Reducer struct {
  id int
  num_mappers int
  in_channel chan StringTuple
  main_channel chan StringTuple
  reduce_f func([]StringTuple, int) []StringTuple
}

func (reducer *Reducer) Run() {

  // Copy
  data := make([]StringTuple, 0)
  finished_mappers := 0
  for finished_mappers < reducer.num_mappers {
    s := <- reducer.in_channel
    if s == FINISHED_SIG {
      finished_mappers += 1
    } else {
      data = append(data, s)
    }
  }

  // Reduce
  data = reducer.reduce_f(data, reducer.id) // []StringTuple

  // Output
  for _, v := range data { // int (index), StringTuple
    reducer.main_channel <- StringTuple{v.s0, v.s1}
  }
  reducer.main_channel <- FINISHED_SIG
}

func map_f(input StringTuple) []StringTuple {
  counts := make(map[string]int, 0)
  invalid := []string{".", ",", "\n", "”", "“", "?", "!", ":", ";", "–"}
  for i := 0; i < len(invalid); i++ {
    input.s1 = strings.Replace(input.s1, invalid[i], " ", -1)
  }
  words := strings.Split(input.s1, " ")
  for i := range words {
    word := words[i]
    value, ok := counts[word]
    if ok {
      counts[word] = value + 1
    } else if len(word) > 1 {
      counts[word] = 1
    }
  }
  result := make([]StringTuple, 0)
  for i := range counts {
    result = append(result, StringTuple{i, strconv.Itoa(counts[i])})
  }
  return result
}

func reduce_f(pairs []StringTuple, id int) []StringTuple {
  // Create map {word, count}.
  counts := make(map[string]string, 0) // word, "int"
  tenth_index := len(pairs) / 10
  for i := range pairs { // for each entry in key-value pairs
    word := pairs[i].s0
    count, _ := strconv.Atoi(pairs[i].s1)
    _, ok := counts[word]
    if ok {
      old_count, _ := strconv.Atoi(counts[word])
      counts[word] = strconv.Itoa(old_count + count)
    } else {
      counts[word] = "1"
    }
    if i % tenth_index == 0 {
      fmt.Printf("Reducer %d: %03d%%\n", id, (i / tenth_index) * 10)
    }
  }

  // Convert to []StringTuple.
  result := make([]StringTuple, 0) // [{string, string}]
  for word := range counts {
    tuple := StringTuple{word, counts[word]}
    result = append(result, tuple)
  }
  return result
}

func dist_f(input StringTuple, num_Workers int) int {
  hash := md5.Sum(Stoba(input.s0))
  sum := 0
  for i := range hash {
    sum += int(hash[i])
  }// return hash % num_Workers
  return sum % num_Workers
}

func Stoba(s string) []byte {
  ba := make([]byte, 0)
  for i := 0; i < len(s); i++ { // for index in string
    ba = append(ba, s[i])
  }
  return ba
}

func MapReduce(num_mappers, num_reducers, lines_to_print int,
               initial_data [][]StringTuple,
               map_f func(StringTuple) []StringTuple,
               dist_f func(StringTuple, int) int,
               reduce_f func([]StringTuple, int) []StringTuple) {

  // Input channel for each Reducer.
  reducer_channels := make([]chan StringTuple, num_reducers)
  for i := 0; i < num_reducers; i++ {
    reducer_channels[i] = make(chan StringTuple)
  }

  // Mappers.
  mappers := make([]Mapper, num_mappers)
  for i := 0; i < num_mappers; i++ {
    mappers[i] = Mapper{i, initial_data[i], reducer_channels, map_f, dist_f}
    go mappers[i].Run()
  }

  // Channel for Reducer to send results to the main thread (this thread).
  main_channel := make(chan StringTuple, num_mappers)

  // Reducers.
  reducers := make([]Reducer, num_reducers)
  for i := 0; i < num_reducers; i++ {
    reducers[i] = Reducer{i, num_mappers, reducer_channels[i], main_channel, reduce_f}
    go reducers[i].Run()
  }

  // Print returned value from each Worker.
  result := make([]StringTuple, 0)
  finished_reducers := 0
  for finished_reducers < num_reducers {
    s := <- main_channel
    if s == FINISHED_SIG {
      finished_reducers += 1
    } else {
      result = append(result, s)
    }
  }
  result = Mergesort(result)
  fmt.Printf("\n%d most occuring words:\n", lines_to_print)
  for i := len(result) - 1; i > len(result) - 1 - lines_to_print; i-- {
    amount, _ := strconv.Atoi(result[i].s1)
    fmt.Printf("%05d\t%s\n", amount, result[i].s0)
  }
}

func main() {

  num_workers, _ := strconv.Atoi(os.Args[1])
  num_reducers, _ := strconv.Atoi(os.Args[2])
  lines_to_print, _ := strconv.Atoi(os.Args[4])

  file, _ := ioutil.ReadFile(os.Args[3])
  lines := strings.Split(string(file), "\n")
  pairs := make([]StringTuple, len(lines))
  for i := 0; i < len(lines); i++ {
    pairs[i] = StringTuple{strconv.Itoa(i), lines[i]}
  }
  // TODO PRint percentages across the screen
  data := make([][]StringTuple, num_workers)
  pairs_per_worker := len(pairs) / num_workers
  remaining_pairs := len(pairs) % num_workers
  lo_index := 0
  for i := 0; i < num_workers; i++ {
    hi_index := lo_index + pairs_per_worker
    if remaining_pairs > 0 {
      hi_index += 1
      remaining_pairs -= 1
    }
    data[i] = make([]StringTuple, 0)
    for j := lo_index; j < hi_index; j++ {
      data[i] = append(data[i], pairs[j])
    }
    lo_index = hi_index
  }


  MapReduce(num_workers, num_reducers, lines_to_print, data, map_f, dist_f, reduce_f)
}
