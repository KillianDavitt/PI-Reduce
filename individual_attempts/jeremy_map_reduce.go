package main

import (
  "flag"
  "fmt"
  "strconv"
)

var FINISHED_SIG = StringTuple{strconv.Itoa(0), strconv.Itoa(0)}

type StringTuple struct {
  s0 string
  s1 string
}

type Mapper struct {
  data map[string]string
  reducer_channels []chan StringTuple
  map_f func(map[string]string) map[string]string
  dist_f func(StringTuple, int) int
}

func (mapper *Mapper) Run() {
  // Map
  mapper.data = map_f(mapper.data)

  // Shuffle
  for s0, s1 := range mapper.data {
    reducer_id := dist_f(StringTuple{s0, s1}, len(mapper.reducer_channels))
    mapper.reducer_channels[reducer_id] <- StringTuple{s0, s1}
  }
  for i := 0; i < len(mapper.reducer_channels); i++ {
    mapper.reducer_channels[i] <- FINISHED_SIG
  }
}

type Reducer struct {
  num_mappers int
  in_channel chan StringTuple
  main_channel chan StringTuple
  reduce_f func(map[string]string) map[string]string
}

func (reducer *Reducer) Run() {
  // Copy
  data := make(map[string]string)
  finished_mappers := 0
  for finished_mappers < reducer.num_mappers {
    s := <- reducer.in_channel
    if s == FINISHED_SIG {
      finished_mappers += 1
    } else {
      data[s.s0] = s.s1
    }
  }

  // Reduce
  data = reducer.reduce_f(data)

  // Output
  for k, v := range data {
    reducer.main_channel <- StringTuple{k, v}
  }
  reducer.main_channel <- FINISHED_SIG
}

func map_f(input map[string]string) map[string]string {
  return map[string]string{"jim": "jim"}
}


func dist_f(input StringTuple, num_Workers int) int {
  return 0
}

func MapReduce(num_mappers, num_reducers int,
               initial_data []map[string]string,
               map_f func(map[string]string) map[string]string,
               dist_f func(StringTuple, int) int,
               reduce_f func(map[string]string) map[string]string) {

  // Input channel for each Reducer.
  reducer_channels := make([]chan StringTuple, num_reducers)
  for i := 0; i < num_reducers; i++ {
    reducer_channels[i] = make(chan StringTuple)
  }

  // Mappers.
  mappers := make([]Mapper, num_mappers)
  for i := 0; i < num_mappers; i++ {
    mappers[i] = Mapper{initial_data[i], reducer_channels, map_f, dist_f}
    go mappers[i].Run()
  }

  // Channel for Reducer to send results to the main thread (this thread).
  main_channel := make(chan StringTuple, num_mappers)

  // Reducers.
  reducers := make([]Reducer, num_reducers)
  for i := 0; i < num_reducers; i++ {
    reducers[i] = Reducer{num_mappers, reducer_channels[i], main_channel, reduce_f}
    go reducers[i].Run()
  }

  // Print returned value from each Worker.
  finished_reducers := 0
  for finished_reducers < num_reducers {
    s := <- main_channel
    if s == FINISHED_SIG {
      finished_reducers += 1
    } else {
      fmt.Println(s)
    }
  }
}


func main() {
  flag.Parse()

  num_workers := strconv.Atoi(flag.Arg(0))
  num_reducers := strconv.Atoi(flag.Arg(1))

  initial_data := make([]map[string]string, num_workers)
  for i := 0; i < len(initial_data); i++ {
    initial_data[i] = map[string]string{"Toronto": "5"}
  }

  MapReduce(num_workers, num_reducers, initial_data, map_f, dist_f, map_f)
}
