package main

import (
  "log"
  "os"

  "github.com/pushkar-gr/AlakaAssignment/src"
)

//path of file to read
const directory = "data/candles/BANKNIFTY/2024-01-10/"
//path to store csv file
const csvDir = "5min_candles/"
//day of data to be processed
const day = "2024-01-10"

func main() {
  directories, err := os.ReadDir(directory)
  if err != nil {
    log.Fatalf("Error reaing directory entries %v\n%v", directory, err)
    return
  }

  for _, dir := range directories {
    filePath := directory + dir.Name()
    err := src.ConvertTo5minCandle(filePath, csvDir, day)
    if err != nil {
      log.Fatalf("Error prasing file %v\n%v", filePath, err)
    }
  }
}
