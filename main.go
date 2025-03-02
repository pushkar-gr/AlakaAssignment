package main

import (
  "log"
  "os"
  "fmt"

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
    R1, R2, R3, S1, S2, S3, err := src.ConvertTo5minCandle(filePath, csvDir, day)
    if err != nil {
      log.Fatalf("Error prasing file %v\n%v", filePath, err)
    }
    fmt.Printf("Generaged csv for %v\n\tR1 = %.5f, R2 = %.5f, R3 = %.5f, S1 = %.5f, S2 = %.5f, S3 = %.5f\n", filePath, R1, R2, R3, S1, S2, S3)
  }
}
