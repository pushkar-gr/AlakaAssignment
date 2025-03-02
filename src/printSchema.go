package main

import (
  "fmt"
  "log"
  "os"

  "github.com/parquet-go/parquet-go"
)

//path of file to read
const filePath = "data/candles/BANKNIFTY/2024-01-10/46900PE.parquet.gz"

func main() {
  //open file
  rf, err := os.Open(filePath)
  if err != nil {
    log.Fatalf("Error opening file %v\n%v", filePath, err)
    return
  }
  defer rf.Close()

  //get file stats
  info, err := rf.Stat()
  if err != nil {
    log.Fatalf("Error getting stats of file %v\n%v", filePath, err)
    return
  }

  //open file and read content
  file, err := parquet.OpenFile(rf, info.Size())
  if err != nil {
    log.Fatalf("Error opening file %v\n%v", filePath, err)
    return
  }
  
  //print file schema
  fmt.Println(file.Schema())
}
