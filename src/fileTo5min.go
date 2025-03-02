package main

import (
  "fmt"
  "log"
  "io"
  "os"
  "time"
  "encoding/csv"
  "path"

  "github.com/parquet-go/parquet-go"
)

//path of file to read
const filePath = "data/candles/BANKNIFTY/2024-01-10/46900PE.parquet.gz"
//path to store csv file
const csvDir = "5min_candles/"
//day of data to be processed
const day = "2024-01-10"

//Candle structure
type Candle struct {
  Date int64 `parquet:"date"` //DateTime
  Open float32 `parquet:"open"` //opening value of candle
  High float32 `parquet:"high"` //maximum value of candle
  Low float32 `parquet:"low"` //minimum value of candle
  Close float32 `parquet:"close"` //closing value of candle
  Volume int32 `parquet:"volume"` //volume of candle
}

func main() {
  //get timeDate range to read data in
  start, end, err := getTimeRange(day)
  if err != nil {
    log.Fatalf("Error parsing date %v\n%v", day, err)
  }

  //open file
  rf, err := os.Open(filePath)
  if err != nil {
    log.Fatalf("Error opening file %v\n%v", filePath, err)
    return
  }
  defer rf.Close()

  //open or create csv file to write data
  csvPath := csvDir + path.Base(filePath)
  csvFile, err := os.OpenFile(csvPath, os.O_CREATE|os.O_WRONLY, 0644)
  if err != nil {
    log.Fatalf("Error opening file %v\n%v", csvPath, err)
    return
  }
  defer csvFile.Close()

  //use csv.NewWriter to write data
  csvW := csv.NewWriter(csvFile)
  defer csvW.Flush()
  
  //read parquet file
  pf := parquet.NewReader(rf)
  defer pf.Close()

  //process data
  EOF := false
  for {
    //5 min candle
    var candle Candle
    for i := 0; i < 5; i++ {
      //update candle with 5 data
      err := updateCandle(pf, &candle, start, end, i == 0, i == 4)
      if err == io.EOF {
        EOF = true
        break
      } else if err != nil {
        log.Fatalf("Error reading row \n%v", err)
      }
    }
    //break if EOF is reached
    if EOF {
      break
    }
    //write data to csv
    err := csvW.Write(toString(&candle))
    if err != nil {
      log.Fatalf("Error writing record to csv \n%v", err)
    }
  }
}

//input: candle
//output: slice of string
//convert candle to slice of string to write to csv
func toString(candle *Candle) []string {
  return []string{
    fmt.Sprintf("%v", candle.Date),
    fmt.Sprintf("%v", candle.Open),
    fmt.Sprintf("%v", candle.High),
    fmt.Sprintf("%v", candle.Low),
    fmt.Sprintf("%v", candle.Close),
    fmt.Sprintf("%v", candle.Volume),
  }
}

//input: parquet.Reader, candle, start time, end time for range, first, last indicating if candle is first or last candle of 5 min time stamp
//output: error if any
//update candle data from reader if in time range
func updateCandle(reader *parquet.Reader, candle *Candle, start, end int64, first, last bool) error {
  //create new candle to read data into
  newCandle := new(Candle)
  for {
    //read next row
    err := reader.Read(newCandle)
    if err != nil {
      return err
    }
    //convert time from nano sec to sec
    newCandle.Date /= 1000000000
    //check if date is in given range
    if newCandle.Date >= start && newCandle.Date < end {
      //if candle is first candle
      if first {
        *candle = *newCandle
      } else {
        //update candle data
        if candle.High < newCandle.High {
          candle.High = newCandle.High
        }
        if candle.Low > newCandle.Low {
          candle.Low = newCandle.Low
        }
        //update close data if candle is last candle
        if last {
          candle.Close = newCandle.Close
        }
        candle.Volume += newCandle.Volume
      }
      return nil
    }
  }
  return nil
}

//input: DateDMY
//output start time, end time, error
//takes date in string and returns time range
func getTimeRange(day string) (int64, int64, error) {
  //calculate start time
  dayStart := day + " 00:00:00"
  start, err := time.Parse("2006-01-02 15:04:05", dayStart)
  if err != nil {
    return 0, 0, err
  }

  //calculate end time
  dayEnd := day + " 23:59:59"
  end, err := time.Parse("2006-01-02 15:04:05", dayEnd)
  if err != nil {
    return 0, 0, err
  }

  return start.Unix(), end.Unix(), nil
}
