# Goroutines and Channels (Go)

A university laboratory project demonstrating **concurrent data processing in Go** using goroutines and channels instead of shared memory and synchronization monitors.

The program reads exercise data from a JSON file, calculates a score for each exercise, filters the results, and saves them to a text file together with worker statistics.

## How it works

- **Main goroutine** reads the JSON file, starts the other goroutines, sends exercises, and saves the final output.
- **Data manager** stores exercises in a circular buffer with a maximum size of `n / 2` and sends them to workers on request.
- **Workers** calculate exercise scores, send results where `score > 300`, and report how many exercises they processed and passed.
- **Result manager** keeps the accepted results sorted by score and collects statistics from every worker.

All communication and synchronization use **unbuffered channels**. The program starts `n / 4` workers and finishes automatically after processing all exercises.

## Requirements

- Go 1.21 or newer
- At least 8 exercises in the input JSON file

## Running the program

From the project directory:

```bash
go run .
```

By default, the program reads `IFF4-3_StelmokasO_L1_dat_2.json` and writes `IFF4-3_StelmokasO_L1_rez.txt`.

You can also specify the input and output files:

```bash
go run . IFF4-3_StelmokasO_L1_dat_1.json
go run . IFF4-3_StelmokasO_L1_dat_3.json results.txt
```

## Input and output

Each exercise in the JSON file has a name, repetition count, and weight:

```json
{
  "exercises": [
    { "name": "Bench Press", "reps": 10, "weight": 80 }
  ]
}
```

The output text file contains a table of exercises that passed the score threshold, sorted in ascending order, followed by each worker's statistics (`Processed` and `Passed`).

## Technologies

Go · Goroutines · Unbuffered channels · JSON
