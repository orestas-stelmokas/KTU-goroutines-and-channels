// Task: Process exercise data from a JSON file using Go goroutines
// and channels without shared memory or synchronization monitors.
//
// The main goroutine reads exercises and sends them to a data manager,
// which stores them in a limited-size buffer.
//
// Multiple workers request exercises, perform calculations, and send
// results that meet the filtering condition to a result manager.
//
// The result manager sorts the results, collects worker statistics,
// and sends the final data back to the main goroutine.
//
// All communication uses unbuffered channels to ensure synchronization.
// The program finishes automatically and saves results to a text file.

package main

//  "encoding/json" - for working with JSON format data files.
//  "fmt" - used to work with I/O functions
//  "math" - mathematical methods for the trivial function's calculations
//  "os" - working with files both for data retrieval and result writing
import (
	"encoding/json"
	"fmt"
	"math"
	"os"
)

// Exercise - data type for the exercise
// Name - name of the exercise, e.g. "Bench Press"
// Repetitions - count of repetitions done on the exercise
// Weight - weight of the equipment used to perfom the exercise
type Exercise struct {
	Name        string  `json:"name"`
	Repetitions int     `json:"reps"`
	Weight      float64 `json:"weight"`
}

// Data type for a list of exercises
// Exercises - a buffer with Exercise objects
type Exercises struct {
	Exercises []Exercise `json:"exercises"`
}

// Result - data type to store the result of the main trivial function
// Exercise - exercise, whose result is stores
// Score - calculated score for the exercise
type Result struct {
	Exercise Exercise
	Score    float64
}

// WorkerStats - data type for storing worker thread's statistics
// ID - id of the worker thread
// Processed - how many data (exercises) the thread has processed
// Passed - how many exercises met the criteria (Score > threshold)
type WorkerStats struct {
	ID        int
	Processed int
	Passed    int
}

// FinalResults - data structure to store the program's results
// Results - a buffer containing sorted list of results (Exercises and their Score)
// Stats - a buffer containing all worker threads' statistics
type FinalResults struct {
	Results []Result
	Stats   []WorkerStats
}

// Constants for the program
// dataFile - file name for the data (JSON format)
// resultFile - file name to print the results
// threshold - a boundary for determining whether the exercise passes compared to it's score
const (
	dataFile   = "IFF4-3_StelmokasO_L1_dat_2.json"
	resultFile = "IFF4-3_StelmokasO_L1_rez.txt"
	threshold  = 300.0
)

// calculateScore - calculates the score for given exercise
//
// The algorithm was free of choice, with only one criterion - it has to take some time
// So I made it calculate a square root, multiplication and two additions 10000 times
func calculateScore(ex Exercise) float64 {
	score := ex.Weight
	for i := 0; i < 10000; i++ {
		score = math.Sqrt(score*score + float64(ex.Repetitions) + 1)
	}
	return score
}

// dataThread - a method to handle data input from the main thread
// and pass data to worker threads.
//
// Parameters:
// input - data channel to get data (Exercise) sent from the main thread
// requests - channel to get a request from a worker thread (each worker has its own request channel)
// size - size for the data buffer (at most n/2 which is exactly what I chose,
// where n = count of data entries)
// workerCount - how many total worker threads are started
func dataThread(input <-chan Exercise, requests <-chan chan Exercise, size, workerCount int) {
	buffer := make([]Exercise, size) // A ring buffer to store data
	head, count, finished := 0, 0, 0

	for finished < workerCount {
		add := input // Renaming for convenience
		remove := requests

		if count == len(buffer) {
			add = nil // If the buffer is full, we do not accept more data.
		}
		if count == 0 && input != nil {
			remove = nil // If the buffer is empty, but the input channel is not closed,
			// thread rejects removal requests from worker threads.
		}

		select {
		case exercise, ok := <-add:
			if !ok { // If a channel returns ok = false, that means that the channel was closed.
				input = nil // The main thread has finished reading and sending the data.
				continue    // Break of the block and let worker threads finish the remaining data in the buffer.
			}
			buffer[(head+count)%len(buffer)] = exercise // Putting the exercise at the end of the list.
			count++

		case reply := <-remove:
			if count == 0 {
				close(reply) // If the buffer is empty and the input channel is closed,
				// it closes request channel indicating that the worker thread is done.
				finished++
				continue
			}
			reply <- buffer[head]           // Pass the first element of the list to the request channel (for the worker).
			buffer[head] = Exercise{}       // Delete the sent entry from the buffer.
			head = (head + 1) % len(buffer) // Calculate the new pointer to the first element.
			count--
		}
	}
}

// worker - a method for threads to process exercises and calculate their scores.
//
// Parameters:
// id - unique ID of the worker thread
// requests - channel to send requests for exercises to the data thread
// results - channel to send exercises that meet the criteria to the result thread
// statsChannel - channel to send the worker's statistics to the result thread
func worker(id int, requests chan<- chan Exercise, results chan<- Result, statsChannel chan<- WorkerStats) {
	stats := WorkerStats{ID: id}
	reply := make(chan Exercise) // Channel to receive exercises from the data thread.

	for {
		requests <- reply       // Send a request for a new exercise to the data thread.
		exercise, ok := <-reply // Wait until the data thread sends an exercise.
		if !ok {
			break // If the channel is closed, there are no more exercises to process.
		}

		score := calculateScore(exercise)
		stats.Processed++
		if score > threshold { // Check whether the exercise meets the criteria.
			results <- Result{Exercise: exercise, Score: score}
			stats.Passed++
		}
	}

	statsChannel <- stats // Send the worker's final statistics to the result thread.
}

// resultThread - a method to receive results from worker threads,
// store them in a sorted list and collect worker statistics.
//
// Parameters:
// results - channel to receive calculated results from worker threads
// statsChannel - channel to receive statistics from worker threads
// done - channel to send all results and statistics to the main thread
// n - total number of exercises in the data file
// workerCount - how many total worker threads are started
func resultThread(results <-chan Result, statsChannel <-chan WorkerStats, done chan<- FinalResults, n, workerCount int) {
	sorted := make([]Result, 0, n) // Create an empty list for sorted results with enough capacity
	// to fit all data if every entry meets the criteria
	stats := make([]WorkerStats, workerCount)

	for finished := 0; finished < workerCount; { // Loop until all workers are finished.
		select { // Wait until a result or worker statistics are received.
		case result := <-results:
			// Inserts the result to a sorted buffer.
			index := 0
			for index < len(sorted) && sorted[index].Score < result.Score {
				index++
			}
			sorted = append(sorted, Result{})
			copy(sorted[index+1:], sorted[index:])
			sorted[index] = result

		case stat := <-statsChannel:
			stats[stat.ID-1] = stat // Save the received statistics in the correct worker's position.
			finished++
		}
	}

	// Workers send their statistics only after sending all their results.
	// This means that all results have already been received when all workers are finished.
	done <- FinalResults{Results: sorted, Stats: stats} // Send the final sorted results and statistics to the main thread.
}

func main() {

	// If running via the terminal, check the arguments and correct the user if needed.
	if len(os.Args) > 3 {
		fmt.Println("Naudojimas: go run . [duomenu_failas.json] [rezultatu_failas.txt]")
		return
	}

	inputPath, outputPath := dataFile, resultFile
	if len(os.Args) > 1 {
		inputPath = os.Args[1]
	}
	if len(os.Args) > 2 {
		outputPath = os.Args[2]
	}

	data, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Println("Nepavyko nuskaityti failo:", err)
		return
	}

	var fileData Exercises // store Exercises from the JSON file.

	if err := json.Unmarshal(data, &fileData); err != nil {
		fmt.Println("Nepavyko nuskaityti JSON:", err)
		return
	}

	n := len(fileData.Exercises) // Count of data entries.
	if n < 8 {
		fmt.Println("Reikia bent 8 įrašų, kad būtų galima turėti 2–n/4 darbininkų.")
		return
	}
	workerCount := n / 4

	// All the channels for the program
	input := make(chan Exercise)
	requests := make(chan chan Exercise)
	results := make(chan Result)
	stats := make(chan WorkerStats)
	done := make(chan FinalResults)

	// Starting the data, result and worker threads.
	go dataThread(input, requests, n/2, workerCount)
	go resultThread(results, stats, done, n, workerCount)
	for i := 0; i < workerCount; i++ {
		go worker(i+1, requests, results, stats)
	}

	// Pass each exercise to the data thread.
	for _, exercise := range fileData.Exercises {
		input <- exercise
	}
	close(input) // When all data was sent, close the channel.

	final := <-done // Retrieve results

	// Print data to files
	if err := writeResults(outputPath, final.Results, final.Stats); err != nil {
		fmt.Println("Nepavyko įrašyti rezultatų:", err)
		return
	}
	fmt.Println("Rezultatai įrašyti į", outputPath)
}

// writeResults - function to write formatted results to a file.
func writeResults(path string, results []Result, stats []WorkerStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	fmt.Fprintf(file, "%-3s | %-24s | %-8s | %-8s | %-10s\n", "Nr.", "Pratimas", "Kartoj.", "Svoris", "Rezultatas")
	fmt.Fprintln(file, "--------------------------------------------------------------------")
	for i, result := range results {
		fmt.Fprintf(file, "%-3d | %-24s | %-8d | %-8.1f | %-10.2f\n",
			i+1, result.Exercise.Name, result.Exercise.Repetitions, result.Exercise.Weight, result.Score)
	}

	fmt.Fprintln(file, "\nGiju statistika:")
	fmt.Fprintf(file, "%-6s | %-10s | %-7s\n", "Gija", "Apdorota", "Atitiko")
	fmt.Fprintln(file, "-------------------------------")
	for _, stat := range stats {
		fmt.Fprintf(file, "%-6d | %-10d | %-7d\n", stat.ID, stat.Processed, stat.Passed)
	}
	return file.Close()
}
