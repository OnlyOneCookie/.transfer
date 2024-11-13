package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

// Result holds the statistics for a weather station
type Result struct {
	min   float64
	max   float64
	sum   float64
	count int64
}

// chunk represents a portion of the file to be processed
type chunk struct {
	offset int64
	size   int64
}

// Station holds the final statistics for output
type Station struct {
	Name string  `json:"name"`
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Mean float64 `json:"mean"`
}

// StationOutput is used for JSON file output
type StationOutput struct {
	Stations []Station `json:"stations"`
	Stats    struct {
		ProcessingTime string `json:"processing_time"`
		FileSize       int64  `json:"file_size_bytes"`
		NumStations    int    `json:"number_of_stations"`
	} `json:"stats"`
}

// parseTemp parses temperature value without using strconv
func parseTemp(s []byte) float64 {
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	val := 0.0
	decimal := false
	decimalPos := 1.0

	for _, c := range s {
		if c == '.' {
			decimal = true
			continue
		}
		if decimal {
			val = val + float64(c-'0')/10.0
			decimalPos *= 10
		} else {
			val = val*10 + float64(c-'0')
		}
	}

	if neg {
		return -val
	}
	return val
}

// processChunk processes a chunk of the file and returns station statistics
func processChunk(file *os.File, chunk chunk) map[string]*Result {
	results := make(map[string]*Result)
	buf := make([]byte, chunk.size)

	file.Seek(chunk.offset, 0)
	_, err := io.ReadFull(file, buf)
	if err != nil {
		return results
	}

	start := 0
	if chunk.offset != 0 {
		for start < len(buf) && buf[start] != '\n' {
			start++
		}
		start++
	}

	end := len(buf)
	for end > 0 && buf[end-1] != '\n' {
		end--
	}

	buf = buf[start:end]

	pos := 0
	for pos < len(buf) {
		semicolon := pos
		for buf[semicolon] != ';' {
			semicolon++
		}

		stationName := string(buf[pos:semicolon])

		tempStart := semicolon + 1
		tempEnd := tempStart
		for tempEnd < len(buf) && buf[tempEnd] != '\n' {
			tempEnd++
		}

		temp := parseTemp(buf[tempStart:tempEnd])

		if result, exists := results[stationName]; exists {
			if temp < result.min {
				result.min = temp
			}
			if temp > result.max {
				result.max = temp
			}
			result.sum += temp
			result.count++
		} else {
			results[stationName] = &Result{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		}

		pos = tempEnd + 1
	}

	return results
}

func formatFileSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	startTime := time.Now()

	if len(os.Args) != 2 {
		fmt.Println("Usage: program <inputfile>")
		os.Exit(1)
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := fileInfo.Size()

	numCPUs := runtime.NumCPU()
	chunkSize := fileSize / int64(numCPUs)
	if chunkSize < 1024*1024 {
		chunkSize = 1024 * 1024
	}

	var chunks []chunk
	for offset := int64(0); offset < fileSize; offset += chunkSize {
		size := chunkSize
		if offset+size > fileSize {
			size = fileSize - offset
		}
		chunks = append(chunks, chunk{offset: offset, size: size})
	}

	var wg sync.WaitGroup
	resultsChan := make(chan map[string]*Result, len(chunks))

	for _, c := range chunks {
		wg.Add(1)
		go func(c chunk) {
			defer wg.Done()
			results := processChunk(file, c)
			resultsChan <- results
		}(c)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	finalResults := make(map[string]*Result)
	for results := range resultsChan {
		for station, result := range results {
			if final, exists := finalResults[station]; exists {
				if result.min < final.min {
					final.min = result.min
				}
				if result.max > final.max {
					final.max = result.max
				}
				final.sum += result.sum
				final.count += result.count
			} else {
				finalResults[station] = result
			}
		}
	}

	stations := make([]Station, 0, len(finalResults))
	for name, result := range finalResults {
		stations = append(stations, Station{
			Name: name,
			Min:  result.min,
			Max:  result.max,
			Mean: result.sum / float64(result.count),
		})
	}

	sort.Slice(stations, func(i, j int) bool {
		return stations[i].Name < stations[j].Name
	})

	// Prepare output
	output := StationOutput{
		Stations: stations,
	}
	output.Stats.ProcessingTime = time.Since(startTime).String()
	output.Stats.FileSize = fileSize
	output.Stats.NumStations = len(stations)

	// Write to JSON file
	jsonData, err := json.MarshalIndent(output, "", "    ")
	if err != nil {
		panic(err)
	}

	err = os.WriteFile("results.json", jsonData, 0644)
	if err != nil {
		panic(err)
	}

	// Console output
	fmt.Println("\n🌍 1BRC (One Billion Row Challenge) Results")
	fmt.Println("==========================================")
	fmt.Printf("Input File Size: %s\n", formatFileSize(fileSize))
	fmt.Printf("Number of Stations: %d\n", len(stations))
	fmt.Printf("Processing Time: %s\n", output.Stats.ProcessingTime)
	fmt.Println("\nTop 5 Stations by Temperature Range:")
	fmt.Println("------------------------------------")

	// Sort by temperature range for display
	sort.Slice(stations, func(i, j int) bool {
		rangeI := stations[i].Max - stations[i].Min
		rangeJ := stations[j].Max - stations[j].Min
		return rangeJ < rangeI
	})

	for i := 0; i < min(5, len(stations)); i++ {
		station := stations[i]
		fmt.Printf("%s:\n", station.Name)
		fmt.Printf("  Min: %.1f°C\n", station.Min)
		fmt.Printf("  Max: %.1f°C\n", station.Max)
		fmt.Printf("  Mean: %.1f°C\n", station.Mean)
		fmt.Printf("  Range: %.1f°C\n", station.Max-station.Min)
		if i < min(4, len(stations)-1) {
			fmt.Println()
		}
	}

	fmt.Println("\n✅ Full results have been written to results.json")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
