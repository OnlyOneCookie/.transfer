package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/stergiotis/boxer/public/fffi/runtime"
	"github.com/stergiotis/boxer/public/imzero/application"
	"github.com/stergiotis/boxer/public/imzero/imgui"
	"os"
	"sort"
)

// Station holds the statistics for a weather station
type Station struct {
	Name string
	Min  float64
	Max  float64
	Mean float64
}

// GUI state
type AppState struct {
	stations     []Station
	inputFile    [256]byte
	error        string
	isProcessing bool
}

var state = &AppState{
	stations:     make([]Station, 0),
	isProcessing: false,
}

// Initialize the input file with default value
func init() {
	copy(state.inputFile[:], "measurements.txt")
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

	for _, c := range s {
		if c == '.' {
			decimal = true
			continue
		}
		if decimal {
			val = val + float64(c-'0')/10.0
		} else {
			val = val*10 + float64(c-'0')
		}
	}

	if neg {
		return -val
	}
	return val
}

func calculateStats(filename string) error {
	// Reset state
	state.error = ""

	// Open and read file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Process file line by line
	stations := make(map[string]*struct {
		min   float64
		max   float64
		sum   float64
		count int64
	})

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()

		// Find separator
		sepIndex := bytes.IndexByte(line, ';')
		if sepIndex == -1 {
			continue
		}

		// Extract station name and temperature
		stationName := string(line[:sepIndex])
		temp := parseTemp(line[sepIndex+1:])

		// Update station stats
		if station, exists := stations[stationName]; exists {
			if temp < station.min {
				station.min = temp
			}
			if temp > station.max {
				station.max = temp
			}
			station.sum += temp
			station.count++
		} else {
			stations[stationName] = &struct {
				min   float64
				max   float64
				sum   float64
				count int64
			}{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	// Convert results to slice of stations
	state.stations = make([]Station, 0, len(stations))
	for name, stats := range stations {
		state.stations = append(state.stations, Station{
			Name: name,
			Min:  stats.min,
			Max:  stats.max,
			Mean: stats.sum / float64(stats.count),
		})
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func renderUI(marshaller *runtime.Marshaller) error {
	imgui.Begin("1BRC - One Billion Row Challenge")

	// File input
	input := string(bytes.TrimRight(state.inputFile[:], "\x00"))
	outText, changed := imgui.InputText("Input File", input, imgui.Size_t(len(state.inputFile)))
	if changed {
		copy(state.inputFile[:], outText)
	}

	// Calculate button
	if imgui.Button("Calculate") && !state.isProcessing {
		state.isProcessing = true
		go func() {
			inputFile := string(bytes.TrimRight(state.inputFile[:], "\x00"))
			err := calculateStats(inputFile)
			if err != nil {
				state.error = err.Error()
			}
			state.isProcessing = false
		}()
	}

	// Show processing indicator
	if state.isProcessing {
		imgui.Text("Processing... Please wait")
	}

	// Show error if any
	if state.error != "" {
		imgui.Text(state.error)
	}

	// Results section
	if len(state.stations) > 0 {
		imgui.Separator()
		imgui.Text(fmt.Sprintf("Number of Stations: %d", len(state.stations)))

		// Top stations table
		if imgui.BeginTable("Stations", 5) {
			// Sort by temperature range
			sortedStations := make([]Station, len(state.stations))
			copy(sortedStations, state.stations)
			sort.Slice(sortedStations, func(i, j int) bool {
				rangeI := sortedStations[i].Max - sortedStations[i].Min
				rangeJ := sortedStations[j].Max - sortedStations[j].Min
				return rangeJ < rangeI
			})

			// Show table headers
			imgui.TableSetupColumn("Station")
			imgui.TableSetupColumn("Min °C")
			imgui.TableSetupColumn("Max °C")
			imgui.TableSetupColumn("Mean °C")
			imgui.TableSetupColumn("Range °C")
			imgui.TableHeadersRow()

			// Show top 10 stations
			for i := 0; i < min(10, len(sortedStations)); i++ {
				station := sortedStations[i]
				imgui.TableNextRow()
				imgui.TableNextColumn()
				imgui.Text(station.Name)
				imgui.TableNextColumn()
				imgui.Text(fmt.Sprintf("%.1f", station.Min))
				imgui.TableNextColumn()
				imgui.Text(fmt.Sprintf("%.1f", station.Max))
				imgui.TableNextColumn()
				imgui.Text(fmt.Sprintf("%.1f", station.Mean))
				imgui.TableNextColumn()
				imgui.Text(fmt.Sprintf("%.1f", station.Max-station.Min))
			}
			imgui.EndTable()
		}
	}

	imgui.End()
	return nil
}

func main() {
	// Create application configuration
	cfg := &application.Config{
		UseWasm:              false,
		ImGuiBinary:          "", // We're not using a separate binary
		MainFontTTF:          "", // Default font
		MainFontSizeInPixels: 16,
		MaxRelaunches:        1,
	}

	// Create and initialize the application
	app, err := application.NewApplication(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to create application: %v", err))
	}

	// Set the render handler
	app.RenderLoopHandler = renderUI

	// Launch the application
	err = app.Launch()
	if err != nil {
		panic(fmt.Sprintf("Failed to launch application: %v", err))
	}

	// Run the application
	err = app.Run()
	if err != nil {
		panic(fmt.Sprintf("Failed to run application: %v", err))
	}
}
