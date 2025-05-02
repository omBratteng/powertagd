package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv" // Added for parsing numbers
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

const ProgName string = "powertag2influx"

// Define a struct to represent the data we want to send to MQTT
// We'll use more specific field names based on the sample data
type PowerTagData struct {
	VoltageP1          float64 `json:"voltage_p1,omitempty"`
	CurrentP1          float64 `json:"current_p1,omitempty"`
	TotalPowerActive   float64 `json:"total_power_active,omitempty"`
	PowerP1Active      float64 `json:"power_p1_active,omitempty"`
	TotalPowerApparent float64 `json:"total_power_apparent,omitempty"`
	Freq               float64 `json:"freq,omitempty"`
	PowerFactor        float64 `json:"power_factor,omitempty"`
	// Add other relevant fields if needed
}

func main() {
	var url string
	var token string
	var orgId string
	var bucket string

	var mqttBroker string
	var mqttTopicPrefix string // Changed to prefix to allow for device ID in topic
	var mqttClientID string
	var mqttUsername string
	var mqttPassword string

	flag.StringVar(&url, "url", "http://localhost:8086", "InfluxDB server URL")
	flag.StringVar(&token, "token", "", "InfluxDB auth token")
	flag.StringVar(&orgId, "orgId", "", "InfluxDB organization ID")
	flag.StringVar(&bucket, "bucket", "", "InfluxDB bucket")

	flag.StringVar(&mqttBroker, "mqtt-broker", "", "MQTT broker URL (e.g., tcp://localhost:1883)")
	flag.StringVar(&mqttTopicPrefix, "mqtt-topic-prefix", "", "MQTT topic prefix to publish data to (device ID will be appended)")
	flag.StringVar(&mqttClientID, "mqtt-clientid", ProgName, "MQTT client ID")
	flag.StringVar(&mqttUsername, "mqtt-username", "", "MQTT username (optional)")
	flag.StringVar(&mqttPassword, "mqtt-password", "", "MQTT password (optional)")

	flag.Parse()

	// InfluxDB argument validation
	if token == "" {
		fmt.Fprintf(os.Stderr, "%s: --token argument is required\n", ProgName)
		os.Exit(2)
	}
	if orgId == "" {
		fmt.Fprintf(os.Stderr, "%s: --orgId argument is required\n", ProgName)
		os.Exit(2)
	}
	if bucket == "" {
		fmt.Fprintf(os.Stderr, "%s: --bucket argument is required\n", ProgName)
		os.Exit(2)
	}

	// Standard input check
	stat, _ := os.Stdin.Stat()
	if stat.Mode()&os.ModeCharDevice != 0 {
		fmt.Fprintf(os.Stderr, "%s: no data on stdin\n", ProgName)
		fmt.Fprintf(os.Stderr, "%s expects data to be piped to stdin, i.e.:\n", ProgName)
		fmt.Fprintf(os.Stderr, "    powertagd | powertag2influx\n")
		os.Exit(2)
	}

	fmt.Printf("Debug: InfluxDB URL is: '%s'\n", url)

	// --- InfluxDB Client Setup ---
	opts := influxdb2.DefaultOptions()
	opts.SetApplicationName(ProgName)
	opts.SetLogLevel(1) // warn
	opts.SetPrecision(time.Second)
	opts.SetFlushInterval(1000 * 30) // 30s
	opts.SetBatchSize(10)

	client := influxdb2.NewClientWithOptions(url, token, opts)
	defer client.Close()

	health, err := client.Health(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", ProgName, err)
		fmt.Fprintf(os.Stderr, "%s: failed connecting to InfluxDB server", ProgName)
		os.Exit(1)
	}

	fmt.Printf("%s: connected to InfluxDB at %s (%s %s)\n", ProgName, url, health.Name, *health.Version)

	writeAPI := client.WriteAPI(orgId, bucket)
	defer writeAPI.Flush()

	// Get errors channel for InfluxDB writes
	errorsCh := writeAPI.Errors()
	// Create go proc for reading and logging InfluxDB errors
	go func() {
		for err := range errorsCh {
			fmt.Fprintf(os.Stderr, "%s: influxdb write error: %s\n", ProgName, err.Error())
		}
	}()

	// --- MQTT Client Setup (if broker is specified) ---
	var mqttClient mqtt.Client
	mqttEnabled := false // Flag to track if MQTT is enabled
	if mqttBroker != "" && mqttTopicPrefix != "" {
		mqttOpts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID(mqttClientID)
		if mqttUsername != "" {
			mqttOpts.SetUsername(mqttUsername)
		}
		if mqttPassword != "" {
			mqttOpts.SetPassword(mqttPassword)
		}

		mqttClient = mqtt.NewClient(mqttOpts)
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			fmt.Fprintf(os.Stderr, "%s: failed connecting to MQTT broker at %s: %v\n", ProgName, mqttBroker, token.Error())
			// We won't exit here, the program can still write to InfluxDB
		} else if token.Error() == nil {
			fmt.Printf("%s: connected to MQTT broker at %s\n", ProgName, mqttBroker)
			defer mqttClient.Disconnect(250) // Disconnect gracefully on exit
			mqttEnabled = true               // Set flag if connected
		}
	} else if mqttBroker != "" || mqttTopicPrefix != "" {
		// Warn if only one of broker or topic prefix is provided
		fmt.Fprintf(os.Stderr, "%s: warning: both --mqtt-broker and --mqtt-topic-prefix must be specified to enable MQTT\n", ProgName)
	}

	// --- Read from Stdin and Process ---
	lnscan := bufio.NewScanner(os.Stdin)
	for lnscan.Scan() {
		line := lnscan.Text()

		// Write to InfluxDB
		writeAPI.WriteRecord(line)

		// Process for MQTT if enabled and client is connected
		if mqttEnabled && mqttClient.IsConnected() {
			// Attempt to parse the InfluxDB line protocol for MQTT
			deviceID, powerTagData, parseErr := parseInfluxLineForMQTT(line)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "%s: failed to parse influxdb line for mqtt: %v (line: %s)\n", ProgName, parseErr, line)
				// Continue processing the next line
				continue
			}

			// Construct the MQTT topic using the prefix and device ID
			mqttTopic := fmt.Sprintf("%s/%s/state", strings.TrimSuffix(mqttTopicPrefix, "/"), deviceID)

			// Marshal the data into JSON
			jsonData, marshalErr := json.Marshal(powerTagData)
			if marshalErr != nil {
				fmt.Fprintf(os.Stderr, "%s: failed to marshal power data to json: %v\n", ProgName, marshalErr)
				// Continue processing the next line
				continue
			}

			// Publish the JSON to MQTT
			token := mqttClient.Publish(mqttTopic, 0, false, jsonData) // QoS 0, not retained
			token.Wait()
			if token.Error() != nil {
				fmt.Fprintf(os.Stderr, "%s: mqtt publish error: %v\n", ProgName, token.Error())
			}
		}
	}

	if err := lnscan.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error reading from stdin: %v\n", ProgName, err)
		os.Exit(1)
	}
}

// parseInfluxLineForMQTT attempts to parse an InfluxDB Line Protocol string
// and extract the device ID and relevant power-related fields.
// It returns the device ID, a PowerTagData struct, and an error.
func parseInfluxLineForMQTT(line string) (string, *PowerTagData, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", nil, fmt.Errorf("invalid influxdb line protocol: not enough parts")
	}

	// The first part contains measurement and tags
	measurementAndTags := parts[0]
	tagSet := strings.Split(measurementAndTags, ",")

	deviceID := ""
	// Iterate over tags to find the device ID
	for _, tag := range tagSet[1:] { // Skip the measurement
		tagParts := strings.SplitN(tag, "=", 2)
		if len(tagParts) == 2 {
			if tagParts[0] == "id" {
				deviceID = tagParts[1]
				break // Found the device ID, no need to check other tags
			}
		}
	}

	if deviceID == "" {
		return "", nil, fmt.Errorf("device ID tag 'id' not found in line")
	}

	// The fields are typically the second part of the line protocol
	fieldsStr := parts[1]
	fieldPairs := strings.Split(fieldsStr, ",")

	powerTagData := &PowerTagData{}

	for _, pair := range fieldPairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue // Skip invalid key=value pairs
		}
		key := keyValue[0]
		valueStr := keyValue[1]

		// Attempt to parse the value as a float64 and assign to the struct
		// based on the key.
		switch key {
		case "voltage_p1":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.VoltageP1 = val
			}
		case "current_p1":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.CurrentP1 = val
			}
		case "total_power_active":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.TotalPowerActive = val
			}
		case "power_p1_active":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.PowerP1Active = val
			}
		case "total_power_apparent":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.TotalPowerApparent = val
			}
		case "freq":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.Freq = val
			}
		case "power_factor":
			val, err := parseFloat(valueStr)
			if err == nil {
				powerTagData.PowerFactor = val
			}
			// Add other cases for fields you want to expose via MQTT
		}
	}

	// Basic check if any relevant power data was extracted.
	// This might need adjustment based on your expectations.
	if powerTagData.VoltageP1 == 0 && powerTagData.CurrentP1 == 0 &&
		powerTagData.TotalPowerActive == 0 && powerTagData.PowerP1Active == 0 &&
		powerTagData.TotalPowerApparent == 0 && powerTagData.Freq == 0 &&
		powerTagData.PowerFactor == 0 {
		// Depending on your data, you might want a stricter check.
		// For now, we'll proceed even if no power fields were found in a specific line.
	}

	return deviceID, powerTagData, nil
}

// parseFloat attempts to parse a string as a float64, handling potential type suffixes.
func parseFloat(s string) (float64, error) {
	// Remove potential type suffixes for numeric values
	s = strings.TrimSuffix(s, "i") // Integer suffix
	// You might need to handle other suffixes like 't' (boolean) or '"' (string)
	// if you want to expose those via MQTT, but for power data, float is common.

	return strconv.ParseFloat(s, 64)
}
