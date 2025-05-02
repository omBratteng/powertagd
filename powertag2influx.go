package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync" // Added for concurrency safety (if needed, though the current loop is sequential)
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

const ProgName string = "powertag2influx"

// Define a struct to hold the full state for a PowerTag device
type PowerTagState struct {
	VoltageP1          float64 `json:"voltage_p1,omitempty"`
	CurrentP1          float64 `json:"current_p1,omitempty"`
	TotalPowerActive   float64 `json:"total_power_active,omitempty"`
	PowerP1Active      float64 `json:"power_p1_active,omitempty"`
	TotalPowerApparent float64 `json:"total_power_apparent,omitempty"`
	Freq               float64 `json:"freq,omitempty"`
	PowerFactor        float64 `json:"power_factor,omitempty"`
	Serial             string  `json:"serial,omitempty"` // Added serial
	FwVer              string  `json:"fw_ver,omitempty"` // Added firmware version
	HwVer              string  `json:"hw_ver,omitempty"` // Added hardware version
	// Add other relevant fields you want to track and send
}

// Map to hold the current state for each device, keyed by device ID
var deviceStates map[string]*PowerTagState
var statesMutex sync.Mutex // Mutex to protect access to deviceStates (good practice)

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

		// Set a reasonable reconnect interval
		mqttOpts.SetKeepAlive(60 * time.Second)
		mqttOpts.SetPingTimeout(1 * time.Second)
		mqttOpts.SetConnectRetryInterval(2 * time.Second)
		mqttOpts.SetAutoReconnect(true)

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

	// Initialize the device states map
	deviceStates = make(map[string]*PowerTagState)

	// --- Read from Stdin and Process ---
	lnscan := bufio.NewScanner(os.Stdin)
	for lnscan.Scan() {
		line := lnscan.Text()

		// Write to InfluxDB
		writeAPI.WriteRecord(line)

		// Process for MQTT if enabled and client is connected
		if mqttEnabled && mqttClient.IsConnected() {
			// Attempt to parse the InfluxDB line protocol for MQTT
			deviceID, parsedFields, parseErr := parseInfluxLineForMQTT(line)
			if parseErr != nil {
				fmt.Fprintf(os.Stderr, "%s: failed to parse influxdb line for mqtt: %v (line: %s)\n", ProgName, parseErr, line)
				// Continue processing the next line
				continue
			}

			// Update the in-memory state for the device
			statesMutex.Lock() // Lock to protect concurrent access
			if _, ok := deviceStates[deviceID]; !ok {
				// If the device is not in the map, initialize its state
				deviceStates[deviceID] = &PowerTagState{}
			}
			// Apply the parsed fields to the device's state
			updateDeviceState(deviceStates[deviceID], parsedFields)
			statesMutex.Unlock() // Unlock after updating

			// Construct the MQTT topic using the prefix and device ID
			mqttTopic := fmt.Sprintf("%s/%s/state", strings.TrimSuffix(mqttTopicPrefix, "/"), deviceID)

			// Marshal the *full* current state into JSON
			statesMutex.Lock() // Lock while accessing the state for marshaling
			jsonData, marshalErr := json.Marshal(deviceStates[deviceID])
			statesMutex.Unlock() // Unlock after marshaling

			if marshalErr != nil {
				fmt.Fprintf(os.Stderr, "%s: failed to marshal device state to json for device %s: %v\n", ProgName, deviceID, marshalErr)
				// Continue processing the next line
				continue
			}

			// Publish the JSON to MQTT
			token := mqttClient.Publish(mqttTopic, 0, false, jsonData) // QoS 0, not retained
			token.Wait()
			if token.Error() != nil {
				fmt.Fprintf(os.Stderr, "%s: mqtt publish error for device %s: %v\n", ProgName, deviceID, token.Error())
			}
		}
	}

	if err := lnscan.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error reading from stdin: %v\n", ProgName, err)
		os.Exit(1)
	}

	// Ensure all buffered InfluxDB points are flushed before exiting
	writeAPI.Flush()
	client.Close() // Explicitly close client for InfluxDB

	// Disconnect MQTT client if connected (defer handles this too, but explicit is fine)
	if mqttClient != nil && mqttClient.IsConnected() {
		mqttClient.Disconnect(250)
	}
}

// parseInfluxLineForMQTT attempts to parse an InfluxDB Line Protocol string
// and extract the device ID and a map of the fields.
// It returns the device ID, a map of field key-value pairs, and an error.
func parseInfluxLineForMQTT(line string) (string, map[string]interface{}, error) {
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

	parsedFields := make(map[string]interface{})

	for _, pair := range fieldPairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue // Skip invalid key=value pairs
		}
		key := keyValue[0]
		valueStr := keyValue[1]

		// Attempt to parse different value types
		if strings.HasPrefix(valueStr, "\"") && strings.HasSuffix(valueStr, "\"") {
			// Handle strings
			parsedFields[key] = strings.Trim(valueStr, "\"")
		} else if strings.EqualFold(valueStr, "true") || strings.EqualFold(valueStr, "false") {
			// Handle booleans
			val, err := strconv.ParseBool(valueStr)
			if err == nil {
				parsedFields[key] = val
			}
		} else if strings.Contains(valueStr, ".") || strings.Contains(valueStr, "e") || strings.Contains(valueStr, "E") {
			// Handle floats (contains decimal or exponential)
			val, err := strconv.ParseFloat(valueStr, 64)
			if err == nil {
				parsedFields[key] = val
			}
		} else if strings.HasSuffix(valueStr, "i") {
			// Handle integers with 'i' suffix
			val, err := strconv.ParseInt(strings.TrimSuffix(valueStr, "i"), 10, 64)
			if err == nil {
				parsedFields[key] = val
			}
		} else {
			// Try parsing as integer without suffix
			val, err := strconv.ParseInt(valueStr, 10, 64)
			if err == nil {
				parsedFields[key] = val
			} else {
				// If all else fails, treat as string (or handle other types if needed)
				parsedFields[key] = valueStr
				fmt.Fprintf(os.Stderr, "%s: warning: could not parse value '%s' for key '%s' as known type, treating as string\n", ProgName, valueStr, key)
			}
		}
	}

	return deviceID, parsedFields, nil
}

// updateDeviceState updates the fields of a PowerTagState struct
// with values from a map of parsed fields.
func updateDeviceState(state *PowerTagState, fields map[string]interface{}) {
	for key, value := range fields {
		switch key {
		case "voltage_p1":
			if val, ok := value.(float64); ok {
				state.VoltageP1 = val
			}
		case "current_p1":
			if val, ok := value.(float64); ok {
				state.CurrentP1 = val
			}
		case "total_power_active":
			if val, ok := value.(float64); ok {
				state.TotalPowerActive = val
			} else if val, ok := value.(int64); ok { // Handle potential integer parsing
				state.TotalPowerActive = float64(val)
			}
		case "power_p1_active":
			if val, ok := value.(float64); ok {
				state.PowerP1Active = val
			} else if val, ok := value.(int64); ok { // Handle potential integer parsing
				state.PowerP1Active = float64(val)
			}
		case "total_power_apparent":
			if val, ok := value.(float64); ok {
				state.TotalPowerApparent = val
			} else if val, ok := value.(int64); ok { // Handle potential integer parsing
				state.TotalPowerApparent = float64(val)
			}
		case "freq":
			if val, ok := value.(float64); ok {
				state.Freq = val
			}
		case "power_factor":
			if val, ok := value.(float64); ok {
				state.PowerFactor = val
			} else if val, ok := value.(int64); ok { // Handle potential integer parsing
				state.PowerFactor = float64(val)
			}
		case "serial":
			if val, ok := value.(string); ok {
				state.Serial = val
			}
		case "fw_ver":
			if val, ok := value.(string); ok {
				state.FwVer = val
			}
		case "hw_ver":
			if val, ok := value.(string); ok {
				state.HwVer = val
			}
			// Add cases for other fields you added to PowerTagState
		}
	}
}
