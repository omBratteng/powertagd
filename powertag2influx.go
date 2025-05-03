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
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

const ProgName string = "powertag2influx"
const HassDiscoveryTopic string = "homeassistant" // Default Home Assistant discovery topic

// Define a struct to hold the state for a PowerTag device, focusing on key metrics
type PowerTagState struct {
	Voltage float64 `json:"voltage,omitempty"` // Renamed from VoltageP1
	Current float64 `json:"current,omitempty"` // Renamed from CurrentP1
	Power   float64 `json:"power,omitempty"`   // Renamed from TotalPowerActive
	// Filtered out other fields
}

// Define a struct for the Home Assistant MQTT Discovery payload for a sensor
type HassMqttSensorConfig struct {
	Name              string              `json:"name"`
	StateTopic        string              `json:"state_topic"`
	ValueTemplate     string              `json:"value_template"`
	UnitOfMeasurement string              `json:"unit_of_measurement,omitempty"`
	DeviceClass       string              `json:"device_class,omitempty"`
	StateClass        string              `json:"state_class,omitempty"`
	UniqueID          string              `json:"unique_id"`
	Device            *HassMqttDeviceInfo `json:"device,omitempty"`
	// Add other sensor configuration options as needed
}

// Define a struct for the Home Assistant MQTT Discovery Device Info
type HassMqttDeviceInfo struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Model        string   `json:"model,omitempty"`
	Manufacturer string   `json:"manufacturer,omitempty"`
	// sw_version and hw_version can be added if you reliably extract them and want them in HA device info
}

// Map to hold the current state for each device, keyed by device ID
var deviceStates map[string]*PowerTagState
var statesMutex sync.Mutex // Mutex to protect access to deviceStates

// Map to track which devices have had their discovery payloads published
var discoveryPublished map[string]bool
var discoveryMutex sync.Mutex // Mutex to protect access to discoveryPublished

var debugEnabled bool    // Flag to control debug output
var influxdbEnabled bool // Flag to track if InfluxDB is enabled and connected

func main() {
	// Check for DEBUG environment variable
	if os.Getenv("DEBUG") == "true" {
		debugEnabled = true
	}

	var url string
	var token string
	var orgId string
	var bucket string

	var mqttBroker string
	var mqttTopic string          // Base topic for state updates (device ID will be appended)
	var mqttDiscoveryTopic string // Home Assistant discovery topic
	var mqttClientID string
	var mqttUsername string
	var mqttPassword string

	flag.StringVar(&url, "url", "http://localhost:8086", "InfluxDB server URL")
	flag.StringVar(&token, "token", "", "InfluxDB auth token")
	flag.StringVar(&orgId, "orgId", "", "InfluxDB organization ID")
	flag.StringVar(&bucket, "bucket", "", "InfluxDB bucket")

	flag.StringVar(&mqttBroker, "mqtt-broker", "", "MQTT broker URL (e.g., tcp://localhost:1883)")
	flag.StringVar(&mqttTopic, "mqtt-topic", "powertag", "MQTT base topic for state updates (device ID will be appended)")
	flag.StringVar(&mqttDiscoveryTopic, "mqtt-discovery-topic", HassDiscoveryTopic, "Home Assistant MQTT discovery topic")
	flag.StringVar(&mqttClientID, "mqtt-clientid", ProgName, "MQTT client ID")
	flag.StringVar(&mqttUsername, "mqtt-username", "", "MQTT username (optional)")
	flag.StringVar(&mqttPassword, "mqtt-password", "", "MQTT password (optional)")

	flag.Parse()

	// InfluxDB argument validation (still required even if connection fails later)
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
	defer client.Close() // Defer closing regardless of connection success

	health, err := client.Health(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", ProgName, err)
		fmt.Fprintf(os.Stderr, "%s: failed connecting to InfluxDB server. InfluxDB writes will be disabled.\n", ProgName)
		influxdbEnabled = false // Set flag to false
	} else {
		fmt.Printf("%s: connected to InfluxDB at %s (%s %s)\n", ProgName, url, health.Name, *health.Version)
		influxdbEnabled = true // Set flag to true on successful connection
	}

	// Only get WriteAPI and error channel if InfluxDB is enabled
	var writeAPI api.WriteAPI
	if influxdbEnabled {
		writeAPI = client.WriteAPI(orgId, bucket)
		defer writeAPI.Flush()

		// Get errors channel for InfluxDB writes
		errorsCh := writeAPI.Errors()
		// Create go proc for reading and logging InfluxDB errors
		go func() {
			for err := range errorsCh {
				fmt.Fprintf(os.Stderr, "%s: influxdb write error: %s\n", ProgName, err.Error())
			}
		}()
	}

	// --- MQTT Client Setup (if broker is specified) ---
	var mqttClient mqtt.Client
	mqttEnabled := false  // Flag to track if MQTT is enabled
	if mqttBroker != "" { // Only require broker for MQTT
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

		// Optional: Set Last Will and Testament (LWT) - informs broker if client disconnects unexpectedly
		// mqttOpts.SetWill(fmt.Sprintf("%s/status", mqttClientID), "offline", 0, true)

		mqttClient = mqtt.NewClient(mqttOpts)
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			fmt.Fprintf(os.Stderr, "%s: failed connecting to MQTT broker at %s: %v\n", ProgName, mqttBroker, token.Error())
			// We won't exit here, the program can still write to InfluxDB if enabled
		} else if token.Error() == nil {
			fmt.Printf("%s: connected to MQTT broker at %s\n", ProgName, mqttBroker)
			defer mqttClient.Disconnect(250) // Disconnect gracefully on exit
			mqttEnabled = true               // Set flag if connected
		}
	} else {
		fmt.Fprintf(os.Stderr, "%s: MQTT broker not specified, MQTT disabled.\n", ProgName)
	}

	// Initialize the device states map and discovery published map
	deviceStates = make(map[string]*PowerTagState)
	discoveryPublished = make(map[string]bool)

	// --- Read from Stdin and Process ---
	lnscan := bufio.NewScanner(os.Stdin)
	for lnscan.Scan() {
		line := lnscan.Text()

		// Debug print the input line if debug is enabled
		if debugEnabled {
			fmt.Fprintf(os.Stderr, "%s: DEBUG: Received line: %s\n", ProgName, line)
		}

		// Write to InfluxDB only if it's enabled and the WriteAPI is initialized
		if influxdbEnabled && writeAPI != nil {
			writeAPI.WriteRecord(line)
		} else if influxdbEnabled && writeAPI == nil {
			// This case should ideally not happen if influxdbEnabled is true,
			// but as a safety net, we'll log it if WriteAPI is somehow nil.
			fmt.Fprintf(os.Stderr, "%s: warning: InfluxDB enabled but WriteAPI is nil. Skipping write.\n", ProgName)
		}

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
				// If this is a new device, publish discovery messages
				publishDiscoveryMessages(mqttClient, deviceID, mqttDiscoveryTopic, mqttTopic)
			}
			// Apply the parsed fields to the device's state
			updateDeviceState(deviceStates[deviceID], parsedFields)
			statesMutex.Unlock() // Unlock after updating

			// Construct the MQTT state topic using the prefix and device ID
			mqttStateTopic := fmt.Sprintf("%s/%s/state", strings.TrimSuffix(mqttTopic, "/"), deviceID)

			// Marshal the *full* current state into JSON
			statesMutex.Lock() // Lock while accessing the state for marshaling
			jsonData, marshalErr := json.Marshal(deviceStates[deviceID])
			statesMutex.Unlock() // Unlock after marshaling

			if marshalErr != nil {
				fmt.Fprintf(os.Stderr, "%s: failed to marshal device state to json for device %s: %v\n", ProgName, deviceID, marshalErr)
				// Continue processing the next line
				continue
			}

			// Publish the JSON state to MQTT
			token := mqttClient.Publish(mqttStateTopic, 0, false, jsonData) // QoS 0, not retained
			token.Wait()
			if token.Error() != nil {
				fmt.Fprintf(os.Stderr, "%s: mqtt publish state error for device %s: %v\n", ProgName, deviceID, token.Error())
			}
		}
	}

	if err := lnscan.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error reading from stdin: %v\n", ProgName, err)
		os.Exit(1)
	}

	// Ensure all buffered InfluxDB points are flushed before exiting
	// This defer will only be active if influxdbEnabled is true
	// writeAPI.Flush() // Removed explicit flush, defer handles it

	// Explicitly close InfluxDB client (defer also handles this)
	// client.Close() // Removed explicit close

	// Disconnect MQTT client if connected (defer handles this too)
	// if mqttClient != nil && mqttClient.IsConnected() {
	// 	mqttClient.Disconnect(250)
	// }
}

// parseInfluxLineForMQTT attempts to parse an InfluxDB Line Protocol string
// and extract the device ID and a map of the fields.
// It returns the device ID, a map of field key-value pairs for the *relevant* fields, and an error.
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
	for _, tag := range tagSet { // Iterate over all tags including the first one
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

	// Only parse the fields we are interested in for MQTT
	parsedFields := make(map[string]interface{})
	relevantFields := map[string]string{
		"total_power_active": "power",
		"voltage_p1":         "voltage",
		"current_p1":         "current",
	}

	for _, pair := range fieldPairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue // Skip invalid key=value pairs
		}
		influxKey := keyValue[0]
		valueStr := keyValue[1]

		// Check if this is a relevant field
		mqttKey, isRelevant := relevantFields[influxKey]
		if !isRelevant {
			continue // Skip fields we are not interested in for MQTT
		}

		// Attempt to parse different value types for the relevant field
		if strings.Contains(valueStr, ".") || strings.Contains(valueStr, "e") || strings.Contains(valueStr, "E") {
			// Handle floats
			val, err := strconv.ParseFloat(valueStr, 64)
			if err == nil {
				parsedFields[mqttKey] = val // Use the desired MQTT key
			} else {
				fmt.Fprintf(os.Stderr, "%s: warning: failed to parse relevant float value '%s' for key '%s': %v\n", ProgName, valueStr, influxKey, err)
			}
		} else if strings.HasSuffix(valueStr, "i") {
			// Handle integers with 'i' suffix
			val, err := strconv.ParseInt(strings.TrimSuffix(valueStr, "i"), 10, 64)
			if err == nil {
				parsedFields[mqttKey] = float64(val) // Convert integers to float64 for consistency in the struct
			} else {
				fmt.Fprintf(os.Stderr, "%s: warning: failed to parse relevant integer value '%s' for key '%s': %v\n", ProgName, valueStr, influxKey, err)
			}
		} else {
			// Try parsing as integer without suffix
			val, err := strconv.ParseInt(valueStr, 10, 64)
			if err == nil {
				parsedFields[mqttKey] = float64(val) // Convert integers to float64
			} else {
				// This shouldn't happen for the targeted fields if they are always numeric
				fmt.Fprintf(os.Stderr, "%s: warning: failed to parse relevant numeric value '%s' for key '%s': %v\n", ProgName, valueStr, influxKey, err)
			}
		}
	}

	return deviceID, parsedFields, nil
}

// updateDeviceState updates the fields of a PowerTagState struct
// with values from a map of parsed fields (which now only contains relevant fields).
func updateDeviceState(state *PowerTagState, fields map[string]interface{}) {
	for key, value := range fields {
		switch key {
		case "voltage":
			if val, ok := value.(float64); ok {
				state.Voltage = val
			}
		case "current":
			if val, ok := value.(float64); ok {
				state.Current = val
			}
		case "power":
			if val, ok := value.(float64); ok {
				state.Power = val
			}
			// No cases for other fields, as they are filtered out earlier
		}
	}
}

// publishDiscoveryMessages publishes Home Assistant MQTT discovery messages for a device.
func publishDiscoveryMessages(client mqtt.Client, deviceID string, discoveryTopicPrefix string, stateTopicPrefix string) {
	discoveryMutex.Lock()
	if discoveryPublished[deviceID] {
		discoveryMutex.Unlock()
		return // Discovery messages already published for this device
	}
	discoveryMutex.Unlock()

	// Base device information for Home Assistant
	deviceInfo := &HassMqttDeviceInfo{
		Identifiers:  []string{fmt.Sprintf("powertag_%s", deviceID)},
		Name:         fmt.Sprintf("PowerTag %s", deviceID),
		Model:        "PowerTag",           // You might be able to extract a more specific model if available
		Manufacturer: "Schneider Electric", // Assuming Schneider Electric based on PowerTag name
	}

	// Construct the base state topic for this device
	mqttStateTopic := fmt.Sprintf("%s/%s/state", strings.TrimSuffix(stateTopicPrefix, "/"), deviceID)

	// Define the sensors to publish via discovery (only the ones we are interested in)
	sensorsToDiscover := []struct {
		mqttFieldName     string // The key name in the MQTT JSON payload
		name              string
		unitOfMeasurement string
		deviceClass       string
		stateClass        string
		valueTemplate     string
	}{
		{"power", "Power", "W", "power", "measurement", "{{ value_json.power }}"},
		{"voltage", "Voltage", "V", "voltage", "measurement", "{{ value_json.voltage }}"},
		{"current", "Current", "A", "current", "measurement", "{{ value_json.current }}"},
	}

	for _, sensor := range sensorsToDiscover {
		// Construct the unique object ID for the sensor
		objectID := fmt.Sprintf("%s_%s", deviceID, sensor.mqttFieldName) // e.g., 0xe2063d31_power

		// Construct the discovery topic for this specific sensor
		discoveryTopic := fmt.Sprintf("%s/sensor/%s/%s/config", strings.TrimSuffix(discoveryTopicPrefix, "/"), deviceID, objectID)

		// Create the discovery payload
		configPayload := HassMqttSensorConfig{
			Name:              sensor.name,
			StateTopic:        mqttStateTopic,
			ValueTemplate:     sensor.valueTemplate,
			UnitOfMeasurement: sensor.unitOfMeasurement,
			DeviceClass:       sensor.deviceClass,
			StateClass:        sensor.stateClass,
			UniqueID:          objectID, // Must be unique across all sensors in HA
			Device:            deviceInfo,
		}

		// Marshal the configuration payload to JSON
		payloadJSON, marshalErr := json.Marshal(configPayload)
		if marshalErr != nil {
			fmt.Fprintf(os.Stderr, "%s: failed to marshal discovery payload for device %s sensor %s: %v\n", ProgName, deviceID, sensor.name, marshalErr)
			continue // Skip publishing this sensor's discovery message
		}

		// Publish the discovery message with retain flag set
		token := client.Publish(discoveryTopic, 0, true, payloadJSON) // QoS 0, Retained = true
		token.Wait()
		if token.Error() != nil {
			fmt.Fprintf(os.Stderr, "%s: mqtt publish discovery error for device %s sensor %s: %v\n", ProgName, deviceID, sensor.name, token.Error())
		} else {
			// Removed verbose discovery publish log
			// fmt.Printf("%s: published discovery message for device %s sensor %s to topic: %s\n", ProgName, deviceID, sensor.name, discoveryTopic)
		}
	}

	// Mark discovery as published for this device
	discoveryMutex.Lock()
	discoveryPublished[deviceID] = true
	discoveryMutex.Unlock()
}
