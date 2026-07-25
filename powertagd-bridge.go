package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	ProgName           string = "powertagd-bridge"
	HassDiscoveryTopic string = "homeassistant" // Default Home Assistant discovery topic
)

// Define a struct to hold the state for a PowerTag device, focusing on key metrics
type PowerTagState struct {
	Voltage  float64   `json:"voltage,omitempty"` // Renamed from VoltageP1
	Current  float64   `json:"current,omitempty"` // Renamed from CurrentP1
	Power    float64   `json:"power,omitempty"`   // Renamed from TotalPowerActive
	LastSeen time.Time `json:"last_seen"`         // Added LastSeen timestamp
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
	EnabledByDefault  bool                `json:"enabled_by_default"`
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
var (
	deviceStates map[string]*PowerTagState
	statesMutex  sync.Mutex // Mutex to protect access to deviceStates
)

// Map to track which devices have had their discovery payloads published
var (
	discoveryPublished map[string]bool
	discoveryMutex     sync.Mutex // Mutex to protect access to discoveryPublished
)

var debugEnabled bool // Flag to control debug output

func main() {
	// Check for DEBUG environment variable
	if os.Getenv("DEBUG") == "true" {
		debugEnabled = true
	}

	var mqttBroker string
	var mqttTopic string          // Base topic for state updates (device ID will be appended)
	var mqttDiscoveryTopic string // Home Assistant discovery topic
	var mqttClientID string
	var mqttUsername string
	var mqttPassword string

	flag.StringVar(&mqttBroker, "mqtt-broker", "", "MQTT broker URL (e.g., tcp://localhost:1883)")
	flag.StringVar(&mqttTopic, "mqtt-topic", "powertag", "MQTT base topic for state updates (device ID will be appended)")
	flag.StringVar(&mqttDiscoveryTopic, "mqtt-discovery-topic", HassDiscoveryTopic, "Home Assistant MQTT discovery topic")
	flag.StringVar(&mqttClientID, "mqtt-clientid", ProgName, "MQTT client ID")
	flag.StringVar(&mqttUsername, "mqtt-username", "", "MQTT username (optional)")
	flag.StringVar(&mqttPassword, "mqtt-password", "", "MQTT password (optional)")

	flag.Parse()

	// Standard input check
	stat, _ := os.Stdin.Stat()
	if stat.Mode()&os.ModeCharDevice != 0 {
		fmt.Fprintf(os.Stderr, "%s: no data on stdin\n", ProgName)
		fmt.Fprintf(os.Stderr, "%s expects data to be piped to stdin, i.e.:\n", ProgName)
		fmt.Fprintf(os.Stderr, "    powertagd | %s\n", ProgName)
		os.Exit(2)
	}

	// MQTT is the only output, so a broker is required.
	if mqttBroker == "" {
		fmt.Fprintf(os.Stderr, "%s: -mqtt-broker is required (e.g. tcp://localhost:1883)\n", ProgName)
		os.Exit(2)
	}

	// Initialize the device states map and discovery published map
	deviceStates = make(map[string]*PowerTagState)
	discoveryPublished = make(map[string]bool)

	// --- MQTT Client Setup ---
	statusTopic := fmt.Sprintf("%s/status", strings.TrimSuffix(mqttTopic, "/"))

	mqttOpts := mqtt.NewClientOptions().AddBroker(mqttBroker).SetClientID(mqttClientID)
	if mqttUsername != "" {
		mqttOpts.SetUsername(mqttUsername)
	}
	if mqttPassword != "" {
		mqttOpts.SetPassword(mqttPassword)
	}

	// Connection tuning for robust automatic reconnects.
	mqttOpts.SetKeepAlive(60 * time.Second)
	mqttOpts.SetPingTimeout(5 * time.Second)
	mqttOpts.SetConnectTimeout(10 * time.Second)
	mqttOpts.SetAutoReconnect(true)
	mqttOpts.SetConnectRetryInterval(2 * time.Second)
	mqttOpts.SetMaxReconnectInterval(30 * time.Second)
	// Retry the initial connect too, so startup succeeds even if the broker
	// is not yet reachable.
	mqttOpts.SetConnectRetry(true)
	// Resume the same session across reconnects so subscriptions/state persist.
	mqttOpts.SetCleanSession(false)

	// Last Will and Testament: broker publishes "offline" if we drop unexpectedly.
	mqttOpts.SetWill(statusTopic, "offline", 1, true)

	// OnConnect fires on both the first connect and every successful reconnect.
	mqttOpts.SetOnConnectHandler(func(c mqtt.Client) {
		fmt.Printf("%s: connected to MQTT broker at %s\n", ProgName, mqttBroker)

		// Announce availability (retained).
		if token := c.Publish(statusTopic, 1, true, "online"); token.WaitTimeout(5*time.Second) && token.Error() != nil {
			fmt.Fprintf(os.Stderr, "%s: mqtt publish status error: %v\n", ProgName, token.Error())
		}

		// Re-publish Home Assistant discovery after a (re)connect. The broker
		// may have restarted and lost retained discovery messages, so reset the
		// tracking map and re-announce every device we already know about.
		discoveryMutex.Lock()
		discoveryPublished = make(map[string]bool)
		discoveryMutex.Unlock()

		statesMutex.Lock()
		knownDevices := make([]string, 0, len(deviceStates))
		for id := range deviceStates {
			knownDevices = append(knownDevices, id)
		}
		statesMutex.Unlock()

		for _, id := range knownDevices {
			publishDiscoveryMessages(c, id, mqttDiscoveryTopic, mqttTopic)
		}
	})

	mqttOpts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		fmt.Fprintf(os.Stderr, "%s: mqtt connection lost: %v (auto-reconnecting)\n", ProgName, err)
	})

	mqttOpts.SetReconnectingHandler(func(c mqtt.Client, o *mqtt.ClientOptions) {
		fmt.Fprintf(os.Stderr, "%s: attempting to reconnect to MQTT broker at %s...\n", ProgName, mqttBroker)
	})

	mqttClient := mqtt.NewClient(mqttOpts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		// With SetConnectRetry(true) the client keeps retrying in the
		// background, so we only warn here rather than exit.
		fmt.Fprintf(os.Stderr, "%s: initial MQTT connect to %s failed: %v (will keep retrying)\n", ProgName, mqttBroker, token.Error())
	}
	defer func() {
		// Mark offline and disconnect gracefully on exit.
		if mqttClient.IsConnected() {
			t := mqttClient.Publish(statusTopic, 1, true, "offline")
			t.WaitTimeout(2 * time.Second)
		}
		mqttClient.Disconnect(250)
	}()

	// --- Read from Stdin and Process ---
	lnscan := bufio.NewScanner(os.Stdin)
	for lnscan.Scan() {
		line := lnscan.Text()

		// Debug print the input line if debug is enabled
		if debugEnabled {
			fmt.Fprintf(os.Stderr, "%s: DEBUG: Received line: %s\n", ProgName, line)
		}

		// Only process/publish while connected. Auto-reconnect handles
		// transient outages; QoS 0 state messages during a disconnect would
		// be dropped anyway, so skip them.
		if mqttClient.IsConnected() {
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
			// Apply the parsed fields to the device's state and update LastSeen
			updateDeviceState(deviceStates[deviceID], parsedFields)
			deviceStates[deviceID].LastSeen = time.Now()
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
}

// parseInfluxLineForMQTT attempts to parse an InfluxDB Line Protocol string
// and extract the device ID and a map of the fields.
// It returns the device ID, a map of field key-value pairs for the *relevant* fields, and an error.
func parseInfluxLineForMQTT(line string) (string, map[string]any, error) {
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
	parsedFields := make(map[string]any)
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
		} else if before, ok := strings.CutSuffix(valueStr, "i"); ok {
			// Handle integers with 'i' suffix
			val, err := strconv.ParseInt(before, 10, 64)
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
func updateDeviceState(state *PowerTagState, fields map[string]any) {
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
		enabledByDefault  bool
	}{
		{"power", "Power", "W", "power", "measurement", "{{ value_json.power }}", true},
		{"voltage", "Voltage", "V", "voltage", "measurement", "{{ value_json.voltage }}", true},
		{"current", "Current", "A", "current", "measurement", "{{ value_json.current }}", true},
		{"last_seen", "Last Seen", "", "timestamp", "", "{{ value_json.last_seen }}", false},
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
			EnabledByDefault:  sensor.enabledByDefault,
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
