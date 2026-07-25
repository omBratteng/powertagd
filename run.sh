#!/usr/bin/env bash

DEVICE="${DEVICE:-/dev/ttyUSB0}"

/powertagd -d "$DEVICE" | /powertagd-bridge \
	--mqtt-broker "$MQTT_BROKER" \
	--mqtt-topic "$MQTT_TOPIC"
