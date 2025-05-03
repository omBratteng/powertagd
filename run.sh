#!/usr/bin/env bash

DEVICE="${DEVICE:-/dev/ttyUSB0}"

/powertagd -d "$DEVICE" | /powertagd-bridge \
  --url "$INFLUX_URL" \
  --orgId "$INFLUX_ORG" \
  --bucket "$INFLUX_BUCKET" \
  --token "$INFLUX_TOKEN" \
  --mqtt-broker "$MQTT_BROKER" \
  --mqtt-topic "$MQTT_TOPIC"
