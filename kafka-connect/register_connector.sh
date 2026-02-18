#!/bin/bash

# Configuration
CONNECT_HOST="localhost"
CONNECT_PORT="8083"
CONFIG_DIR="/kafka-connect"
MAX_RETRIES=30
RETRY_INTERVAL=2

# Check for required tools (envsubst)
if ! command -v envsubst &> /dev/null; then
    echo "Warning: envsubst not found. Installing via gettext..."
    # Attempt to install if running as root/inside alpine container
    if [ -f /etc/alpine-release ]; then
        apk add gettext
    fi
fi

# Function to check if Connect is ready
check_connect_ready() {
    curl -s -f "http://$CONNECT_HOST:$CONNECT_PORT/" > /dev/null
    return $?
}

# Wait for Connect to be ready
echo "Waiting for Kafka Connect to be ready at $CONNECT_HOST:$CONNECT_PORT..."
count=0
while ! check_connect_ready; do
    echo "Connect not ready yet. Retrying in $RETRY_INTERVAL seconds..."
    sleep $RETRY_INTERVAL
    count=$((count+1))
    if [ $count -ge $MAX_RETRIES ]; then
        echo "Error: Timeout waiting for Kafka Connect."
        exit 1
    fi
done

echo "Kafka Connect is ready."

# Iterate over all JSON config files
for CONFIG_FILE in $CONFIG_DIR/*.json; do
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "No connector configuration files found in $CONFIG_DIR."
        exit 0
    fi
    
    # Use envsubst to replace variables in JSON (creates a temporary file)
    echo "Processing $CONFIG_FILE with env substitution..."
    TEMP_CONFIG_FILE="/tmp/$(basename "$CONFIG_FILE")"
    envsubst < "$CONFIG_FILE" > "$TEMP_CONFIG_FILE"

    # Extract connector name from JSON (from the processed file)
    CONNECTOR_NAME=$(grep -o '"name": *"[^"]*"' "$TEMP_CONFIG_FILE" | cut -d'"' -f4)
    
    if [ -z "$CONNECTOR_NAME" ]; then
        echo "Error: Could not extract connector name from $CONFIG_FILE"
        rm -f "$TEMP_CONFIG_FILE"
        continue
    fi

    # Register or update the connector
    echo "Registering connector $CONNECTOR_NAME from $CONFIG_FILE..."

    response=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
      -H "Content-Type: application/json" \
      -d @"$TEMP_CONFIG_FILE" \
      "http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME/config")

    if [[ "$response" -ge 200 && "$response" -lt 300 ]]; then
        echo "Connector $CONNECTOR_NAME registered successfully (HTTP $response)."
    else
        echo "Error registering connector $CONNECTOR_NAME. HTTP Status: $response"
        echo "Response body:"
        curl -s -X PUT \
          -H "Content-Type: application/json" \
          -d @"$TEMP_CONFIG_FILE" \
          "http://$CONNECT_HOST:$CONNECT_PORT/connectors/$CONNECTOR_NAME/config"
    fi
    
    # Cleanup temp file
    rm -f "$TEMP_CONFIG_FILE"
done

# Verify status of all connectors
echo "Checking status of all connectors..."
curl -s "http://$CONNECT_HOST:$CONNECT_PORT/connectors?expand=status" | python3 -m json.tool
