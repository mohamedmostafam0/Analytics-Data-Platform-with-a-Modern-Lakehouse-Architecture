#!/bin/bash

# Configuration
CONNECT_HOST="localhost"
CONNECT_PORT="8083"
SCHEMA_REGISTRY_HOST="localhost"
SCHEMA_REGISTRY_PORT="8081"
OPENSEARCH_HOST="localhost"
OPENSEARCH_PORT="9200"

echo "=================================================="
echo "🔍 Starting Pipeline Verification"
echo "=================================================="

# 1. Check Kafka Connect Status
echo -e "\n📡 Checking Kafka Connectors..."
connect_status=$(curl -s "http://$CONNECT_HOST:$CONNECT_PORT/connectors?expand=status")
if [ -z "$connect_status" ]; then
    echo "❌ Kafka Connect is not reachable."
else
    echo "✅ Connectors Found:"
    echo "$connect_status" | python3 -c "import sys, json; 
data = json.load(sys.stdin); 
for name, info in data.items(): 
    state = info['status']['connector']['state']; 
    worker_id = info['status']['connector']['worker_id']; 
    print(f'   - {name}: {state} (Worker: {worker_id})'); 
    for task in info['status']['tasks']: 
        print(f'     - Task {task['id']}: {task['state']}')"
fi

# 2. Check Schema Registry
echo -e "\n📖 Checking Schema Registry Subjects..."
subjects=$(curl -s "http://$SCHEMA_REGISTRY_HOST:$SCHEMA_REGISTRY_PORT/subjects")
if [ -z "$subjects" ]; then
    echo "❌ Schema Registry is not reachable."
else
    echo "✅ Registered Schemas: $subjects"
fi

# 3. Check OpenSearch Indices
echo -e "\n🔎 Checking OpenSearch Indices..."
indices=$(curl -s "http://$OPENSEARCH_HOST:$OPENSEARCH_PORT/_cat/indices?v&h=index,health,docs.count,store.size")
if [ -z "$indices" ]; then
    echo "❌ OpenSearch is not reachable."
else
    echo "✅ OpenSearch Indices:"
    echo "$indices" | grep -E 'index|items|dbz' # Filter for relevant indices
fi

# 4. Check for Data in OpenSearch
echo -e "\n📦 Checking for 'items' in OpenSearch..."
item_count=$(curl -s "http://$OPENSEARCH_HOST:$OPENSEARCH_PORT/items/_count" | python3 -c "import sys, json; print(json.load(sys.stdin).get('count', 'Error'))")
echo "   - Total Items indexed: $item_count"

echo -e "\n=================================================="
echo "✅ Verification Complete"
echo "=================================================="
