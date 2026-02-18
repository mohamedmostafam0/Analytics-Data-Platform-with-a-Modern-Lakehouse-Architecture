# Dockerfile
FROM quay.io/debezium/connect:3.1.1.Final

USER root
RUN microdnf -y install unzip tar gzip && microdnf clean all

# Install Confluent Hub Client
RUN curl -L -o /tmp/confluent-hub-client-latest.tar.gz http://client.hub.confluent.io/confluent-hub-client-latest.tar.gz && \
    mkdir -p /usr/local/confluent-hub && \
    tar -xzf /tmp/confluent-hub-client-latest.tar.gz -C /usr/local/confluent-hub && \
    rm /tmp/confluent-hub-client-latest.tar.gz

ENV PATH=$PATH:/usr/local/confluent-hub/bin

USER kafka

# Install Avro Converter
RUN touch /tmp/worker.properties && \
    confluent-hub install confluentinc/kafka-connect-avro-converter:7.6.0 \
    --component-dir /kafka/connect \
    --worker-configs /tmp/worker.properties \
    --no-prompt && \
    rm /tmp/worker.properties

# Download OpenSearch sink connector (keeping previous functionality)
RUN curl -L https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.1.1/opensearch-connector-for-apache-kafka-3.1.1.zip -o /tmp/opensearch-connector-for-kafka.zip && \
    unzip /tmp/opensearch-connector-for-kafka.zip -d /kafka/connect && \
    rm -f /tmp/opensearch-connector-for-kafka.zip