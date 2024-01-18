FROM confluentinc/cp-server-connect-operator:6.1.6.0 AS base

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage:latest \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage-source:1.4.5

USER user