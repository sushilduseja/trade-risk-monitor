# FX Risk Monitoring System

A real-time foreign exchange (FX) trade risk monitoring system built with Kafka/Redpanda and Java 21.

## Overview

This application monitors foreign exchange trades, calculates risk scores, and generates alerts for high-risk trades. It consists of:

- FX trade producers that publish trade events to Kafka
- Risk assessment consumers that evaluate trades and generate alerts
- Notification consumers that process alerts and send notifications

## Features

- Real-time trade risk assessment
- Event-driven architecture using Kafka/Redpanda
- Resilient consumers with proper error handling
- Dead Letter Queue (DLQ) for error management
- Utilizes Java 21 features like virtual threads

## Requirements

- Java 21 or higher
- Apache Kafka or Redpanda
- Maven

## Configuration

The application can be configured through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| KAFKA_BOOTSTRAP_SERVERS | Kafka/Redpanda connection string | localhost:9092 |
| KAFKA_USE_SASL | Whether to use SASL authentication | false |
| KAFKA_SASL_USERNAME | SASL username | |
| KAFKA_SASL_PASSWORD | SASL password | |
| KAFKA_MAX_POLL_INTERVAL_MS | Maximum time between poll calls | 300000 |
| KAFKA_SESSION_TIMEOUT_MS | Session timeout | 10000 |
| KAFKA_ACKS_CONFIG | Producer acknowledgment level | all |
| KAFKA_RETRIES_CONFIG | Producer retry count | 3 |
| KAFKA_LINGER_MS_CONFIG | Producer linger time | 1 |

## Building

To build the project:

```sh
mvn clean package
```

## Running

To run the application:

```sh
java -jar target/fx-risk-monitoring-1.0.0.jar
```

To run in demo mode (publishes sample trades):

```sh
java -jar target/fx-risk-monitoring-1.0.0.jar demo
```
