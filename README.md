# Kafka Order Processing System with Avro

A complete Kafka-based order processing system featuring Avro serialization, real-time aggregation, retry logic, and Dead Letter Queue (DLQ) handling.

## 🎯 Features

- ✅ **Avro Serialization**: Schema-based message serialization using Apache Avro
- ✅ **Real-time Aggregation**: Running average calculation of order prices
- ✅ **Retry Logic**: Automatic retry mechanism for temporary failures (max 3 retries)
- ✅ **Dead Letter Queue**: Failed messages sent to DLQ after max retries exceeded
- ✅ **Schema Registry**: Centralized schema management
- ✅ **Kafka UI**: Web interface for monitoring topics and messages

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/MoraisMNS/kafka_assignment_4077
cd kafka-order-processing
```

### 2. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka Broker (port 9092)
- Schema Registry (port 8081)
- Kafka UI (port 8080)

**Verify services are running:**
```bash
docker-compose ps
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the System

**Terminal 1 - Start Consumer:**
```bash
python consumer.py
```

**Terminal 2 - Start Producer:**
```bash
python producer.py
```

## 📊 Monitoring

Access Kafka UI at: http://localhost:8080

Here you can:
- View topics (`orders` and `orders-dlq`)
- Monitor messages in real-time
- Check consumer groups
- View schemas in the registry

## 🏗️ Architecture

```
Producer → Kafka Topic (orders) → Consumer
                                      ↓
                                  Processing
                                      ↓
                        ┌─────────────┴─────────────┐
                        ↓                           ↓
                   Success                       Failure
                        ↓                           ↓
                  Aggregation                  Retry Logic
                        ↓                           ↓
                  Commit Offset           Max Retries Exceeded?
                                                    ↓
                                          Dead Letter Queue (DLQ)
```

## 📂 Project Structure

```
kafka-order-processing/
├── order.avsc              # Avro schema definition
├── producer.py             # Message producer
├── consumer.py             # Message consumer with retry & DLQ
├── requirements.txt        # Python dependencies
├── docker-compose.yml      # Kafka infrastructure
└── README.md              # This file
```

## 🔧 Configuration

### Producer Configuration
- **Bootstrap Servers**: localhost:9092
- **Topic**: orders
- **Serialization**: Avro with Schema Registry

### Consumer Configuration
- **Bootstrap Servers**: localhost:9092
- **Topic**: orders
- **Group ID**: order-consumer-group
- **Max Retries**: 3
- **Retry Delay**: 2 seconds
- **Window Size**: 10 (for running average)
- **Auto Offset Reset**: earliest

### Avro Schema
```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.orderprocessing",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "price", "type": "float"}
  ]
}
```

## 🎮 How It Works

### Producer
1. Generates random order messages with orderId, product, and price
2. Serializes messages using Avro format
3. Sends to Kafka topic `orders`
4. Reports delivery status

### Consumer
1. Subscribes to `orders` topic
2. Deserializes Avro messages
3. Processes each order:
   - **Success**: Updates running average and commits offset
   - **Failure**: Retries up to 3 times with 2-second delay
   - **Max Retries Exceeded**: Sends to DLQ topic
4. Maintains real-time statistics:
   - Running average (last 10 orders)
   - Overall average
   - Total orders processed
   - Total revenue

### Real-time Aggregation
- Maintains a sliding window of the last 10 prices
- Calculates running average
- Tracks overall average across all processed orders
- Displays statistics after each successful processing

### Retry Logic
- Automatically retries failed messages up to 3 times
- 2-second delay between retries
- Tracks retry count per order ID
- Resets count on successful processing

### Dead Letter Queue
- Failed messages sent to `orders-dlq` topic after max retries
- Includes original order, error message, retry count, and timestamp
- Allows for later analysis and reprocessing

## 📈 Sample Output

**Producer:**
```
🚀 Starting to produce 20 orders...

📦 Producing order: {'orderId': '1000', 'product': 'Lipstick', 'price': 899.99}
✅ Message delivered to orders [0] at offset 0
```

**Consumer:**
```
🎯 Consumer started. Waiting for messages...

✅ Processed: Order 1000 | Product: Lipstick | Price: Rs.899.99
   📊 Running Avg: Rs.899.99 | Overall Avg: Rs.899.99 | Total Orders: 1

⚠️  Processing error: Processing failed for order 1001
🔄 Retry 1/3 for order 1001
✅ Processed: Order 1001 | Product: Foundation | Price: $699.50
   📊 Running Avg: Rs.799.75 | Overall Avg: Rs.799.75 | Total Orders: 2

💀 Sent to DLQ: Order 1002 after 3 retries
```

## 🧪 Testing

### Test Retry Logic
The consumer simulates a 20% failure rate. Run both producer and consumer to see:
- Successful processing
- Automatic retries
- DLQ messages

### View DLQ Messages
```bash
# Using Kafka UI (http://localhost:8080)
# Navigate to: Topics → orders-dlq → Messages
```

### Manual Testing
Modify the failure rate in `consumer.py`:
```python
if random.random() < 0.2:  # Change to 0.5 for 50% failure rate
    raise Exception(f'Processing failed for order {order_id}')
```

## 🛠️ Troubleshooting

### Services not starting
```bash
docker-compose down
docker-compose up -d
```

### Check service logs
```bash
docker-compose logs kafka
docker-compose logs schema-registry
```

### Reset Kafka data
```bash
docker-compose down -v
docker-compose up -d
```

### Python dependency issues
```bash
pip install --upgrade pip
pip install -r requirements.txt --force-reinstall
```

## 🎓 Key Concepts Demonstrated

1. **Avro Serialization**: Efficient binary format with schema evolution
2. **Schema Registry**: Centralized schema management and versioning
3. **Consumer Groups**: Parallel processing with offset management
4. **At-least-once Delivery**: Manual offset commit after successful processing
5. **Error Handling**: Graceful degradation with retry and DLQ patterns
6. **Real-time Processing**: Stream processing with stateful aggregation

## 📝 Assignment Requirements

✅ Kafka-based system with producer and consumer  
✅ Avro serialization with schema  
✅ Real-time aggregation (running average)  
✅ Retry logic for temporary failures  
✅ Dead Letter Queue for permanent failures  
✅ Live demonstration capability  
✅ Git repository with documentation  

## 🔗 Useful Resources

- [Confluent Kafka Python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Apache Avro](https://avro.apache.org/docs/)
- [Kafka Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Kafka UI](https://docs.kafka-ui.provectus.io/)

## 📧 Support

For issues or questions:
1. Check Kafka UI for message flow
2. Review logs: `docker-compose logs`
3. Verify Python dependencies are installed
4. Ensure all ports are available (2181, 8080, 8081, 9092)

## 🎉 Success Criteria

Your system is working correctly when you see:
- ✅ Messages produced successfully
- ✅ Messages consumed and processed
- ✅ Running average calculated and displayed
- ✅ Failed messages retried automatically
- ✅ Permanently failed messages in DLQ
- ✅ All visible in Kafka UI

This was implemented by EG/2020/4077- Morais MNS