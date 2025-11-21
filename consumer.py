from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json
import time
import random
from collections import deque

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'
DLQ_TOPIC = 'orders-dlq'
GROUP_ID = 'order-consumer-group'
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema_str = f.read()

class PriceAggregator:
    """Real-time aggregation for running average of prices"""
    def __init__(self, window_size=10):
        self.prices = deque(maxlen=window_size)
        self.total_orders = 0
        self.total_revenue = 0.0
    
    def add_price(self, price):
        """Add a new price and update statistics"""
        self.prices.append(price)
        self.total_orders += 1
        self.total_revenue += price
    
    def get_running_average(self):
        """Calculate running average of recent prices"""
        if not self.prices:
            return 0.0
        return sum(self.prices) / len(self.prices)
    
    def get_overall_average(self):
        """Calculate overall average of all processed prices"""
        if self.total_orders == 0:
            return 0.0
        return self.total_revenue / self.total_orders
    
    def get_stats(self):
        """Get current statistics"""
        return {
            'running_average': round(self.get_running_average(), 2),
            'overall_average': round(self.get_overall_average(), 2),
            'total_orders': self.total_orders,
            'total_revenue': round(self.total_revenue, 2),
            'window_size': len(self.prices)
        }

class RetryHandler:
    """Handle retry logic for failed messages"""
    def __init__(self, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_count = {}
    
    def should_retry(self, order_id):
        """Check if message should be retried"""
        count = self.retry_count.get(order_id, 0)
        return count < self.max_retries
    
    def increment_retry(self, order_id):
        """Increment retry count for a message"""
        self.retry_count[order_id] = self.retry_count.get(order_id, 0) + 1
        return self.retry_count[order_id]
    
    def reset_retry(self, order_id):
        """Reset retry count for successfully processed message"""
        if order_id in self.retry_count:
            del self.retry_count[order_id]

def create_consumer():
    """Create and configure Kafka consumer with Avro deserializer"""
    # Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # Avro deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        schema_str,
        lambda data, ctx: data
    )
    
    # Consumer configuration
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])
    
    return consumer, avro_deserializer

def create_dlq_producer():
    """Create producer for Dead Letter Queue"""
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'dlq-producer'
    }
    return Producer(producer_conf)

def send_to_dlq(producer, order, error_msg, retry_count):
    """Send failed message to Dead Letter Queue"""
    dlq_message = {
        'original_order': order,
        'error': error_msg,
        'retry_count': retry_count,
        'timestamp': time.time()
    }
    
    producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(dlq_message).encode('utf-8')
    )
    producer.flush()
    print(f'💀 Sent to DLQ: Order {order["orderId"]} after {retry_count} retries')

def process_order(order, aggregator):
    """
    Process order message with simulated failure scenarios
    Simulates 20% failure rate for demonstration
    """
    order_id = order['orderId']
    
    # Simulate processing failures (20% failure rate)
    if random.random() < 0.2:
        raise Exception(f'Processing failed for order {order_id}')
    
    # Successful processing
    price = order['price']
    aggregator.add_price(price)
    
    print(f'✅ Processed: Order {order_id} | Product: {order["product"]} | Price: ${price:.2f}')
    stats = aggregator.get_stats()
    print(f'   📊 Running Avg: ${stats["running_average"]:.2f} | Overall Avg: ${stats["overall_average"]:.2f} | Total Orders: {stats["total_orders"]}')
    
    return True

def consume_orders():
    """Consume and process order messages with retry logic and DLQ"""
    consumer, avro_deserializer = create_consumer()
    dlq_producer = create_dlq_producer()
    aggregator = PriceAggregator(window_size=10)
    retry_handler = RetryHandler()
    
    print('🎯 Consumer started. Waiting for messages...\n')
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'❌ Consumer error: {msg.error()}')
                    continue
            
            try:
                # Deserialize message
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC, MessageField.VALUE)
                )
                
                order_id = order['orderId']
                
                # Attempt to process order
                try:
                    process_order(order, aggregator)
                    retry_handler.reset_retry(order_id)
                    consumer.commit(msg)
                    
                except Exception as e:
                    print(f'⚠️  Processing error: {e}')
                    
                    # Check if we should retry
                    if retry_handler.should_retry(order_id):
                        retry_count = retry_handler.increment_retry(order_id)
                        print(f'🔄 Retry {retry_count}/{MAX_RETRIES} for order {order_id}')
                        time.sleep(RETRY_DELAY)
                        
                        # Retry processing
                        try:
                            process_order(order, aggregator)
                            retry_handler.reset_retry(order_id)
                            consumer.commit(msg)
                        except Exception as retry_error:
                            print(f'⚠️  Retry failed: {retry_error}')
                            
                            # Send to DLQ if max retries exceeded
                            if not retry_handler.should_retry(order_id):
                                send_to_dlq(dlq_producer, order, str(retry_error), retry_count)
                                consumer.commit(msg)
                    else:
                        # Max retries exceeded, send to DLQ
                        retry_count = retry_handler.retry_count.get(order_id, 0)
                        send_to_dlq(dlq_producer, order, str(e), retry_count)
                        consumer.commit(msg)
                
            except Exception as e:
                print(f'❌ Deserialization error: {e}')
                consumer.commit(msg)
    
    except KeyboardInterrupt:
        print('\n👋 Shutting down consumer...')
    
    finally:
        consumer.close()
        print(f'\n📈 Final Statistics:')
        print(json.dumps(aggregator.get_stats(), indent=2))

if __name__ == '__main__':
    consume_orders()