from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import random
import time
import json

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'orders'

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema_str = f.read()

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_producer():
    """Create and configure Kafka producer with Avro serializer"""
    # Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # Avro serializer
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda order, ctx: order
    )
    
    # Producer configuration
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'order-producer'
    }
    
    return Producer(producer_conf), avro_serializer

def generate_order(order_num):
    """Generate a random order message"""
    products = ['Lipstick', 'Foundation', 'Eyeliner', 'Mascara', 'Face Cream', 'Perfume']
    return {
        'orderId': f'{1000 + order_num}',
        'product': random.choice(products),
        'price': round(random.uniform(10.0, 1000.0), 2)
    }

def produce_orders(num_orders=10, delay=1):
    """Produce order messages to Kafka"""
    producer, avro_serializer = create_producer()
    
    print(f'🚀 Starting to produce {num_orders} orders...\n')
    
    for i in range(num_orders):
        try:
            order = generate_order(i)
            print(f'📦 Producing order: {order}')
            
            # Serialize and send
            producer.produce(
                topic=TOPIC,
                value=avro_serializer(
                    order,
                    SerializationContext(TOPIC, MessageField.VALUE)
                ),
                on_delivery=delivery_report
            )
            
            # Trigger callbacks
            producer.poll(0)
            time.sleep(delay)
            
        except Exception as e:
            print(f'❌ Error producing order: {e}')
    
    # Wait for all messages to be delivered
    producer.flush()
    print('\n✨ All orders produced successfully!')

if __name__ == '__main__':
    produce_orders(num_orders=20, delay=0.5)