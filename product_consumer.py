
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from time import sleep
from dotenv import load_dotenv
import os
import sys
from csv_file_writer import CsvFileWriter

# Consumer class for consuming from kafka topic and saving to json file
class ProductConsumer:
    def __init__(self, consumer_name):
        self.consumer_name = consumer_name
        dotenv_path = 'consumer.env'
        load_dotenv(dotenv_path)
        schema_registry_client = SchemaRegistryClient({
            'url': os.getenv('SCHEMA_REGISTRY_URL'),
            'basic.auth.user.info': '{}:{}'.format(os.getenv('SCHEMA_REGISTRY_CLIENT_KEY'), os.getenv('SCHEMA_REGISTRY_CLIENT_SECRET'))
            })
        
        schema_name = os.getenv('SCHEMA_NAME')
        schema_str = schema_registry_client.get_latest_version(schema_name).schema.schema_str

        key_deserializer = StringDeserializer()
        avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
        self.kafka_consumer = DeserializingConsumer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
            'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
            'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
            'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
            'key.deserializer': key_deserializer,
            'value.deserializer': avro_deserializer,
            'group.id': os.getenv('KAFKA_CONSUMER_GROUP_ID'),
            'client.id': self.consumer_name,
            'auto.offset.reset': os.getenv('KAFKA_AUTO_OFFSET_RESET'),
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
            })
        self.kafka_consumer.subscribe([os.getenv('KAKFA_TOPIC_NAME')])
        file_name  = self.consumer_name + ".json"
        self.csv_file_writer = CsvFileWriter(file_name)

    # Continuously read messages from kafka and process them
    def run_consumer(self):
        try:
            while True:
                msg = self.kafka_consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error: {}".format(msg.error()))
                    continue
                product = msg.value()
                print(product)
                category = product['category']
                if type(category) == str:
                    product['category'] = category.upper()
                if(category=='ELECTRONICS'):
                    product['price'] = round(product['price'] * 0.5, 2)
                else:
                    product['price'] = round(product['price'] * 0.9, 2)
                self.csv_file_writer.write(product)
                print('Successfully consumed product with key {} and value {} and partition {} at offset {}'.format(msg.key(), msg.value(), msg.partition(), msg.offset()))
        except Exception as e:
            print('An exception occured while consuming product with key {} and value {} and partition {} at offset {} : {}'.format(msg.key(), msg.value(), msg.partition(), msg.offset(), e))
        finally:
            self.kafka_consumer.close()

if __name__ == "__main__":
    consumer_name = "consumer1"
    if len(sys.argv) > 1:
        consumer_name= sys.argv[1]
    ProductConsumer(consumer_name).run_consumer()
