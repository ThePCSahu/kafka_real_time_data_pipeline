
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from datetime import datetime
import json
from dotenv import load_dotenv
import os
from mysql_product_reader import MysqlProductReader
from time import sleep

class ProductProducer:
    def __init__(self):
        dotenv_path = 'producer.env'
        load_dotenv(dotenv_path)
        schema_registry_client = SchemaRegistryClient({
            'url': os.getenv('SCHEMA_REGISTRY_URL'),
            'basic.auth.user.info': '{}:{}'.format(os.getenv('SCHEMA_REGISTRY_CLIENT_KEY'), os.getenv('SCHEMA_REGISTRY_CLIENT_SECRET'))
            })
        
        schema_name = os.getenv('SCHEMA_NAME')
        schema_str = schema_registry_client.get_latest_version(schema_name).schema.schema_str

        key_serializer = StringSerializer()
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        self.kafka_producer = SerializingProducer({
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
            'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
            'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISMS'),
            'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
            'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
            'key.serializer': key_serializer,
            'value.serializer': avro_serializer
            })
        
        self.product_reader = MysqlProductReader(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
            )
        
        self.product_reader.connect()
    
    # Handles success / failure event
    def delivery_report(self,err,msg):
        if err is not None:
            print("Publish failed for Product {}: {}".format(msg.key(), err))
            return
        print("Product {} successfully published to {} [{}] at offset {}".format(msg.key(), msg.topic(),msg.partition(), msg.offset()))

    #serializes to avro and publishes to kafka topic
    def serialize_and_publish_to_kafka(self,product):
        topic_name = os.getenv('KAKFA_TOPIC_NAME')
        self.kafka_producer.produce(topic=topic_name,key=str(product['id']),value=product, on_delivery=self.delivery_report)
        self.kafka_producer.flush()

    # Fetches incremental data from mysql and publishes to kafka
    def run_producer(self):
        # Load the last read timestamp from the config file
        config_data = {}
        last_read_timestamp = "1970-01-01 00:00:00"
        try:
            with open('last_read_timestamp.json') as file:
                config_data = json.load(file)
                last_read_timestamp = config_data.get('last_read_timestamp')
        except FileNotFoundError:
            pass

        try:
            while True:
                max_date_str = self.product_reader.fetch_max_last_updated().strftime("%Y-%m-%d %H:%M:%S")
                products = self.product_reader.fetch_products_by_last_read_timestamp(last_read_timestamp)
                last_read_timestamp = max_date_str
                # Update the value in the config.json file
                config_data['last_read_timestamp'] = last_read_timestamp

                with open('last_read_timestamp.json', 'w') as file:
                    json.dump(config_data, file)
                for product in products:
                    self.serialize_and_publish_to_kafka(product)
                sleep(5)
        except Exception as e:
            print(f"An exception occured: {e}")

        
if __name__ == "__main__":
    ProductProducer().run_producer()
