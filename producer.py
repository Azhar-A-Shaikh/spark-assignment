import argparse
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import datetime,time
from typing import List
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName("kafka_producer").getOrCreate()

case_df = spark.read.csv(r"C:\Users\Azhar\Desktop\Spark Assignment\Case.csv", header= True, inferSchema=True)

columns = ['case_id','province','city','group','infection_case',"confirmed","latitude","longitude"]

API_KEY = 'B4TJWHICY3TZYC7S'
API_SECRET_KEY = 'OfNJpvMoIuOYXZE2IxeXvBOdyqriLFSaX8LzrmndaMvBqxrW5K2sP/Rxovgtfb2z'
BOOTSTRAP_SERVER = 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
ENDPOINT_SCHEMA_URL  = 'https://psrc-6y63j.ap-southeast-2.aws.confluent.cloud'
SCHEMA_REGISTRY_API_KEY = 'HWNOSFSAYR7GHHYK'
SCHEMA_REGISTRY_API_SECRET = 'r6cBfDD2BzY/k53/x07DZgxIFWxgOk3rhfKx2p2LCQSstTFzo0/vYecBA062G9d6'

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"
    }

class Car:   
    def __init__(self, record: dict):
        self.case_id = record["case_id"]
        self.province = record["province"]
        self.city = record["city"]
        self.group = record["group"]
        self.infection_case = record["infection_case"]
        self.confirmed = record["confirmed"]
        self.latitude = record["latitude"]
        self.longitude = record["longitude"]

    @staticmethod
    def dict_to_car(data: dict, ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"

def car_to_dict(car: Car, ctx):
    return {
        "case_id": str(car.case_id),
        "province": car.province,
        "city": car.city,
        "group": str(car.group),
        "infection_case": car.infection_case,
        "confirmed": car.confirmed,
        "latitude": float(car.latitude),
        "longitude": float(car.longitude)
    }


def get_car_instance(case_df):
    cars: List[Car] = []
    for row in case_df.collect():
        data = row.asDict()
        car = Car(data)
        cars.append(car)
        yield car

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    subject = topic + '-value'

    schema = schema_registry_client.get_latest_version(subject)
    schema_str = schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)

    producer = Producer(sasl_conf())

    print("Producing records to topic {}. ^C to exit.".format(topic))

    producer.poll(0.0)
    try:
        for car in get_car_instance(case_df):
            producer.produce(topic=topic,
                             key=string_serializer(str(uuid4())),
                             value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()


main("case_data")