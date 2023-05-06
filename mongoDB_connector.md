Create a Kafka topic in Confluent Kafka Cloud to store the data you want to send to MongoDB Atlas Cloud.
Set up a Kafka connector that can read data from the Kafka topic and write it to MongoDB Atlas Cloud.
Configure the Kafka connector with the appropriate settings and credentials to connect to both Confluent Kafka Cloud and MongoDB Atlas Cloud.
Start the Kafka connector to begin streaming data from the Kafka topic to MongoDB Atlas Cloud.
Here are more detailed steps to follow:

Create a Kafka topic in Confluent Kafka Cloud:
Log in to your Confluent Kafka Cloud account and select your cluster.
Go to the Topics tab and click Create topic.
Specify the topic name and any other desired settings, such as the number of partitions.
Click Create topic to create the topic.
Set up a Kafka connector:
Choose a Kafka connector that can read from Kafka topics and write to MongoDB Atlas Cloud. For example, you can use the MongoDB Connector for Apache Kafka.
Download and install the Kafka connector on your local machine or a server that has access to Confluent Kafka Cloud and MongoDB Atlas Cloud.
Configure the Kafka connector with the appropriate settings and credentials to connect to both Confluent Kafka Cloud and MongoDB Atlas Cloud. This typically involves creating a configuration file for the Kafka connector that specifies the Kafka topic to read from, the MongoDB Atlas cluster and database to write to, and any authentication credentials required.
Refer to the Kafka connector's documentation for detailed instructions on how to set up and configure the connector.
Start the Kafka connector:
Once the Kafka connector is set up and configured, start it to begin streaming data from the Kafka topic to MongoDB Atlas Cloud.
This can typically be done by running a command that starts the Kafka connector and points it to the configuration file you created earlier.
Refer to the Kafka connector's documentation for detailed instructions on how to start the connector.
That's it! Once the Kafka connector is running, it should begin streaming data from the Kafka topic to MongoDB Atlas Cloud. You can monitor the progress of the Kafka connector and view any errors or warnings that may occur in the Kafka connector's logs.

import pymongo
import pandas as pd

# Define MongoDB Atlas connection string and database/collection names
mongo_uri = "mongodb+srv://<username>:<password>@<cluster-name>.mongodb.net/<dbname>?retryWrites=true&w=majority"
mongo_dbname = "<dbname>"
mongo_collection = "<collection-name>"

# Connect to MongoDB Atlas using PyMongo
client = pymongo.MongoClient(mongo_uri)
db = client[mongo_dbname]
collection = db[mongo_collection]

# Convert pandas DataFrame to a list of dictionaries for bulk insert
data = result_df.to_dict(orient='records')

# Insert data into MongoDB Atlas collection
collection.insert_many(data)
