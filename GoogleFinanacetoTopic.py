from google.cloud import pubsub_v1
from yahoo_fin import stock_info as si
import os
import datetime
import yfinance as yf

# Explicitly setting up environment variable by proving path which has service account details for the project.
# You have to create service account .json file by clicking on Navigation menu --> IAM and Admin --> Service Account -->
# --> Create service account.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="D:\GCP\PubSub_26_04_2020\PubsubwithAPIs-4cb3590c129d.json"

# Project ID of my project.
project_id = "pubsubwithapis-1587900731997"

# Topic name. You can create topic from console or from CLI.
topic_name = "my-topic"

# TO display the environment variable. You may ignore it.
print('Credentials from environ: {}'.format(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')))

# Creating publisher object using PublisherClient() method of pubsub_v1 class.
publisher = pubsub_v1.PublisherClient()

# The `topic_path` method creates a fully qualified identifier in the form `projects/{project_id}/topics/{topic_name}`
topic_path = publisher.topic_path(project_id, topic_name)

# Getting Microsoft Corporation live stock rate from Yahoo Finance.
data = si.get_live_price("MSFT")

# Adding Stock name, Stock Code, Stock price and time into the data which will be sent to topic.
data = "Microsoft Corporation" + ",MSTF," + str(data).encode("utf-8") + "," + str(datetime.datetime.now())
print(data)

# Sending the data to topic.
future = publisher.publish(topic_path, data=data)

print("Published messages.")