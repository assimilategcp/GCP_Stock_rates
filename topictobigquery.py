from google.cloud import pubsub_v1
import os
from google.cloud import bigquery
import logging

# Explicitly setting up environment variable by proving path which has service account details for the project.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="D:\GCP\PubSub_26_04_2020\PubsubwithAPIs-4cb3590c129d.json"

# Project ID of my project.
project_id = "pubsubwithapis-1587900731997"

# Subscription name.
subscription_name = "my-sub"

# "How long the subscriber should listen for the messages in seconds"
timeout = 500.0

# Created subscriber object.
subscriber = pubsub_v1.SubscriberClient()

# The `subscription_path` method creates a fully qualified identifier in the form `projects/{project_id}/subscriptions/{subscription_name}`
subscription_path = subscriber.subscription_path(project_id, subscription_name)

def callback(message):

    # Storing message data into variable.
    stock_details = message.data

    # Message was sent with comma as separator so splitting the same.
    temp_list = stock_details.split(",")

    # Printing the message for my reference. Not necessary, you may ignore it.
    print(temp_list[0] + " " + temp_list[1] + " " + temp_list[2] + " " + temp_list[3])

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Set table_id to the ID of the model to fetch.
    table_id = "pubsubwithapis-1587900731997.mydataset.stock_rates1"

    # Make an API request.
    table = client.get_table(table_id)

    # Setting up the row to insert in the bigquery table.
    rows_to_insert = [(temp_list[0], temp_list[1], temp_list[2], temp_list[3],'','','','','','')]

    # Inserting the row in bigquery table.
    errors = client.insert_rows(table, rows_to_insert)
    if errors == []:
       print("New rows have been added.")
    else:
        print(errors)

    # Acknowledging the message, if not done then pubsub will resend the message once ackDeadline time lapses.
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for messages on {}..\n".format(subscription_path))

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except:
        streaming_pull_future.cancel()