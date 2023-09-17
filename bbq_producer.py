"""
   This program emits task from a CSV file and sends the message to a queue on the RabbitMQ server.
   Original source code: Denise Case
   Modifications for CSV input: Lindsey Sullivan
   Date: 09/09/2023

"""

# Import Libraries
import pika
import sys
import webbrowser
import csv
import time
import os

from util_logger import setup_logger
logger, logname = setup_logger(__file__)

# Setup path to directory for CSV file
os.chdir("/Users/lindseysullivan/Documents/School/Streaming-Data/Modules/streaming-05-smart-smoker")

# Declare Constants
INPUT_CSV_FILE = "smoker-temps.csv"
HOST = "localhost"
QUEUE1 = "01-smoker"
QUEUE2 = "02-food-A"
QUEUE3 = "03-food-B"

# ----------------------------------------------------------
# Define Program Functions
# ----------------------------------------------------------
def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer == True:
         ans = input("Would you like to monitor RabbitMQ queues? y or n ")
         print()
         if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
def prepare_message(row,field_index):
    """
    Returns timestamp and the desired field index value to send as a tuple
    Checks if the field_value is not empty then it will be a float value, else it will pass
    that no data was received from the row. 
    """
    timestamp = row[0]
    if field_index < len(row):
        field_value_str = row[field_index]
        if field_value_str:  # Check if the field_value_str is not empty
            try:
                field_value = float(field_value_str)
            except ValueError:
                field_value = "Invalid float value"
        else:
            field_value = "No data received from row"
    else:
        field_value = "No data received from row"

    # construct binary message from data
    fstring_message= f"[{timestamp},{field_value}]"
    message = fstring_message.encode()

    logger.info(f"Prepared binary message {message}....")

    return message

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.
    Leaving in this function due to the Single Responsibility Principle, therefore; this code can be reused
    in the future even though it will not be leveraged in this version.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message} to {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def stream_csv_messages (input_file_name: str,host: str,queue_name1: str, queue_name2: str, queue_name3: str):
    """
    Read input CSV file and send each row as a message to the dedicated queue for a worker.
    Messages are seperated into specialized messages and index information is joined into a singular message.
    
    Parameters:
        input_file_name (str): The name of the CSV file
        host (str): host name or IP address of the rabbitmq server
        queue_name1 (str): the name of the first queue
        queue_name2 (str): the name of the second queue
        queue_name3 (str): the name of the third queue
    """
    try:

        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()

        # use the channels to declare a durable queue
        ch.queue_declare(queue=queue_name1, durable=True) # queue for "01-smoker"
        ch.queue_declare(queue=queue_name2, durable=True) # queue for "02-food-A"
        ch.queue_declare(queue=queue_name3, durable=True) # queue for "03-food-B"

        
        logger.info(f"Reading messages from {input_file_name}...")
        logger.info(f"--- USER GUIDE: To close the program please select CTRL + c on your keyboard. ---")

        with open(input_file_name,"r",encoding="utf-8") as input_file:
            reader = csv.reader(input_file,delimiter=",")
            next(reader,None)

            for row in reader:
                timestamp = row[0]
                
                # send the messages to the desired queue
                send_message(host,queue_name1,prepare_message(row,1))
                send_message(host,queue_name2,prepare_message(row,2))
                send_message(host,queue_name3,prepare_message(row,3))
                time.sleep(30) # wait 30 seconds between messages

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # Close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    # true shows the offer/false turns off the offer for the user
    offer_rabbitmq_admin_site(show_offer=True)

    # Stream messages from the CSV file and send them to RabbitMQ
    stream_csv_messages(INPUT_CSV_FILE,HOST,QUEUE1,QUEUE2,QUEUE3)
    
    