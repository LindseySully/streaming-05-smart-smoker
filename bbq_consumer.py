"""
    This program will continuously wait for messages sent from the bbq_producer.
    It listens for messages across 3 queues - Smoker, Food A, and Food B.
    If a Smoker Alert or Food Stall occurs then the notification is written to a CSV file. 
"""

# Import project libraries
import pika
import time
import csv
import sys
from collections import deque
import json

# Declare constants
HOST = "localhost"
QUEUE1 = "01-smoker"
QUEUE2 = "02-food-A"
QUEUE3 = "03-food-B"

# set deque max length
smoker_temperature = deque(maxlen=5)
food_a_temperature = deque(maxlen=20)
food_b_temperature = deque(maxlen=20)

# declare constants for temperature change
SMOKER_ALERT = 15.0 # smoker changes less than 15 degrees F in 2.5 minutes
FOOD_ALERT = 1.0 # food changes less than 1 degree F in 10 minutes


def record_alert_to_csv(file_name, timestamp, alert):
    """
    Opens a CSV file to write the alert message to a CSV file from the queue.
    Used in the callback functions.
    """
    with open(file_name, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([timestamp, alert])

def smoker_callback(ch, method, properties, body):
    """
    Define behavior on getting a message from the 01-Smoker queue. 
    Smoker_callback looks for a potential stall if the smoker temperature decreases by more than 15 degrees in 2.5 minutes.
    """
    try:
        # Decode the JSON message
        data = json.loads(body)
        temp = float(data.get('value', 0.0))  # Extract the temperature value
        
        smoker_temperature.append(temp)

        if len(smoker_temperature) == 5.0 and (smoker_temperature[0] - smoker_temperature[-1] >= SMOKER_ALERT):
            alert_message = f"Smoker Alert! Temperature decreased by more than 15F in 2.5 minutes. Current Temp: {temp}F"
            print(alert_message)
            record_alert_to_csv("Smoker_alerts.csv", time.strftime("%Y-%m-%d %H:%M:%S"), alert_message)
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print("Error decoding JSON message")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except ValueError:
        print("Error converting temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def food_a_callback(ch, method, properties, body):
    """
    Define behavior on getting a message from the 02-food-a queue
    food_a_callback looks for a potential stall if the food temperature changes less than 1F in 10 minutes.
    """
     # Decode the JSON message
    try:
        data = json.loads(body)
        temp = float(data.get('value', 0.0))  # Extract the temperature value
        
        food_a_temperature.append(temp)

        if len(food_a_temperature) == 20 and (abs(max(food_a_temperature) - min(food_a_temperature) <= FOOD_ALERT)):
            alert_message = f"Food Stall Alert for Food A! Temperature changed less than 1F in 10 minutes. Current Temp: {temp}F"
            print(alert_message)
            print(f"Food A temperatures: {list(food_a_temperature)}")
            print(f"Max Temp: {max(food_a_temperature)}, Min Temp: {min(food_a_temperature)}")
            print(f"Temperature difference: {max(food_a_temperature) - min(food_a_temperature)}")

            record_alert_to_csv("Food_stall_alerts.csv", time.strftime("%Y-%m-%d %H:%M:%S"), alert_message)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print("Error decoding JSON message")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except ValueError:
        print("Error converting temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def food_b_callback(ch, method, properties, body):
    """
    Define behavior on getting a message from the 03-food-b queue
    food_b_callback looks for a potential stall if the food temperature changes less than 1F in 10 minutes.
    """
    try:
         # Decode the JSON message
        data = json.loads(body)
        temp = float(data.get('value', 0))  # Extract the temperature value
   
        food_b_temperature.append(temp)

        if len(food_b_temperature) == 20 and (abs(max(food_b_temperature) - min(food_b_temperature) <= FOOD_ALERT)):
            alert_message = f"Food Stall Alert for Food B! Temperature changed less than 1F in 10 minutes. Current Temp: {temp}F"
            print(alert_message)
            record_alert_to_csv("Food_stall_alerts.csv", time.strftime("%Y-%m-%d %H:%M:%S"), alert_message)


    # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except json.JSONDecodeError:
        print("Error decoding JSON message")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except ValueError:
        print("Error converting temperature to float")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main(hn: str,queue1,queue2,queue3):
    """
    Continuously listen for a message across 3 queues and processes message using the
    appropriate callback functions.
    """
    try:
        # Create a connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
     # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:    
       # Declare each queue
        for queue_name in [queue1, queue2, queue3]:
            channel.queue_declare(queue=queue_name, durable=True)

        # Set up the consumers for each queue
        channel.basic_consume(queue=queue1, on_message_callback=smoker_callback, auto_ack=False)
        channel.basic_consume(queue=queue2, on_message_callback=food_a_callback, auto_ack=False)
        channel.basic_consume(queue=queue3, on_message_callback=food_b_callback, auto_ack=False)

        # Inform the user that the consumer is ready to begin
        print(' [*] Waiting for messages. To exit press CTRL+C')
        
        # Start the consumers
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as e:
        print("Error connecting to RabbitMQ server: ", e)
    except KeyboardInterrupt:
        print("\nConsumer has been stopped.")
    except Exception as e:
        print("An unexpected error occurred: ", e)
    finally:
        try:
            connection.close()
        except NameError:  # In case connection was not established, the variable won't exist
            pass

if __name__ == "__main__":
    main(HOST,QUEUE1,QUEUE2,QUEUE3)