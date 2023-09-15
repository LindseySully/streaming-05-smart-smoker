# streaming-05-smart-smoker
### Student Information:
- Lindsey Sullivan
- Date: 9/15/2023
- GitHub Repository: https://github.com/LindseySully/streaming-05-smart-smoker

# Project Prerequisites
1. Git
1. Python 3.7+ (3.11+ Preferred)
1. VS Code Editor
1. VS Code Extension: Python (by Microsoft)
1. RabbitMQ

# Program Details

## Project Libraries
1. sys
    - Used to provide various functions and variables that can be used to manipulate different parts of the python runtime environment
1. time
    - Used to delay the messages sent from producer.py
1. csv
    - Used to read & write to CSV files
1. os
    - Used to specify dedicated paths to directories within the project
1. pika
    - Used to implement the AMQP protocol for RabbitMQ.
1. webbrowser
    - Used to connect to the RabbitMQ Admin webpage

## Project Files

### Producer - bbq_producer.py
This program is responsible for emit_messages from a dedicated CSV file via RabbitMQ. The program completes the following:
1. Define Constant Variables
    - INPUT_CSV_FILE: Sets the CSV file as the smoker-temps.csv
    - HOST: Localhost - can be modified for different host information
    - QUEUE1: Set dedicated queue for smoker temperatures - 01-smoker
    - QUEUE2: Set dedicated queue for food A temperature - 02-food-A
    - QUEUE3: Set dedicated queue for food B temperature - 03-food-B
1. Define Program Functions
    - offer_rabbitmq_admin_site: offers the user to open the RabbitMQ admin page
    - send_to_queue: connect and declare the queue and print message of what queue and server the message was sent to.
        - host (str): host name or IP address of rabbitmq server
        - queue_name (str): the name of the queue
        - message (str): message being sent
