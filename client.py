import json
import socket
import time
import traceback
from thread import *
import threading
from random import randint


data_center_id = raw_input("Enter the data center id to connect to:")
client_id = raw_input("Enter the client id :")
with open("config.json", "r") as configFile:
    config = json.load(configFile)
    ip_address = config["data_center_details"][data_center_id]["ip"]
    port_number = config["data_center_details"][data_center_id]["port"]

    data_center_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        data_center_socket.connect((ip_address, int(port_number)))
        print 'Connected to ' + ip_address + ' on port ' + str(port_number)
        data = json.dumps({'name': 'client', 'type': 'CON'})
        data_center_socket.send(data)
    except:
        print 'Exception occurred while connecting to data center'
        print traceback.print_exc()


def receive_statemachine_output():
    while True:
        try:
            msg_data=data_center_socket.recv(4096)
            received_message_data=json.loads(msg_data)
            ticket_counter_result=received_message_data['ticket_count']
            log_value_result=received_message_data['log_value']
            print 'Request has been processed'
            print 'The resulting tickets from state machine is ',ticket_counter_result
            print 'The log value of data center is',log_value_result
        except:
            print 'Data center is down'
            break

start_new_thread(receive_statemachine_output, ())

while True:
    message = raw_input("Enter the request : ")
    if message.startswith("buy"):
        number_of_tickets = int(message[4:])
        print "number of tickets is ", number_of_tickets
        message_id = randint(0,10000)
        data = json.dumps({'client_id': client_id, 'type': 'BUY',
                           'number_of_tickets':number_of_tickets,
                           'message_id':message_id })
        data_center_socket.send(data)
    elif message.startswith("show"):
        data=json.dumps({'client_id':client_id,'type':'SHOW'})
        data_center_socket.send(data)
    elif message.startswith("change"):
        new_data_center_details = message.split(" ")
        ip_address = new_data_center_details[1]
        port = new_data_center_details[2]
        message_id = randint(0, 10000)
        data = json.dumps({'client_id': client_id, 'type': 'CHANGE', 'ip': ip_address,'port': port,
                           'message_id': message_id})
        data_center_socket.send(data)


