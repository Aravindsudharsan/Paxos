import json
import socket
import time
import traceback
from thread import *
import threading

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

def receive_statemachine_output(data_center_socket):#added
	msg_data=data_center_socket.recv(4096)
	received_message_data=json.loads(msg_data)
	print "The resulting tickets from state machine is", received_message_data['ticket_count']
	print "The commited log value is" , received_message_data['log_value']

while True:
    message = raw_input("Enter request for tickets : ")
    if message.startswith("buy"):
        number_of_tickets = int(message[4:])
        print "number of tickets is ", number_of_tickets
        data = json.dumps({'client_id': client_id, 'type': 'BUY', 'number_of_tickets':number_of_tickets})
        data_center_socket.send(data)
start_new_thread(receive_statemachine_output,())#added
    


