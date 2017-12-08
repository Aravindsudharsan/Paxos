import json
import socket
import time
import traceback
from thread import *
from collections import deque
import threading
from Queue import Queue
import re
import math

recv_client_channel = []
recv_data_center_channels = []
send_data_center_channels = {}
recv_channels = []
data_center_id = None
highest_data_center_id = 0

class MultiPaxos:
    data_center_id = None
    leader_data_center_id = None
    lowest_available_index = 0
    ballot_number = 0
    accept_num_index_dict ={}
    accept_val_index_dict = {}
    quorum_size = 2
    alive_sites = 3
    acks_ballot_dict ={}
    acks_ballot_accept_numVal ={}
    buy_request_queue_dict = {}
    accept_replies_ballot_dict={}
    log = []
    ticket_counter = 100
    leader_heartbeat_queue = Queue()
    reconfigure = False
    ip_address = None
    port_number = None

    def initiate_phase_one(self, msg):
        message_id = msg['message_id']
        if msg['type'] == 'BUY':
            self.add_to_ticket_request_queue(message_id, msg['number_of_tickets'])
            value = msg['number_of_tickets']
        elif msg['type'] == 'CHANGE':
            self.add_to_ticket_request_queue(message_id, json.dumps(msg))
            value = msg
        start_new_thread(self.check_request_queue_cleared,(message_id,value,))
        self.send_prepare_message(message_id)

    def add_to_ticket_request_queue(self,message_id,number_of_tickets):
        print 'Adding to ticket queue :',number_of_tickets
        self.buy_request_queue_dict[message_id] = number_of_tickets

    def send_prepare_message(self,message_id):
        self.ballot_number = self.ballot_number + 1
        #incrementing acks for this ballot number as leader votes for itself
        self.acks_ballot_dict[self.ballot_number] = 1
        data = json.dumps({'type': 'PREPARE',
                           'ballot_number': {'ballot_num': self.ballot_number, 'data_center_id': self.data_center_id},
                           'index': self.lowest_available_index,
                           'message_id': message_id})

        for send_data_center_id in send_data_center_channels:
            try:
                send_data_center_channels[send_data_center_id].send(data)
            except:
                continue

    def receive_prepare_message(self, prepare_message):

        received_ballot_number = prepare_message['ballot_number']
        origin_data_center_id = received_ballot_number['data_center_id']
        received_index = prepare_message['index']
        message_id = prepare_message['message_id']
        self.send_ack_message(received_ballot_number, origin_data_center_id, received_index, message_id)

    def send_ack_message(self,received_ballot_number, origin_data_center_id, received_index, message_id):

        if received_ballot_number['ballot_num'] >= self.ballot_number:
            self.ballot_number = received_ballot_number['ballot_num']
            if received_index in self.accept_num_index_dict and received_index in self.accept_val_index_dict:
                #A value has already been accepted by this data center for the given log index
                accept_num = self.accept_num_index_dict[received_index]
                accept_val = self.accept_val_index_dict[received_index]
            else:
                accept_num = None
                accept_val = None

            data = json.dumps(
                {'type': 'ACK',
                 'ballot_number': received_ballot_number,
                 'index': received_index,
                 'accept_num' : accept_num,
                 'accept_val' : accept_val,
                 'message_id' : message_id
                 })

            send_data_center_channels[origin_data_center_id].send(data)

        else:
            print "should i send reject ? CHECK CHECK !!!"

    def receive_ack_message(self, ack_message):

            ballot_number = ack_message['ballot_number']
            received_ack_ballot_number = ack_message['ballot_number']['ballot_num']
            received_ack_accept_num = ack_message['accept_num']
            received_ack_accept_val = ack_message['accept_val']
            index = ack_message['index']
            message_id = ack_message['message_id']

            if received_ack_ballot_number in self.acks_ballot_dict:
                self.acks_ballot_dict[received_ack_ballot_number] = self.acks_ballot_dict[received_ack_ballot_number] + 1

            if received_ack_ballot_number in self.acks_ballot_accept_numVal:
                self.acks_ballot_accept_numVal[received_ack_ballot_number].append({
                'accept_num': received_ack_accept_num,
                'accept_val': received_ack_accept_val
            })
            else:
                self.acks_ballot_accept_numVal[received_ack_ballot_number] = [{
                    'accept_num': received_ack_accept_num,
                    'accept_val': received_ack_accept_val
            }]
            #received acks from majority of data centers
            if self.acks_ballot_dict[received_ack_ballot_number] == self.quorum_size:
                try:
                    array_accept_numVal = self.acks_ballot_accept_numVal[received_ack_ballot_number]
                    highest_recv_ballot_num = max(d['accept_num'] for d in array_accept_numVal)
                    for d in array_accept_numVal:
                        if d['accept_num'] == highest_recv_ballot_num:
                            highest_recv_ballot_val = d['accept_val']
                            break

                    #if a value is got from highest ballot from received ACKs, set my value to that value; else take the first element from the request queue
                    if highest_recv_ballot_val:
                        my_value = highest_recv_ballot_val
                    else:
                        my_value = self.buy_request_queue_dict[message_id]

                    self.send_accept_message(ballot_number,my_value,index, message_id)
                except:
                    print traceback.print_exc()

            else:
                print 'here received ack could be of a number less than or greater than the quorum size --- what to be done ???'


    def send_accept_message(self,received_ack_ballot_number,my_value,index, message_id):
        # setting ACCEPT REPLIES for this ballot number as 1 as the leader accepts its own value
        self.accept_replies_ballot_dict[received_ack_ballot_number['ballot_num']] = 1
        data = json.dumps({'type': 'ACCEPT',
                           'ballot_number': received_ack_ballot_number,
                           'my_value': my_value,
                           'index': index,
                           'message_id': message_id
                           })

        for send_data_center_id in send_data_center_channels:
            try:
                send_data_center_channels[send_data_center_id].send(data)
            except:
                continue

    #method to be executed when followers receive accept message from the leader
    def receive_accept_message(self,accept_message):
        received_index = accept_message['index']
        received_ballot_number = accept_message['ballot_number']
        proposed_value = accept_message['my_value']
        message_id = accept_message['message_id']


        if received_ballot_number['ballot_num'] >= self.ballot_number:
            #accepting the proposed value and setting AcceptNum and AcceptVal for that index
            self.accept_num_index_dict[received_index] = received_ballot_number['ballot_num']
            self.accept_val_index_dict[received_index] = proposed_value

            #send accept reply message to leader
            self.send_accept_reply_message(received_ballot_number,proposed_value,received_index, message_id)

    def send_accept_reply_message(self,received_ballot_number,proposed_value,received_index, message_id):
        origin_data_center_id = received_ballot_number['data_center_id']
        data = json.dumps(
            {'type': 'ACCEPT_REPLY',
             'ballot_number': received_ballot_number,
             'index': received_index,
             'my_value': proposed_value,
             'message_id': message_id
             })

        send_data_center_channels[origin_data_center_id].send(data)


    def receive_accept_reply_message(self,accept_reply_message):

        ballot_number = accept_reply_message['ballot_number']
        received_accept_reply_ballot = ballot_number['ballot_num']
        decided_value = accept_reply_message['my_value']
        index = accept_reply_message['index']
        message_id = accept_reply_message['message_id']

        if received_accept_reply_ballot in self.accept_replies_ballot_dict:
            self.accept_replies_ballot_dict[received_accept_reply_ballot] = self.accept_replies_ballot_dict[received_accept_reply_ballot] + 1

        # received ACCEPT_REPLY from majority of cohort/follower data centers
        if self.accept_replies_ballot_dict[received_accept_reply_ballot] == self.quorum_size:
            # here can/should i execute state machine ??
            #here i have gotten majority in 2nd phase, so I'm the leader and now I can start sending heartbeat messages to all other data centers
            if self.leader_data_center_id == None:
                self.send_heartbeat_from_leader()
            if self.buy_request_queue_dict[message_id] == decided_value:
                del self.buy_request_queue_dict[message_id]

                if len(self.log) == index:
                    self.log.append(decided_value)
                    self.lowest_available_index = index + 1
                    self.execute_state_machine(decided_value)
                    self.send_decide_message(ballot_number,decided_value,index,message_id)

        else:
            print "***"

    def send_decide_message(self,ballot_number,decided_value,index,message_id):
        data = json.dumps(
            {'type': 'DECIDE',
             'ballot_number': ballot_number,
             'my_value': decided_value,
             'index': index,
             'message_id': message_id
             })

        for data_center_id in send_data_center_channels:
            try:
                send_data_center_channels[data_center_id].send(data)
            except:
                continue

    def receive_decide_message(self,decide_message):
        index = decide_message['index']
        decided_value = decide_message['my_value']
        if len(self.log) == index:
            self.log.append(decided_value)
            self.lowest_available_index = index+1
            #should i execute state machine here ??
            self.execute_state_machine(decided_value)

    def execute_state_machine(self,decided_value):
        try:
            self.ticket_counter = self.ticket_counter - decided_value
            print 'Current value of state machine :', self.ticket_counter, ' tickets'
        except:
            print 'Config change request'
            self.add_data_center(decided_value)


    def add_data_center(self,decided_value):
        data_center_details = json.loads(decided_value)
        ip = data_center_details['ip']
        port = int(data_center_details['port'])
        data_center_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            global highest_data_center_id
            if ip == self.ip_address and port == int(self.port_number):
                print 'Detected own data center'
            else:
                data_center_socket.connect((ip, int(port)))
                print 'Connected to ' + ip + ' on port ' + str(port)
                data = json.dumps({'name': 'data_center', 'type': 'CON'})
                data_center_socket.send(data)
                new_data_center_id = highest_data_center_id + 1
                send_data_center_channels[new_data_center_id] = data_center_socket
                highest_data_center_id = new_data_center_id
                self.alive_sites += 1
                self.quorum_size = math.floor(self.alive_sites/2) + 1
                print 'New quorum size ', self.quorum_size
        except:
            print traceback.print_exc()
	
    def send_heartbeat_from_leader(self):
        self.leader_data_center_id = self.data_center_id
        start_new_thread(self.send_heartbeat_to_followers, ())

    def send_heartbeat_to_followers(self):
        while True:
            time.sleep(3)
            data = json.dumps({
                'type': 'HEARTBEAT',
                'leader_data_center_id': self.data_center_id,
                'lowest_index':self.lowest_available_index
            })

            for send_data_center_id in send_data_center_channels:
                try:
                    send_data_center_channels[send_data_center_id].send(data)
                except:
                    continue

    def receive_heartbeat_from_leader(self,msg):
        #3 scenarios
        # - 1.receiving heartbeat for the first time from the very first leader
        # - 2.receiving heartbeat for the subsequent times after a leader has been elected
        # - 3.receiving heartbeat after old leader is dead and new leader has gotten elected --- instead of this can we reset leader data center id to be none
        #   when the leader failure gets detected ???
        self.leader_heartbeat_queue.put(msg)
        if self.leader_data_center_id == None:
            # - 1.receiving heartbeat for the first time from the very first leader
            self.leader_data_center_id = msg['leader_data_center_id']
            start_new_thread(self.check_heartbeat_from_leader, ())
        leader_lowest_available_index = msg['lowest_index']

        if leader_lowest_available_index > self.lowest_available_index:
            #Log inconsistency
            self.send_update_request_leader()

    def check_heartbeat_from_leader(self):
        while True:
            #check the queue max every 3 seconds to see if a value is present (True means it is a blocking call )
            time.sleep(3)
            try:
                #print 'BEFORE POLLING%%%'
                heartbeat_message = self.leader_heartbeat_queue.get(False)
            except:
                #print traceback.print_exc()
                print '** Detected leader failure **'
                del send_data_center_channels[self.leader_data_center_id]
                self.leader_data_center_id = None
                break

    def forward_request_to_leader(self,message):
        send_data_center_channels[self.leader_data_center_id].send(json.dumps(message))

    def initiate_phase_two(self,message):
        #this function directly initiates phase 2 as leader is stable
        if message['type'] == 'BUY':
            my_value = message['number_of_tickets']
            value = message['number_of_tickets']

        elif message['type'] == 'CHANGE':
            my_value = json.dumps(message)
            value = message
        message_id = message['message_id']
        self.add_to_ticket_request_queue(message_id, my_value)
        ballot_number = {'ballot_num': self.ballot_number, 'data_center_id': self.data_center_id}
        start_new_thread(self.check_request_queue_cleared, (message_id,value,))
        self.send_accept_message(ballot_number,my_value, self.lowest_available_index, message_id)

    def send_update_request_leader(self):
        message = json.dumps({
            'type': 'UPDATE',
            'last_index_follower': self.lowest_available_index-1,
            'data_center_id': self.data_center_id
        })
        try:
            send_data_center_channels[self.leader_data_center_id].send(message)
        except:
            print traceback.print_exc()

    def send_log_update(self, msg):
        try:
            received_follower_last_index = msg['last_index_follower']
            data_center_id = msg['data_center_id']
            start_index = received_follower_last_index + 1
            new_log = self.log[start_index:]
            message = json.dumps({
                'type': 'UPDATED LOG',
                'log': new_log,
                'start_index' : start_index
            })
            send_data_center_channels[data_center_id].send(message)
        except:
            print traceback.print_exc()

    def receive_log_update(self, msg):
        received_log_value = msg['log']
        start_index = msg['start_index']
        self.log[start_index:] = received_log_value
        self.lowest_available_index = len(self.log)
        self.execute_log_update(start_index)

    def execute_log_update(self, start_index):
        for log_value in self.log[start_index:]:
            self.execute_state_machine(log_value)

    def check_request_queue_cleared(self,message_id,value):
        while True:
            time.sleep(15)
            if self.buy_request_queue_dict:
                if message_id in self.buy_request_queue_dict:
                    print "Retrying request"
                    if self.leader_data_center_id is None:
                        self.send_prepare_message(message_id)
                    elif self.leader_data_center_id != self.data_center_id:
                        del self.buy_request_queue_dict[message_id]
                        if isinstance(value, int):
                            self.forward_request_to_leader({'client_id': 1, 'type': 'BUY', 'number_of_tickets': value,'message_id': message_id})
                        else:
                            self.forward_request_to_leader(value)
                        break
                    else:
                        ballot_number = {'ballot_num': self.ballot_number, 'data_center_id': self.data_center_id}
                        if isinstance(value, int):
                            self.send_accept_message(ballot_number, value, self.lowest_available_index, message_id)
                        else:
                            self.send_accept_message(ballot_number, json.dumps(value), self.lowest_available_index, message_id)
                else:
                    break
#******************


def setup_receive_channels(s):
    while 1:
        try:
            conn, addr = s.accept()
            recv_channels.append(conn)

            if paxos_obj.reconfigure:
                print 'Adding new config data center to recv_data_center_channels'
                recv_data_center_channels.append(conn)
        except:
            continue
            print 'Exception occurred while setting up receive channels '
        print 'Connected with ' + addr[0] + ':' + str(addr[1])


def setup_send_channels():
        global highest_data_center_id
        time.sleep(10)
        data_center_counter = 1
        for i in range(3):
            if str(i+1) != data_center_id:
                data_center_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ip_address = config["data_center_details"][str(i+1)]["ip"]
                port_number = config["data_center_details"][str(i+1)]["port"]
                try:
                    data_center_socket.connect((ip_address, int(port_number)))
                    print 'Connected to ' + ip_address + ' on port ' + str(port_number)
                    data = json.dumps({'name': 'data_center', 'type': 'CON'})
                    data_center_socket.send(data)
                    send_data_center_channels[i+1] = data_center_socket
                    data_center_counter += 1
                    highest_data_center_id = data_center_counter
                except:
                    print 'Exception occurred while connecting to the other data centers '
                    print traceback.print_exc()

def receive_connect_message_common(socket):
        try:
            received_message = socket.recv(4096)
            if received_message:
                messages = re.split('(\{.*?\})(?= *\{)', received_message)
                for message in messages:
                    if message == '\n' or message == '' or message is None:
                        continue
                    msg = json.loads(message)
                    if msg['type'] == 'CON':
                        if msg['name'] == 'client':
                            print 'RECEIVED CONNECTION FROM CLIENT'
                            start_new_thread(receive_message_client, (socket,))
                        elif msg['name'] == 'data_center':
                            print 'RECEIVED CONNECTION FROM DATACENTER'
                            start_new_thread(receive_message_datacenters, (socket,))
        except:
            print traceback.print_exc()

def receive_message_client(socket):
    while True:
        try:
            message = socket.recv(4096)
            if message:
                print "Message received from client ", message
                msg = json.loads(message)
                if msg['type'] == 'BUY' or msg['type'] == 'CHANGE':
                    # initiate Phase 1 of Paxos to float a leader election
                    # here we need to capture the number of tickets !!!! should i send it in parameter to initiate_phase_one ??
                    # 3 scenarios - 1.I'm the leader 2.I'm a follower and a leader exists 3.No leader has been elected till now
                    if paxos_obj.data_center_id == paxos_obj.leader_data_center_id:
                        print 'Leader here - Directly run phase 2 '
                        # here must initiate phase 2 directly
                        paxos_obj.initiate_phase_two(msg)
                    elif paxos_obj.leader_data_center_id is not None and paxos_obj.leader_data_center_id != paxos_obj.data_center_id:
                        print 'Follower here - forwarding request to leader'
                        paxos_obj.forward_request_to_leader(msg)
                    elif paxos_obj.leader_data_center_id == None:
                        print 'I have to initiate leader election '
                        paxos_obj.initiate_phase_one(msg)
                elif msg['type'] == 'SHOW':
                    data = json.dumps({'key_value':'RESULT','ticket_count':paxos_obj.ticket_counter,'log_value':paxos_obj.log})
                    socket.send(data)
        except:
            continue


def receive_message_datacenters(socket):
    while True:
        try:
            message_dc = socket.recv(4096)
            if message_dc:
                messages_dc = re.split('(\{.*?\})(?= *\{)', message_dc)
                for message in messages_dc:
                    if message == '\n' or message == '' or message is None:
                        continue
                    msg = json.loads(message)
                    print "Message received from data center ", message
                    if msg['type'] == 'PREPARE':
                        paxos_obj.receive_prepare_message(msg)
                    elif msg['type'] == 'ACK':
                        paxos_obj.receive_ack_message(msg)
                    elif msg['type'] == 'ACCEPT':
                        paxos_obj.receive_accept_message(msg)
                    elif msg['type'] == 'ACCEPT_REPLY':
                        paxos_obj.receive_accept_reply_message(msg)
                    elif msg['type'] == 'DECIDE':
                        paxos_obj.receive_decide_message(msg)
                    elif msg['type'] == 'HEARTBEAT':
                        paxos_obj.receive_heartbeat_from_leader(msg)
                    elif msg['type'] == 'BUY' or msg['type'] == 'CHANGE':
                        #here getting request from peer data center means that this data center is the stable leader
                        #so directly initiate phase two
                        print 'CHECKING FOR MESSAGE FROM PEER ---->'
                        paxos_obj.initiate_phase_two(msg)
                    elif msg['type'] == 'UPDATE':
                        paxos_obj.send_log_update(msg)
                    elif msg['type'] == 'UPDATED LOG':
                        paxos_obj.receive_log_update(msg)
        except:
            continue

#start of program execution

data_center_id = raw_input("Enter the data center id :")
paxos_obj = MultiPaxos()
paxos_obj.data_center_id = int(data_center_id)
with open("config.json", "r") as configFile:
    config = json.load(configFile)
    ip_address = config["data_center_details"][data_center_id]["ip"]
    port_number = config["data_center_details"][data_center_id]["port"]

paxos_obj.ip_address = ip_address
paxos_obj.port_number = port_number

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

s.bind((ip_address, int(port_number)))
print 'Bound at ',ip_address, port_number
s.listen(10)

#start_new_thread(setup_receive_channels, (s,))
t1 = threading.Thread(target=setup_send_channels, args=())
t1.start()
# wait till all send connections have been set up
t1.join()

while True:
    conn, addr = s.accept()
    start_new_thread(receive_connect_message_common, (conn,))

