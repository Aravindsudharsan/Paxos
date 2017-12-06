import json
import socket
import time
import traceback
from thread import *
from collections import deque
import threading
from Queue import Queue
import re

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
    acks_ballot_dict ={}
    acks_ballot_accept_numVal ={}
    buy_request_queue = deque([])
    accept_replies_ballot_dict={}
    log = []
    ticket_counter = 100
    leader_heartbeat_queue = Queue()
    reconfigure = False

    def initiate_phase_one(self, msg):
        if msg['type'] == 'BUY':
            self.add_to_ticket_request_queue(msg['number_of_tickets'])
        elif msg['type'] == 'CHANGE':
            self.add_to_ticket_request_queue(json.dumps(msg))
        self.send_prepare_message()

    def add_to_ticket_request_queue(self,number_of_tickets):
        print 'Adding to ticket queue :',number_of_tickets
        self.buy_request_queue.append(number_of_tickets)

    def send_prepare_message(self):
        self.ballot_number = self.ballot_number + 1
        #incrementing acks for this ballot number as leader votes for itself
        self.acks_ballot_dict[self.ballot_number] = 1
        data = json.dumps({'type': 'PREPARE',
                           'ballot_number': {'ballot_num': self.ballot_number, 'data_center_id': self.data_center_id},
                           'index': self.lowest_available_index})

        for send_data_center_id in send_data_center_channels:
            try:
                send_data_center_channels[send_data_center_id].send(data)
            except:
                continue

    def receive_prepare_message(self, prepare_message):

        received_ballot_number = prepare_message['ballot_number']
        origin_data_center_id = received_ballot_number['data_center_id']
        received_index = prepare_message['index']
        self.send_ack_message(received_ballot_number, origin_data_center_id, received_index)

    def send_ack_message(self,received_ballot_number, origin_data_center_id, received_index):

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
                 'accept_val' : accept_val
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
                        my_value = self.buy_request_queue[0]

                    self.send_accept_message(ballot_number,my_value,index)
                except:
                    print traceback.print_exc()

            else:
                print 'here received ack could be of a number less than or greater than the quorum size --- what to be done ???'


    def send_accept_message(self,received_ack_ballot_number,my_value,index):
        # setting ACCEPT REPLIES for this ballot number as 1 as the leader accepts its own value
        self.accept_replies_ballot_dict[received_ack_ballot_number['ballot_num']] = 1
        data = json.dumps({'type': 'ACCEPT',
                           'ballot_number': received_ack_ballot_number,
                           'my_value': my_value,
                           'index': index})

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

        if received_ballot_number['ballot_num'] >= self.ballot_number:
            #accepting the proposed value and setting AcceptNum and AcceptVal for that index
            self.accept_num_index_dict[received_index] = received_ballot_number['ballot_num']
            self.accept_val_index_dict[received_index] = proposed_value

            #send accept reply message to leader
            self.send_accept_reply_message(received_ballot_number,proposed_value,received_index)

    def send_accept_reply_message(self,received_ballot_number,proposed_value,received_index):
        origin_data_center_id = received_ballot_number['data_center_id']
        data = json.dumps(
            {'type': 'ACCEPT_REPLY',
             'ballot_number': received_ballot_number,
             'index': received_index,
             'my_value': proposed_value})

        send_data_center_channels[origin_data_center_id].send(data)


    def receive_accept_reply_message(self,accept_reply_message):

        ballot_number = accept_reply_message['ballot_number']
        received_accept_reply_ballot = ballot_number['ballot_num']
        decided_value = accept_reply_message['my_value']
        index = accept_reply_message['index']

        if received_accept_reply_ballot in self.accept_replies_ballot_dict:
            self.accept_replies_ballot_dict[received_accept_reply_ballot] = self.accept_replies_ballot_dict[received_accept_reply_ballot] + 1

        # received ACCEPT_REPLY from majority of cohort/follower data centers
        if self.accept_replies_ballot_dict[received_accept_reply_ballot] == self.quorum_size:
            # here can/should i execute state machine ??
            #here i have gotten majority in 2nd phase, so I'm the leader and now I can start sending heartbeat messages to all other data centers
            if self.leader_data_center_id == None:
                self.send_heartbeat_from_leader()
            if self.buy_request_queue[0] == decided_value:
                self.buy_request_queue.popleft()
                if len(self.log) == index:
                    self.log.append(decided_value)
                    self.lowest_available_index = index + 1
                    self.execute_state_machine(decided_value)
                    self.send_decide_message(ballot_number,decided_value,index)

        else:
            print "***"

    def send_decide_message(self,ballot_number,decided_value,index):
        data = json.dumps(
            {'type': 'DECIDE',
             'ballot_number': ballot_number,
             'my_value': decided_value,
             'index': index})

        for data_center_id in send_data_center_channels:
            try:
                send_data_center_channels[data_center_id].send(data)
            except:
                continue

    def receive_decide_message(self,decide_message):
        index = decide_message['index']
        decided_value = decide_message['my_value']
        print 'length of my log ---', len(self.log)
        print 'index value is ----', index
        if len(self.log) == index:
            print 'BEFORE STATE MACHINE EXEC '
            self.log.append(decided_value)
            self.lowest_available_index=index+1
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
            data_center_socket.connect((ip, int(port)))
            print 'Connected to ' + ip + ' on port ' + str(port)
            data = json.dumps({'name': 'data_center', 'type': 'CON'})
            data_center_socket.send(data)
            new_data_center_id = highest_data_center_id + 1
            send_data_center_channels[new_data_center_id] = data_center_socket
            highest_data_center_id = new_data_center_id
        except:
            print traceback.print_exc()

	
    def send_heartbeat_from_leader(self):
        self.leader_data_center_id = self.data_center_id
        start_new_thread(self.send_heartbeat_to_followers, ())

    def send_heartbeat_to_followers(self):
        while True:
            data = json.dumps({
                'type': 'HEARTBEAT',
                'leader_data_center_id': self.data_center_id
            })

            for send_data_center_id in send_data_center_channels:
                try:
                    send_data_center_channels[send_data_center_id].send(data)
                except:
                    continue
            time.sleep(3)

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

        elif message['type'] == 'CHANGE':
            my_value = json.dumps(message)

        self.add_to_ticket_request_queue(my_value)
        ballot_number = {'ballot_num': self.ballot_number, 'data_center_id': self.data_center_id}
        self.send_accept_message(ballot_number,my_value, self.lowest_available_index)

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
        time.sleep(10)
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
                    send_data_center_channels[i+1]=data_center_socket
                    highest_data_center_id = i+1
                except:
                    print 'Exception occurred while connecting to the other data centers '
                    print traceback.print_exc()

def receive_connect_message_common():
    for socket in recv_channels:
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
                            recv_client_channel.append(socket)
                        elif msg['name'] == 'data_center':
                            recv_data_center_channels.append(socket)
        except:
            print traceback.print_exc()

def receive_message_client():
    while True:
        try:
            if recv_client_channel[0]:
                message = recv_client_channel[0].recv(4096)
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
                        recv_client_channel[0].send(data)
        except:
            continue

def receive_message_datacenters():
    while True:
        for data_center_socket in recv_data_center_channels:
            try:
                message_dc = data_center_socket.recv(4096)
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
                        elif msg['type'] == 'BUY' or msg['type'] == 'CHANGE' :
                            #here getting request from peer data center means that this data center is the stable leader
                            #so directly initiate phase two
                            print 'CHECKING FOR MESSAGE FROM PEER ---->'
                            paxos_obj.initiate_phase_two(msg)
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

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.setblocking(0)
s.bind((ip_address, int(port_number)))
print 'Bound at ',ip_address, port_number
s.listen(10)

start_new_thread(setup_receive_channels, (s,))
t1 = threading.Thread(target=setup_send_channels, args=())
t1.start()
# wait till all send connections have been set up
t1.join()

#wait till all connections have been accepted before receiving messages from client/data center
time.sleep(10)
receive_connect_message_common()
start_new_thread(receive_message_client, ())
start_new_thread(receive_message_datacenters, ())
paxos_obj.reconfigure = True

while True:
    message = raw_input("Enter request for tickets : ")
    if message == "buy":
	    paxos_obj.buy_tickets
