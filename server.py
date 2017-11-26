import socket 
from threading import Thread 
from SocketServer import ThreadingMixIn 
from SocketServer import ThreadingMixIn 
import time


class ClientThread(Thread): 
 
    def __init__(self,ip,port): 
        Thread.__init__(self) 
        self.ip = ip 
        self.port = port 
        print "[+] New server socket thread started for " + ip + ":" + str(port) 
 
    def run(self): 
	data = conn.recv(2048) 
        print "Server received data:", data       
	while True : 
            MESSAGE = "HEARTBEAT"
	    time.sleep(3)	
            conn.send(MESSAGE)   


TCP_IP = '0.0.0.0' 
TCP_PORT = 2004 
BUFFER_SIZE = 20  


tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
tcpServer.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
tcpServer.bind((TCP_IP, TCP_PORT)) 
 
 
while True: 
    tcpServer.listen(4) 
    print "Data center waiting for connections" 
    (conn, (ip,port)) = tcpServer.accept() 
    newthread = ClientThread(ip,port) 
    newthread.start() 
  
