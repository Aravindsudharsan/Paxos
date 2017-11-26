import socket 
import time

host = socket.gethostname() 
port = 2004
BUFFER_SIZE = 2000 
MESSAGE = raw_input("Enter a message to send and connect to server:") 
 
tcpClientA = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
tcpClientA.connect((host, port))

tcpClientA.send(MESSAGE)
while MESSAGE != 'exit':
    data = tcpClientA.recv(BUFFER_SIZE)
    print " Client received data:", data
    #time.sleep(3)
    if (data==""):
	print "Leader is dead and election has to be started"	
  

tcpClientA.close() 
