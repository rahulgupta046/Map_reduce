import socket
import threading
import csv 
# from _thread import *
import time 
import os
import yaml

'''
Set command needs a lock on the file. 
Reader writer lock, as soon as set command initiated by any client, Writer lock
'''



#class for readerwriterlock
class ReadWriteLock:
    """ A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self):
        self._read_ready = threading.Condition(threading.Lock(  ))
        self._readers = 0

    def acquire_read(self):
        """ Acquire a read lock. Blocks only if a thread has
        acquired the write lock. """
        self._read_ready.acquire(  )
        try:
            self._readers += 1
        finally:
            self._read_ready.release(  )

    def release_read(self):
        """ Release a read lock. """
        self._read_ready.acquire(  )
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll(  )
        finally:
            self._read_ready.release(  )

    def acquire_write(self):
        """ Acquire a write lock. Blocks until there are no
        acquired read or write locks. """
        self._read_ready.acquire(  )
        while self._readers > 0:
            self._read_ready.wait(  )

    def release_write(self):
        """ Release a write lock. """
        self._read_ready.release(  )

with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)
    hostname = socket.gethostname()
    server_IP = socket.gethostbyname(hostname)
    cfg['store']['ip'] = server_IP
with open('config.yml', 'w') as f:
    yaml.dump(cfg, f)
masterCfg = cfg['master']
storeCfg = cfg['store']

# create client class which inherits from threading
class Client(threading.Thread):
    def __init__(self, client_addr, client_socket):
        threading.Thread.__init__(self)
        self.client_addr = client_addr
        self.client_socket = client_socket
        print("connected to ", client_addr)

    def set_command(self, key, value):
        # print("execute set command for key - %s, value - %s" %(key, value))
        #Get dictionary values from file
        # print("KEY - %s, VALUE - %s"%(key, value))
        # print()
        store_data = {}
        with open('store.txt','r', encoding='cp437', errors = 'ignore') as store:
            
            reader = store.readlines()
            for line in reader:
                #key and value separated by first comma
                tmp = line.find(':')
                #line[0] is key, line[1] is value
                store_data[line[:tmp]] = line[tmp+1:]   
        #make change in dictionary
        store_data[key] = value

        with open('store.txt', 'w', encoding='cp437', errors = 'ignore') as store:
            for k in store_data:
                #write key, value
                tup = k + ":"+ store_data[k]
                if len(tup)>3:
                    store.writelines(tup+'\n')
        return 'STORED'

    def get_command(self, key):
        # print("execute get command for key ", key)

        with open('store.txt','r', encoding='cp437', errors = 'ignore') as store:
            store_data = {}
            reader = store.readlines()
            for line in reader:
                #key and value separated by first comma
                tmp = line.find(':')
                #line[0] is key, line[1] is value
                store_data[line[:tmp]] = line[tmp+1:]

            value = store_data[key]
            #return value
            return_message = "%s"%(value)
        return return_message


    #method to define thread actions
    def run(self):
        global concurrent_clients
        return_message = ''
        while True:
            data = self.client_socket.recv(recvSize)
            data = data.decode()
            # print("data is ", data)
            if data == "DONE":
                # print("%s disconnected\n"%self.client_addr[1])
                concurrent_clients -= 1
                break
            message = data.lower().split('--')
            if message[0] == 'set':
                #Acquire write lock
                file_lock.acquire_write()
                print("write lock acquired by client %s"%self.client_addr[1])

                #[set, key, value]
                #['set', 'mapper_id', 'list of tuples']
                key, value= message[1], message[2]
                return_message = self.set_command(key, value)
                self.client_socket.sendall(bytes(return_message, 'utf-8'))
                
                #release lock
                file_lock.release_write()
                print("write lock released by client %s\n"%self.client_addr[1])
        
                
            elif message[0] == "get":

                #Acquire read_lock
                file_lock.acquire_read()

                key = message[1]
                return_message = self.get_command(key)
                self.client_socket.sendall(bytes(return_message, 'utf-8'))

                #release read_lock
                file_lock.release_read()

                


#Storing format -
# key, value
server_PORT = storeCfg['port']

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', server_PORT))

file_lock = ReadWriteLock()

print("server bound to port")

global concurrent_clients 
concurrent_clients = 0
while True:
    print("store server is listening")
    recvSize = int(cfg['recvBufferSize'])
    server_socket.listen(5)
    client_socket, client_addr = server_socket.accept()
    new_thread = Client(client_addr, client_socket)
    concurrent_clients += 1
    
    new_thread.start()
    # print("number of concurrent clients : ",concurrent_clients)
server_socket.close()



#RESOURCES
# https://www.oreilly.com/library/view/python-cookbook/0596001673/ch06s04.html