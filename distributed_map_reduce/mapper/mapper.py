import string
import socket
import os
import yaml
import random 
import sys
import time

def word_count(id, inputFile, outputFile):
    #list of tuples for key value store
    value = []
    #read input file 
    with open(inputFile, 'r', encoding='cp437', errors = 'ignore') as input:
        data = input.read()
        data = data.translate(str.maketrans('', '', string.punctuation))
        for word in data.split():
            value.append((word,1))
            # with open(outputFile, 'a', encoding='cp437', errors = 'ignore') as output:
            #     output.write('%s,1\n'%(word))
    s_value = str(value).replace(' ', '')
    store_message = 'set--mapper_%s--%s'%(id,s_value)
    store_socket.sendall(bytes(store_message, 'utf-8'))
    time.sleep(1)
    store_socket.recv(1024)

def inverted_index(id, inputFile):
    value = []
    with open(inputFile, 'r', encoding='cp437', errors = 'ignore') as input:
        data = input.read()
        data = data.translate(str.maketrans('', '', string.punctuation))
        for word in data.split():
            if word not in value:
                value.append(word)
    value = [(word,'file_%s'%(id)) for word in value]    
    s_value = str(value).replace(' ', '')
    store_message = 'set--mapper_%s--%s'%(id,s_value)
    store_socket.sendall(bytes(store_message, 'utf-8'))
    time.sleep(1)
    store_socket.recv(1024)

config_path = os.path.join(os.getcwd(), 'config.yml')
with open(config_path, 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

#store
store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
store_PORT = cfg['store']['port']
store_IP  = cfg['store']['ip']
recvSize = cfg['recvBufferSize']
store_socket.connect((store_IP, store_PORT))

arg = sys.argv
input, output, id = arg[1], arg[2], int(arg[3])
print('MAPPER_%s IS RUNNING...........'%id)
app = cfg['application']
if 'word_count' in app:
    word_count(id, input.strip(), output.strip())
else:
    inverted_index(id, input.strip())

fail = random.randint(1,10)
#fail at 1, 2  => 10% chance of failing
#set status with mapper_{id}_status = True of False
fail_thresh = cfg['fault']
if fail<=fail_thresh:
    #fail condition
    msg = 'set--mapper_%s_status--%s'%(id,'False')
else:
    msg = 'set--mapper_%s_status--%s'%(id,'True')
    
print('MAPPER_%s status set'%id)
store_socket.send(bytes(msg, 'utf-8'))
time.sleep(1)
store_socket.send(bytes("DONE", 'utf-8'))
time.sleep(1)
store_socket.close()