import socket
import os
import yaml
import random
import sys
import time 
def hash(word):
    return ord(word.upper()[1])%reducerCount == id-1 
def wc_parse(input, data):
    red_in = data
    #replace space 
    input.replace(' ', '')
    while len(input)>1:
        ob = input.find('(')
        cb = input.find(')')
        tup = input[ob+1:cb]
        k,v = tup.split(',')
        if hash(k):
            if k not in red_in:
                red_in[k] = int(v)
            else:
                red_in[k] += int(v)
        input = input[cb+1:].strip()
    return red_in

def ii_parse(input, data):
    red_in = data
    #replace space 
    input.replace(' ', '')
    while len(input)>1:
        ob = input.find('(')
        cb = input.find(')')
        tup = input[ob+1:cb]
        k,v = tup.split(',')
        if hash(k):
            if k not in red_in:
                red_in[k] = [v]
            else:
                if v not in red_in[k]:
                    red_in[k].append(v)
        input = input[cb+1:].strip()
    return red_in

def reduce(outputFile, id):
    data = {}
    #read input from store 
    for i in range(1, mapperCount+1):
        #read individual mapper outputs as reducer input
        map_key = 'mapper_%s'%(i)
        msg = 'get--%s'%(map_key)
        sock.send(bytes(msg, 'utf-8'))
        time.sleep(1)
        red_input_val = sock.recv(recvSize)

        #input is in format - "[(k,v),(k,v).....]"
        #parse input
        #returns a dictionary of word and count
        if 'word_count' in app:
            data = wc_parse(red_input_val.decode(), data)
        else:
            data = ii_parse(red_input_val.decode(), data)

    with open(outputFile, 'w', encoding='cp437', errors = 'ignore') as output:
        for k in data:
            output.writelines('%s,%s\n'%(k,data[k]))


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

config_path = os.path.join(os.getcwd(),'config.yml')
with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

app = cfg['application']
PORT = cfg['store']['port']
IP  = cfg['store']['ip']
recvSize = int(cfg['recvBufferSize'])
sock.connect((IP, PORT))

reducerCount = cfg['master']['reducerCount']
mapperCount = cfg['master']['mapperCount']

arg = sys.argv
output, id = arg[1], int(arg[2])
reduce(output.strip(), id)

fail = random.randint(1,10)
#fail at 1, 2  => 10% chance of failing
#set status with mapper_{id}_status = True of False
fail_thresh = cfg['fault']
if fail<=fail_thresh:
    #fail condition
    msg = 'set--reducer_%s_status--%s'%(id,'False')
else:
    msg = 'set--reducer_%s_status--%s'%(id,'True')
    
print('REDUCER_%s status set'%id)
sock.send(bytes(msg, 'utf-8'))
time.sleep(1)
sock.send(bytes("DONE", 'utf-8'))
time.sleep(1)
sock.close()