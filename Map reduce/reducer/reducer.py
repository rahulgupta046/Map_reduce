import socket
import os
import yaml
import random
def word_count(inputFile, outputFile):
    data = {}
    #read input file 
    with open(inputFile, 'r', encoding='cp437', errors = 'ignore') as input:
        for idx, line in enumerate(input):
            line.replace('\n', '')
            word, count = line.split(',')
            count = int(count)
            if word in data:
                data[word] += count
            else:
                data[word] = count

    with open(outputFile, 'a', encoding='cp437', errors = 'ignore') as output:
        for k,v in data.items():
            output.write('%s,%s\n'%(k,v))

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

config_path = os.path.join(os.getcwd(),'config.yml')
with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)

PORT = cfg['master']['port']
IP  = cfg['master']['ip']
sock.connect((IP, PORT))

sock.send(bytes("Working", 'utf-8'))

files = sock.recv(1024).decode()

input, output = files.split(',')
word_count(input.strip(), output.strip())

fail = random.randint(1,10)
#fail at 1, 2  => 10% chance of failing
if fail<1:
    sock.close()

sock.send(bytes("DONE", 'utf-8'))

sock.close()