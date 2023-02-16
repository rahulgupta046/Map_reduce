#This is the master code
import yaml
import os
import subprocess
import socket
import time
import threading


#read config file
with open('config.yml', 'r') as yml:
    cfg = yaml.load(yml, Loader= yaml.FullLoader)
masterCfg = cfg['master']

PORT = masterCfg['port']
application = masterCfg['application']

mapperCount = masterCfg['mapperCount']
mapperDir = masterCfg['mapperDir']

reducerCount = masterCfg['reducerCount']
reducerDir = masterCfg['reducerDir']

inputFile = masterCfg['inputFile']
outputFile = masterCfg['outputFile']

hostname = socket.gethostname()
server_IP = socket.gethostbyname(hostname)


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', PORT))
print('master is listening')
server_socket.listen(10)


#Hash function
def hash(word):
    return ord(word.upper()[0])%reducerCount

#GROUPBY function
def groupby():
    for f in mapOutputFiles:
        with open(f, 'r', encoding='cp437', errors = 'ignore') as input:
            for idx, line in enumerate(input):
                line.replace('\n', '')
                word, count = line.split(',')
                id = hash(word)
                with open(redInputFiles[id], 'a', encoding='cp437', errors = 'ignore') as output:
                    ln = '%s,%s'%(word,count)
                    output.write(ln)

def combine():
    for f in redOutputFiles:
        with open(f, 'r', encoding='cp437', errors = 'ignore') as file:
            data = file.read()
        with open(outputFile, 'a', encoding='cp437', errors = 'ignore') as file:
            file.write(data)

#function to set up mapper
#returns 2 lists with input and output file paths
def setup_mapper(count, dir):
    input, output = [], []
    for i in range(1, count+1):
        input.append(os.path.join(dir, 'mapper%s_input.txt'%(i)))
        output.append(os.path.join(dir, 'mapper%s_output.txt'%(i)))
    return input, output

#function to set up reducer
#returns 2 lists with input and output file paths
def setup_reducer(count, dir):
    input, output = [], []
    for i in range(1, count+1):
        input.append(os.path.join(dir, 'reducer%s_input.txt'%(i)))
        output.append(os.path.join(dir, 'reducer%s_output.txt'%(i)))
    return input, output

#function to split input file for mappers
def split_file(mapInputFiles, mapperCount):
    with open(inputFile,'r', encoding='cp437', errors = 'ignore') as file:
        for idx, line in enumerate(file):
            chunk = idx % mapperCount
            with open(mapInputFiles[chunk], 'a', encoding='cp437', errors = 'ignore') as tmp:
                tmp.write(line)

mapInputFiles, mapOutputFiles = setup_mapper(mapperCount, mapperDir)
redInputFiles, redOutputFiles = setup_reducer(reducerCount, reducerDir)

#remove if files exists
for i in range(mapperCount):
    if os.path.exists(mapInputFiles[i]):
        os.remove(mapInputFiles[i])
    if os.path.exists(mapOutputFiles[i]):
        os.remove(mapOutputFiles[i])

for i in range(reducerCount):
    if os.path.exists(redInputFiles[i]):
        os.remove(redInputFiles[i])
    if os.path.exists(redOutputFiles[i]):
        os.remove(redOutputFiles[i])

if os.path.exists(outputFile):
        os.remove(outputFile)

#Split Input Files
print('SPLITTING INPUT FOR MAPPERS')
split_file(mapInputFiles, mapperCount)

class Mapper(threading.Thread):
    def __init__(self, client_addr, client_socket, filepaths):
        threading.Thread.__init__(self)
        self.addr = client_addr
        self.socket = client_socket
        self.input = filepaths[0]
        self.output = filepaths[1]
        print("mapper ", client_addr, " is working")
    
    def run(self):
        # global mappers
        self.socket.recv(1024)

        message = '%s,%s'%(self.input, self.output)
        self.socket.send(bytes(message, 'utf-8'))

        done = self.socket.recv(1024)
        if done.decode() != 'DONE':
            raise Exception()
        print('mapper ', self.addr, ' is done')
        # mappers -= 1

class Reducer(threading.Thread):
    def __init__(self, client_addr, client_socket, filepaths):
        threading.Thread.__init__(self)
        self.addr = client_addr
        self.socket = client_socket
        self.input = filepaths[0]
        self.output = filepaths[1]
        print("reducer ", client_addr, " is working")
    
    def run(self):
        # global reducers
        self.socket.recv(1024)

        message = '%s,%s'%(self.input, self.output)
        self.socket.send(bytes(message, 'utf-8'))

        done = self.socket.recv(1024)
        if done.decode() == '':
            raise Exception()
        print('reducer ', self.addr, ' is %s'%(done.decode()))
        # reducers -= 1


def Mapping():
    print('.....STARTING MAPPERS...........')
    for i in range(mapperCount):
        mapper_path = os.path.join(os.path.join(mapperDir,'mapper.py'))
        subprocess.Popen('python ' + mapper_path, shell = True)

    global mappers 
    mappers = 0
    for i in range(mapperCount):
        client_socket, client_addr = server_socket.accept()
        new_thread = Mapper(client_addr, client_socket, (mapInputFiles[i], mapOutputFiles[i]))
        mappers += 1
        new_thread.start()
    new_thread.join()
    # while mappers != 0:
    #     time.sleep(2)

flag = 0
while flag == 0:
    try:
        Mapping()
        print('ALL MAPPERS DONE')
        flag = 1
    except Exception as e:
        print('Mapper failed...... restarting map phase')
        #clear mapper outputs
        for i in range(mapperCount):
            if os.path.exists(mapOutputFiles[i]):
                os.remove(mapOutputFiles[i])


print('GROUPBY PHASE TO CREATE REDUCER INPUTS')
groupby()

print("GROUPBY DONE")

def Reducing():
    print('.....STARTING REDUCERS...........')
    for i in range(reducerCount):
        reducer_path = os.path.join(os.path.join(reducerDir,'reducer.py'))
        subprocess.Popen('python ' + reducer_path, shell = True)

    global reducers 
    reducers = 0
    for i in range(reducerCount):
        client_socket, client_addr = server_socket.accept()
        new_thread = Reducer(client_addr, client_socket, (redInputFiles[i], redOutputFiles[i]))
        reducers += 1
        new_thread.start()
    new_thread.join()
    # while reducers != 0:
    #     time.sleep(2)


flag = 0
while flag == 0:
    try:
        Reducing()
        print("ALL REDUCERS DONE")
        flag = 1
    except Exception as e:
        print('Reducer failed...... restarting Reducing phase')
        #clear reducer outputs
        for i in range(reducerCount):
            if os.path.exists(redOutputFiles[i]):
                os.remove(redOutputFiles[i])



#combine output
combine()
print("DONE")
server_socket.close()