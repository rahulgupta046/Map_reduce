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
storeCfg = cfg['store']


PORT = masterCfg['port']
application = cfg['application']


mapperCount = masterCfg['mapperCount']
mapperDir = masterCfg['mapperDir']

reducerCount = masterCfg['reducerCount']
reducerDir = masterCfg['reducerDir']


inputFile = masterCfg['inputFile']
outputFile = masterCfg['outputFile']

#store
store_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
store_PORT = cfg['store']['port']
store_IP  = cfg['store']['ip']

store_socket.connect((store_IP, store_PORT))

print(store_IP, store_PORT)
print("CONFIG DONE ")


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

print('.....STARTING MAPPERS...........')
completed_mapper = []
#list to store processes
p = []
for i in range(mapperCount):
    mapper_path = os.path.join(os.path.join(mapperDir,'mapper.py'))
    #pass input file path and id as commandline arguments
    p.append(subprocess.Popen('python %s %s %s %s'%(mapper_path,mapInputFiles[i], mapOutputFiles[i],i+1), shell = True))
for i in p:
    i.communicate()

#status is stored as - 'mapper_{id}_status'
while len(completed_mapper) != mapperCount:
    p = []
    for i in range(mapperCount):
        if i not in completed_mapper:
            msg = 'get--mapper_%s_status'%(i+1)
            store_socket.send(bytes(msg, 'utf-8'))
            time.sleep(1)
            val = store_socket.recv(1024).decode().lower()
            if 'true' in val:
                #append indexes of failed mapper
                completed_mapper.append(i)
            else:
                print('mapper_%s failed, RESTARTING......'%(i+1))
                p.append(subprocess.Popen('python %s %s %s %s'%(mapper_path,mapInputFiles[i], mapOutputFiles[i],i+1), shell = True))
    for i in p:
        i.communicate()

print('.....MAPPERS DONE...........')

print('.....STARTING REDUCERS...........')
completed_reducer = []
#list to store processes
p = []
for i in range(reducerCount):
    reducer_path = os.path.join(os.path.join(reducerDir,'reducer.py'))
    #pass input file path and id as commandline arguments
    p.append(subprocess.Popen('python %s %s %s'%(reducer_path, redOutputFiles[i],i+1), shell = True))
for i in p:
    i.communicate()

#status is stored as - 'reducer_{id}_status'
while len(completed_reducer) != reducerCount:
    p = []
    for i in range(reducerCount):
        msg = 'get--mapper_%s_status'%(i+1)
        store_socket.send(bytes(msg, 'utf-8'))
        time.sleep(1)
        val = store_socket.recv(1024).decode().lower()
        if 'true' in val:
            #append indexes of failed mapper
            completed_reducer.append(i)
        else:
            'reducer_%s failed, RESTARTING......'%(i+1)
            p.append(subprocess.Popen('python %s %s %s'%(reducer_path, redOutputFiles[i],i+1), shell = True))
    for i in p:
        i.communicate()

print('.....REDUCERS DONE...........')

#combine output
combine()
print("DONE")
store_socket.close()