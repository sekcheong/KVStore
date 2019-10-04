from libc.stdlib cimport malloc
from libc.string cimport strcpy, strlen
from Queue import Queue
from threading import Thread
import socket
import random
import json

primary_server = -1
servers = []

cdef public int kv739_init(char **connection_servers, int num_servers):
    cdef char **srvs = connection_servers
    cdef int i

    # Establish socket connection to each of the servers
    success = 0
    for i in range (0,num_servers):
        server = srvs[i][:]
        servers.append(server)
        try:
            sock = socket.socket()
            sock.connect((server.split(":")[0], int(server.split(":")[1])))
            success = success + 1
        except socket.error, exc:
            print "Caught exception socket.error : %s" % exc
        
    if(success < num_servers):
        print "Could not connect to all servers in init"
        cleanup(servers)
        return -1

    # Pick one server to communicate to as a primary. We fallback to secondaries only in event of failure
    primary_server = random.randint(0, num_servers-1)
    print("Assigning primary server")
    return 0

cdef public int kv739_shutdown():
    cleanup(servers)
    return -1

cdef public int kv739_get(char * key, char * value):
    status, val = getValueForKey(key, primary_server)
    strcpy(value, val)
    return status

cdef public int kv739_put(char * key, char * value, char * old_value):
    status, oldval = setValueForKey(key,value, primary_server)
    strcpy(old_value, oldval)
    return status

def cleanup(servers):
    primary_server = -1
    servers = []

def get_value_worker(sock, queue, data, worker_num):
    try:
        sock.sendall(data + "\n")
        response = sock.recv(2048)
        if response is not None:
            json_response = json.loads(response)
            if json_response["status"] == "success":
                queue.put(json_response["value"])
    except socket.error, msg:
        print("Worker " + str(worker_num) + " failed to get a value!")
        print "Couldnt connect with the socket-server: "
    except ValueError:
        print("ValueError during get...")
    finally:
        sock.close()

def getValueForKey(key, primary_server):
    values_queue = Queue()
    data = {}
    data['operation'] = 'GET'
    data['key'] = key
    json_data = json.dumps(data)
    # Attempt to send request to primary server
    threads = []
    for i in range(len(servers)):
        server = servers[i]
        try:
            sock = socket.socket()
            sock.connect((server.split(":")[0], int(server.split(":")[1]))) 
            t = Thread(target=get_value_worker(sock, values_queue,
                                          json_data, i))
            t.start()
            threads.append(t)
        except socket.error:
            print("Could not connect to server ", server)
    for thread in threads:
        thread.join()
    value_count = {}
    response_status = -1
    response_value = "\0"
    #print('Quorum for key ' + str(key))
    if not values_queue.empty():
        while not values_queue.empty():
            value = values_queue.get()
            if value in value_count:
                value_count[value] = value_count[value] + 1
            else:
                value_count[value] = 1
        response_status = 0
        max_count = -1
        max_value = ""
        for value, count in value_count.items():
            if count > max_count:
                max_count = count
                max_value = value
        response_value = max_value
        if response_value is None:
            response_value = "\0"
            response_status = 1
    return response_status, response_value

def setValueForKey(key, value, primary_server):
    data = {}
    data['operation'] = 'PUT'
    data['key'] = key
    data['value'] = value
    #print(data)
    json_data = json.dumps(data)
    failovers = 0
    sock = socket.socket()
    while failovers < len(servers):
        try:
            server = servers[primary_server]
            sock.connect((server.split(":")[0], int(server.split(":")[1])))
            sock.sendall(json_data + "\n")
            break
        except socket.error, msg:
            print "Couldnt connect with the socket-server: initiating fail over" % msg
            primary_server = (primary_server + 1) % len(servers)
            print "New primary server is " % primary_server
            failovers = failovers + 1
            sock.close()
    try:        
        response = sock.recv(2048)
        json_response = json.loads(response)
        old_value = "\0"
        response_status = -1
        if json_response["status"] == "success":
            old_value = json_response["value"]
            if old_value is None:
                old_value = "\0"
                response_status = 1
            else:
                response_status = 0
        return response_status, old_value
    except socket.error, msg:
        print "Exception. Returning empty"
        return -1, "\0"
    finally:
        sock.close()
