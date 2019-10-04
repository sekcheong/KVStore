#include <Python.h>
#include "client.h"
#include <stdio.h>
#include <string.h>
#include <semaphore.h>
#include <unistd.h>
#include <fcntl.h>

#define SERVER_LOC_SIZE 50
#define MAX_VAL_SIZE 2048

#ifndef KVSTORE_SIZE
    #define KVSTORE_SIZE 100000 // In terms of number of keys
#endif

#ifndef SLEEP_TIME_US
    #define SLEEP_TIME_US 0 // Sleep time in microseconds
#endif

char *rand_string(char *str, size_t size)
{
    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJK.1234567890";
    if (size) {
        --size;
        for (size_t n = 0; n < size; n++) {
            int key = rand() % (int) (sizeof charset - 1);
            str[n] = charset[key];
        }
        str[size] = '\0';
    }
    return str;
}

float get_time_elapsed_sec(struct timeval tv1, struct timeval tv2) {
    struct timeval tvdiff = { tv2.tv_sec - tv1.tv_sec, tv2.tv_usec - tv1.tv_usec };
    if (tvdiff.tv_usec < 0) { tvdiff.tv_usec += 1000000; tvdiff.tv_sec -= 1; }
    return tvdiff.tv_sec + tvdiff.tv_usec/(1000.0*1000.0);
}

int main(int argc, char *argv[])
{
    Py_Initialize();
    initclient();
    // Now generate KVSTORE_SIZE random keys
    char **keys;
    keys = (char**) malloc(KVSTORE_SIZE*sizeof(char*));
    for(int i=0;i<KVSTORE_SIZE;i++) {
        keys[i] = (char*)malloc(MAX_VAL_SIZE*sizeof(char));
        rand_string(keys[i], 128);
    }

    // Put seed values for KVSTORE_SIZE random keys
    char **values;
    values = (char**) malloc(KVSTORE_SIZE*sizeof(char*));
    for(int i=0;i<KVSTORE_SIZE;i++) {
        values[i] = (char*)malloc(MAX_VAL_SIZE*sizeof(char));
        rand_string(values[i], 512);
    }

    sem_t *sem1 = sem_open("kvstore1", O_CREAT, S_IRUSR | S_IWUSR, 0);
    sem_t *sem2 = sem_open("kvstore2", O_CREAT, S_IRUSR | S_IWUSR, 0);

    int pid = fork();
    if(pid == 0) {
        // child process
	char **servers;
        int num_servers = 1;
        servers = (char**)malloc(num_servers*sizeof(char*));
        for(int i=0;i<num_servers;i++) {
            servers[i] = (char*)malloc(SERVER_LOC_SIZE*sizeof(char));
        }
        strcpy(servers[0],"localhost:8004");
        int ret = kv739_init(servers, num_servers);
	if(ret == -1) {
	    exit(0);
	}
	int errors = 0;
	char *old_val;
        old_val = (char*) malloc(MAX_VAL_SIZE * sizeof(char));
	for(int i=0;i<KVSTORE_SIZE;i++) {
	   int value;
	   sem_wait(sem1);
           usleep(SLEEP_TIME_US);
           ret = kv739_get(keys[i], old_val);
           assert(ret == 1); // there should be no failure
           if(strcmp(old_val, values[i]) != 0) {
               errors++;
           }
	   sem_post(sem2);
       }
       printf("Errors in putting value for same key: %f percent", (errors*100.0)/KVSTORE_SIZE);
    } else {
	// parent process
	char **servers;
	int num_servers = 1;
        servers = (char**)malloc(num_servers*sizeof(char*));
        for(int i=0;i<num_servers;i++) {
            servers[i] = (char*)malloc(SERVER_LOC_SIZE*sizeof(char));
        }
	strcpy(servers[0],"localhost:8003");
	int ret = kv739_init(servers, num_servers);
	if (ret == -1) {
	    exit(0);
	}
	// Parent process puts the value
	char *old_val;
        old_val = (char*) malloc(MAX_VAL_SIZE * sizeof(char));
        for(int i=0;i<KVSTORE_SIZE;i++) {
            ret = kv739_put(keys[i], values[i], old_val);
	    sem_post(sem1);
            assert(ret == 1); // there should be no failure
            //assert(old_val[0] == '\0'); // old value should be NULL
	    sem_wait(sem2);
        }
    }
    Py_Finalize();
    return 0;
}
