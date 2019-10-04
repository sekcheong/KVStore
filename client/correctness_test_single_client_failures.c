#include <Python.h>
#include "client.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

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
   
    int num_servers = argc-2; 
    // Test code starts here
    char **servers;
    servers = (char**)malloc(num_servers*sizeof(char*));

    for(int i=0;i<num_servers;i++) {
	servers[i] = (char*)malloc(SERVER_LOC_SIZE*sizeof(char));
	strcpy(servers[i], argv[i+1]);
    }

    int ret = kv739_init(servers, num_servers);
    printf("Kv_init %d\n", ret);

    if(ret == -1){
	    exit(0);
    }
    
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

    // Insert the KV pairs
    char *old_val;
    old_val = (char*) malloc(MAX_VAL_SIZE * sizeof(char));
    for(int i=0;i<KVSTORE_SIZE;i++) {
       ret = kv739_put(keys[i], values[i], old_val);
       assert(ret == 1); // there should be no failure
       assert(old_val[0] == '\0'); // old value should be NULL
    }

    // Allow for eventual consistency to play out its magic
    usleep(SLEEP_TIME_US);
    
    // Check tolerance towards one failure - kill one process
    char *system_command = (char*) malloc(100*sizeof(char));
    strcpy(system_command, "kill -9 ");
    strcat(system_command, argv[argc-1]);
    ret = system(system_command);

    // Wait for 500 ms for kill to take effect
    usleep(500*1000);

    // Check if we are able to read back same values
    struct timeval tv1, tv2;
    int errors = 0;
    int num_iterations = 10;
    errors = 0;
    gettimeofday(&tv1, NULL);
    for(int i=0;i<KVSTORE_SIZE;i++) {
        ret = kv739_get(keys[i], old_val);
        assert(ret == 1); // there should be no failure
        if(strcmp(old_val, values[i]) != 0) {
            errors++;
        }
    }
    gettimeofday(&tv2, NULL);
    printf("Error in reading back same value: %f percent", (errors*100.0)/KVSTORE_SIZE);
    printf("Read throughput = %f\n keys/sec", (KVSTORE_SIZE * 1.0)/get_time_elapsed_sec(tv1,tv2));

    // Now restart the killed server and check what happens
    strcpy(system_command, "bash ../server/start_servers.sh -r -p 8005");
    ret = system(system_command);

    // Wait for 5000 ms for kill to take effect
    usleep(5000*1000);

    // Check if we are able to read back same values
    errors = 0;
    gettimeofday(&tv1, NULL);
    for(int i=0;i<KVSTORE_SIZE;i++) {
        ret = kv739_get(keys[i], old_val);
        assert(ret == 1); // there should be no failure
        if(strcmp(old_val, values[i]) != 0) {
            errors++;
        }
    }
    gettimeofday(&tv2, NULL);
    printf("Error in reading back same value: %f percent", (errors*100.0)/KVSTORE_SIZE);
    printf("Read throughput = %f\n keys/sec", (KVSTORE_SIZE * 1.0)/get_time_elapsed_sec(tv1,tv2));

    Py_Finalize();
    return 0;
} 
