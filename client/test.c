#include <Python.h>
#include "client.h"
#include <stdio.h>
#include <string.h>

#define SERVER_LOC_SIZE 50
#define NUM_SERVERS 2
#define MAX_VAL_SIZE 2048

int main()
{
    Py_Initialize();
    initclient();
    
    // Test code starts here
    char **servers;
    servers = (char**)malloc(NUM_SERVERS*sizeof(char*));

    for(int i=0;i<NUM_SERVERS;i++) {
	servers[i] = (char*)malloc(SERVER_LOC_SIZE*sizeof(char));
    }
    strcpy(servers[0], "localhost:8004");
    strcpy(servers[1], "localhost:8005");

    int ret = kv739_init(servers, NUM_SERVERS);
    printf("Kv_init %d\n", ret);

    if(ret == -1){
	    exit(0);
    }
    char *val;

    val = (char*) malloc(MAX_VAL_SIZE * sizeof(char));
    ret = kv739_get("100", val);
    printf("GET %s\n", val);

    ret = kv739_put("100", "danish", val);
    printf("PUT OLD=%s\n", val);

    ret = kv739_get("100", val);
    printf("GET %s\n", val);

    ret = kv739_get("200", val);
    printf("GET %s\n", val);

    ret = kv739_put("200", "arjun", val);
    printf("PUT OLD=%s\n", val);

    ret = kv739_get("200", val);
    printf("GET %s\n", val);

    Py_Finalize();
    return 0;
}
