/* Copyright 2023 <> */
#include <stdlib.h>
#include <string.h>

#include "load_balancer.h"

#define DEBUG 0

void print_server_memory(server_memory *s);

unsigned int hash_function_servers(void *a) {
    unsigned int uint_a = *((unsigned int *)a);

    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = (uint_a >> 16u) ^ uint_a;
    return uint_a;
}

unsigned int hash_function_key(void *a) {
    unsigned char *puchar_a = (unsigned char *)a;
    unsigned int hash = 5381;
    int c;

    while ((c = *puchar_a++))
        hash = ((hash << 5u) + hash) + c;

    return hash;
}

typedef struct sv {
    int id;
    unsigned int hash;
    server_memory *server_mem;
} server_node;

server_node *create_server(int server_id, unsigned int hash) {
    server_node *server = (server_node *)malloc(sizeof(server_node));
    DIE(server == NULL, "error: server list memory allocation");
    server->id = server_id;
    server->hash = hash;
    server->server_mem = init_server_memory();
    return server;
}

void free_server(server_node *s) {
    if (s) {
        free_server_memory(s->server_mem);
        free(s);
    }
}

void print_server(server_node *s) {
    printf("SERVER ID: %d\n", s->id);
    printf("SERVER HASH: %u\n", s->hash);
    print_server_memory(s->server_mem);
    printf("\n");
}

typedef struct load_balancer {
    unsigned int num_servers;
    server_node **servers;
} load_balancer;



load_balancer *init_load_balancer() {
    load_balancer *balancer = (load_balancer *)malloc(sizeof(load_balancer));
    DIE(balancer == NULL, "error: balancer memory allocation");
    balancer->num_servers = 0;
    balancer->servers = NULL;
    return balancer;
}

unsigned int get_tag(int server_id, int replica_id) {
    return replica_id * 100000  + server_id;
}

unsigned int search_server(load_balancer *lb, unsigned int hash) {
    int left = 0, right = lb->num_servers - 1;
    while (left <= right) {
        int mid = (left + right) / 2;
        if (lb->servers[mid]->hash == hash)
            return mid;
        if (hash > lb->servers[mid]->hash)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return left;
}

void balance(load_balancer *lb, unsigned int server_i) {
    server_memory *old_server, *new_server;
    if (server_i == lb->num_servers)
        old_server = lb->servers[0]->server_mem;
    else
        old_server = lb->servers[server_i + 1]->server_mem;
    new_server = lb->servers[server_i]->server_mem;
    
    for (elem *e = old_server->products; e != NULL; e = old_server->products)
        if (e->hash < lb->servers[server_i]->hash) {
            server_store(new_server, e->key, e->value);
            server_remove(old_server, e->key);
        }
        else break;
}

void add_server(load_balancer *lb, server_node *server) {
    if (DEBUG)
        printf("ADDING SERVER %d with hash %u\n", server->id, server->hash);
    unsigned int server_i = search_server(lb, server->hash);
    if (DEBUG)
        printf("SEARCH COMPLETE. i: %u\n", server_i);
    if (server_i >= lb->num_servers)
        lb->servers[server_i] = server;
    else {
        if (lb->servers[server_i]->hash == server->hash)
            if (server->id > lb->servers[server_i]->id)
                server_i++;
        for (unsigned int i = lb->num_servers; i > server_i; i--)
            lb->servers[i] = lb->servers[i - 1];
        lb->servers[server_i] = server;
    }
    if (DEBUG)
        printf("\t\t\t\t\t\tBEFORE BALANCING:\n");
    if (DEBUG)
        for (unsigned int i = 0; i < lb->num_servers; i++)
            print_server(lb->servers[i]);
    if (DEBUG)
        printf("BALANCING: %d\n", server_i);
    balance(lb, server_i);
    lb->num_servers++;
    if (DEBUG)
        printf("BAL COMPLETE!    SERVERS: ");
    if (DEBUG)
        for (unsigned int i = 0; i < lb->num_servers; i++)
            printf("(%d,%d) ", i, lb->servers[i]->id);
    if (DEBUG)
        printf("\n");
}

void loader_add_server(load_balancer *main, int server_id) {
    if (main->num_servers == 0)
        main->servers = (server_node**)malloc(3 * sizeof(server_node*));
    else
        main->servers = (server_node**)realloc(main->servers,
                            (main->num_servers + 3) * sizeof(server_node *));
    for (int i = 0; i < 3; i++) {
        unsigned int tag = get_tag(server_id, i);
        unsigned int hash = hash_function_servers(&tag);
        // printf("created server %d with tag: %u and hash: %u\n", i, tag, hash);
        server_node *server = create_server(server_id, hash);
        add_server(main, server);
    }
}

void remove_server(load_balancer *main, int i) {
    server_memory *old_server = main->servers[i]->server_mem;
    server_memory *new_server = main->servers[(i+1)%main->num_servers]->server_mem;
    for (elem *e = old_server->products; e != NULL; e = old_server->products) {
        server_store(new_server, e->key, e->value);
        server_remove(old_server, e->key);
    }
    free_server(main->servers[i]);
}

void loader_remove_server(load_balancer *main, int server_id) {
    if (DEBUG) {
        printf("REMOVING %d from SERVERS:\n", server_id);
        for (unsigned int i = 0; i < main->num_servers; i++) {
            printf("\ti:%u, id: %d, hash: %u\n", i, main->servers[i]->id, main->servers[i]->hash);
        }
    }

    for (unsigned int i = 0; i < main->num_servers; i++) {
        if (main->servers[i]->id == server_id) {
            // printf("REMOVING SERVER AT POS %u\n", i);
            remove_server(main, i);
            // printf("DONE here\n");
            for (unsigned int j = i; j < main->num_servers - 1; j++)
                main->servers[j] = main->servers[j + 1];
            // printf("DONE REMOVING SERVER\n");
            main->num_servers--;
            i--;
            // printf("SERVERS:\n");
            // for (unsigned int i = 0; i < main->num_servers; i++) {
            //     printf("\t\ti:%u, id: %d, hash: %u\n", i, main->servers[i]->id, main->servers[i]->hash);
            // }
            // for (unsigned int i = 0; i < main->num_servers; i++)
            //     print_server(main->servers[i]);
            // printf("\n\n\n");
        }
    }
    if (DEBUG) {
         printf("DONE REMOVING SERVERS\n");
        for (unsigned int i = 0; i < main->num_servers; i++)
            print_server(main->servers[i]);
        printf("\n\n\n\n\n\n\n\n");
    }
}

void loader_store(load_balancer *main, char *key, char *value, int *server_id) {
    unsigned int hash = hash_function_key(key);

    if (DEBUG) {
        printf("TRING TO STORE %s with hash %u...\n", key, hash);
        for (unsigned int i = 0; i < main->num_servers; i++)
            print_server(main->servers[i]);
    }
    for (unsigned int i = 0; i < main->num_servers; i++)
        if (main->servers[i]->hash > hash) {
            server_store(main->servers[i]->server_mem, key, value);
            *server_id = main->servers[i]->id;
            if (DEBUG) {
                printf("\t\t\t\t\tAFTER:\n");
                for (unsigned int i = 0; i < main->num_servers; i++)
                    print_server(main->servers[i]);
            }
            return;
        }
    server_store(main->servers[0]->server_mem, key, value);
    *server_id = main->servers[0]->id;
    if (DEBUG) {
    printf("\t\tAFTER:\n");
    for (unsigned int i = 0; i < main->num_servers; i++)
        print_server(main->servers[i]);
    }
}

char *loader_retrieve(load_balancer *main, char *key, int *server_id) {
    unsigned int hash = hash_function_key(key);
    char *res = NULL;
    for (unsigned int i = 0; i < main->num_servers; i++)
        if (main->servers[i]->hash > hash) {
            res = server_retrieve(main->servers[i]->server_mem, key);
            if (DEBUG)
                printf("DEBUG: res = %s\n", res);
            *server_id = main->servers[i]->id;
            return res;
        }
    res = server_retrieve(main->servers[0]->server_mem, key);
    if (DEBUG)
        printf("DEBUG: res = %s\n", res);
    *server_id = main->servers[0]->id;
    return res;
}

void free_load_balancer(load_balancer *main) {
    DIE(main == NULL, "error: free a non existing load balancer");
    for (unsigned int i = 0; i < main->num_servers; i++)
        free_server(main->servers[i]);
    free(main->servers);
    free(main);
}
