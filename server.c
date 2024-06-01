/* Copyright 2023 <> */
#include <stdlib.h>
#include <string.h>

#include "server.h"
#include "utils.h"

unsigned int hash_function_key(void *a);

elem *create_elem(char *key, char *value) {
	elem *e = (elem *)malloc(sizeof(elem));
	DIE(e == NULL, "error: elem allocation");
	e->key = (char *)malloc(strlen(key) + 1);
	DIE(e == NULL, "error: elem key allocation");
	e->value = (char *)malloc(strlen(value) + 1);
	DIE(e == NULL, "error: elem value allocation");
	strcpy(e->key, key);
	strcpy(e->value, value);
	e->hash = hash_function_key(key);
	e->next = NULL;
	return e;
}

void free_elem(elem *e) {
	if (e) {
		free(e->key);
		free(e->value);
		free(e);
	}
}

void print_product(elem *e) {
	printf("\tKEY: %s\t", e->key);
	printf("\tVALUE: %s\t", e->value);
	printf("\tHASH: %u\n", e->hash);
}

void print_server_memory(server_memory *s) {
	printf("PRODUCTS:\n");
	for (elem *e = s->products; e; e = e->next) {
		print_product(e);
		printf("\n");
	}
}

server_memory *init_server_memory()
{
	server_memory *server = (server_memory *)malloc(sizeof(server_memory));
	DIE(server == NULL, "error: server memory allocation");
	server->products = NULL;
	return server;
}

void server_store(server_memory *server, char *key, char *value) {
	DIE(server == NULL, "error: add to a non existing server");
	elem *product = create_elem(key, value);
	if (server->products == NULL)
		server->products = product;
	else {
		if (product->hash < server->products->hash) {
			product->next = server->products;
			server->products = product;
		} else {
			elem *e, *last = server->products;
			for (e = last->next; e && (product->hash > e->hash);
													last = e, e = e->next);
			product->next = e;
			last->next = product;
		}
	}
}

char *server_retrieve(server_memory *server, char *key) {
	DIE(server == NULL, "error: retrieve from a non existing server");
	// printf("CALLED here...%s\n", key);
	// printf("%s\n", server->products->value);
	unsigned int hash = hash_function_key(key);
	elem *e;
	for (e = server->products; e && e->hash < hash; e = e->next);
		// printf("DEBUG: e: %s\n", e->value);
	if (e && strcmp(e->key, key) == 0)
		return e->value;
	return NULL;
}

void server_remove(server_memory *server, char *key) {
	DIE(server == NULL, "error: remove from a non existing server");
	unsigned int hash = hash_function_key(key);
	if (server->products != NULL) {
		elem *tmp = server->products;
		if (key == tmp->key) {
			server->products = tmp->next;
			free_elem(tmp);
		} else {
			elem *e;
			for (e = tmp->next; e && e->hash < hash; tmp = tmp->next, e = e->next);
			if (e) {
				tmp->next = e->next;
				free_elem(e);
			}
		}
	}
}

void free_server_memory(server_memory *server) {
	DIE(server == NULL, "error: free a non existing server");
	while (server->products) {
		elem *e = server->products;
		server->products = e->next;
		free_elem(e);
	}
	free(server);
}
