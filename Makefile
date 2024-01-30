build: main.c
	gcc main.c -o cdc -I/usr/include/postgresql -lpq 
