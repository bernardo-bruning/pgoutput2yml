build: main.c options.c
	@mkdir -p bin
	gcc main.c options.c -o bin/cdc2yml -I/usr/include/postgresql -lpq 

clean:
	@rm -R bin
