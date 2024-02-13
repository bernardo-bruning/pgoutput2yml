build: main.c options.c stream.c
	@mkdir -p bin
	gcc main.c options.c stream.c -o bin/cdc2yml -I/usr/include/postgresql -lpq -DDEBUG_ON

clean:
	@rm -R bin
