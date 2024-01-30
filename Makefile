build: main.c
	@mkdir -p bin
	gcc main.c -o bin/cdc2yml -I/usr/include/postgresql -lpq 

clean:
	@rm -R bin
