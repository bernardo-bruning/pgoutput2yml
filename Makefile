build: ./src/main.c ./src/options.c ./src/stream.c
	@mkdir -p bin
	gcc ./src/main.c ./src/options.c ./src/stream.c -o bin/pgoutput2yml -I/usr/include/postgresql -lpq -DDEBUG_ON

clean:
	@rm -R bin
