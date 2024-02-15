CC = gcc
SRC_FILES = ./src/options.c ./src/stream.c
TEST_FILES = ./tests/check.c
FLAGS = -lpq
FLAGS_TESTS = -lcheck -lm -lpthread -lrt -lsubunit 
DEFS = -DDEBUG_ON
INCLUDES = -I/usr/include/postgresql

all: check build

build: ./src/main.c ./src/options.c ./src/stream.c
	@mkdir -p bin
	$(CC) ./src/main.c $(SRC_FILES) -o bin/pgoutput2yml $(INCLUDES) $(FLAGS) $(DEFS)

clean:
	@rm -R bin

check:
	$(CC) $(TEST_FILES) $(SRC_FILES) -o bin/check $(INCLUDES) $(FLAGS_TESTS) $(FLAGS) $(DEFS) && ./bin/check
