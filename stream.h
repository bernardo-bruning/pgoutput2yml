#pragma once

#include <stdlib.h>
#include <arpa/inet.h>
#include <string.h>

typedef struct {
  char* value;
} stream_t;

stream_t *create_buffer(char* value, size_t size);
void delete_buffer(stream_t* buffer);
int8_t read_int8(stream_t* buffer);
int16_t read_int16(stream_t* buffer);
int32_t read_int32(stream_t* buffer);
char read_char(stream_t* buffer);
char* read_string(stream_t* buffer);


