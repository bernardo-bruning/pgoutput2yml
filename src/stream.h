#pragma once

#include <stdlib.h>
#include <string.h>

typedef struct {
  char* current;
  char* start;
} stream_t;

stream_t *create_stream(char* value, size_t size);
void delete_stream(stream_t* stream);
size_t stream_pos(stream_t* stream);
void skip_bytes(stream_t* stream, size_t size);

int8_t read_int8(stream_t* stream);
int16_t read_int16(stream_t* stream);
int32_t read_int32(stream_t* stream);
int64_t read_int64(stream_t* stream);
char read_char(stream_t* stream);
char* read_string(stream_t* stream);

void write_int8(stream_t* stream, int8_t value);
void write_int16(stream_t* stream, int16_t value);
void write_int32(stream_t* stream, int32_t value);
void write_int64(stream_t* stream, int64_t value);
void write_char(stream_t* stream, char value);

