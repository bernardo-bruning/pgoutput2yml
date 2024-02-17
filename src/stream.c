#include "stream.h"
#include <endian.h>

stream_t *create_stream(char* value, size_t size) {
  stream_t* stream = (stream_t*)malloc(sizeof(stream_t));
  stream->start = value;
  stream->current = value;
  return stream;
}

void delete_stream(stream_t* stream) {
  free(stream);
}

size_t stream_pos(stream_t* stream) {
  return stream->current - stream->start;
}

void skip_bytes(stream_t* stream, size_t size) {
  stream->current += size;
}

int8_t read_int8(stream_t* stream) {
  int8_t value = stream->current[0];
  stream->current += 1;
  return value;
}

int16_t read_int16(stream_t* stream) {
  int16_t value = be16toh(*(int16_t*)(stream->current));
  stream->current += 2;
  return value;
}

int32_t read_int32(stream_t* stream) {
  int32_t value = be32toh(*(int32_t*)(stream->current));
  stream->current += 4;
  return value;
}

int64_t read_int64(stream_t* stream) {
  int64_t value = be64toh(*(int64_t*)(stream->current));
  stream->current += 8;
  return value;
}

char read_char(stream_t* stream) {
  char value = stream->current[0];
  stream->current += 1;
  return value;
}

char* read_string(stream_t* stream) {
  char* value = stream->current;
  size_t size = strlen(stream->current)+1;
  stream->current += size;
  return value;
}

void write_int16(stream_t* stream, int16_t value){
  value = htobe16(value);
  (*(int16_t*)(stream->current)) = value;
  stream->current += 2;
}

void write_int32(stream_t* stream, int32_t value) {
  value = htobe32(value);
  (*(int32_t*)(stream->current)) = value;
  stream->current += 4;
}

void write_int64(stream_t* stream, int64_t value) {
  value = htobe64(value);
  (*(int64_t*)(stream->current)) = value;
  stream->current += 8;
}

void write_char(stream_t* stream, char value) {
  stream->current[0] = value;
  stream->current++;
}
