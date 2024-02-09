#include "stream.h"

stream_t *create_buffer(char* value, size_t size) {
  stream_t* buffer = (stream_t*)malloc(sizeof(stream_t));
  buffer->value = value;
  return buffer;
}

void delete_buffer(stream_t* buffer) {
  free(buffer);
}

int8_t read_int8(stream_t* buffer) {
  int8_t value = buffer->value[0];
  buffer->value += 1;
  return value;
}

int16_t read_int16(stream_t* buffer) {
  int16_t value = htons(*(int16_t*)(buffer->value));
  buffer->value += 2;
  return value;
}

int32_t read_int32(stream_t* buffer) {
  int32_t value = htonl(*(int32_t*)(buffer->value));
  buffer->value += 4;
  return value;
}

char read_char(stream_t* buffer) {
  char value = buffer->value[0];
  buffer->value += 1;
  return value;
}

char* read_string(stream_t* buffer) {
  char* value = buffer->value;
  size_t size = strlen(buffer->value)+1;
  buffer->value += size;
  return value;
}


