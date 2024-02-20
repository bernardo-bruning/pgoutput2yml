#pragma once

#include <stdlib.h>
#include "stream.h"

enum Error { OK, FAILED };

typedef struct {
  int64_t lsn;
  int64_t transaction;
  int64_t timestamp;
} commit_t;

commit_t* parse_commit(stream_t *stream);
void delete_commit(commit_t* commit);

typedef struct {
  int64_t id;
  char* namespace;
  char* name;
  int8_t replicate_identity_settings;
  int16_t number_columns;
  char** columns;
} relation_t;

relation_t* parse_relation(stream_t *stream);
void delete_relation(relation_t* relation);

char* parse_tuple(stream_t *stream);

typedef struct {
  int16_t size;
  char** values;
} tuples_t;

tuples_t* parse_tuples(stream_t* stream);
void delete_tuples(tuples_t* tuples);
