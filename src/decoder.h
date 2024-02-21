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

typedef struct {
  int32_t relation_id;
  tuples_t* from;
  tuples_t* to;
} update_t;

update_t* parse_update(stream_t* stream);
void delete_update(update_t* update);

typedef struct {
  int32_t relation_id;
  tuples_t* data;
} delete_t;

delete_t* parse_delete(stream_t* stream);
void delete_delete(delete_t* del);

typedef struct {
  int32_t relation_id;
  tuples_t* data;
} insert_t;

insert_t* parse_insert(stream_t* stream);
void delete_insert(insert_t* insert);
