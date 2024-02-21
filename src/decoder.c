#include <stdio.h>

#include "logging.h"
#include "decoder.h"

const char* NULL_STR = "NULL";

commit_t* parse_commit(stream_t *stream) {
  commit_t* commit = malloc(sizeof(commit_t));
  if(read_int8(stream) != 0) {
    ERROR("flag commit should be zero");
    return NULL;
  } 

  commit->lsn = read_int64(stream);
  commit->transaction = read_int64(stream);
  commit->timestamp = read_int64(stream);
  return commit;
}

void delete_commit(commit_t* commit) {
  free(commit);
}

relation_t* parse_relation(stream_t *stream) {
  relation_t* relation = malloc(sizeof(relation_t));
  relation->id = read_int32(stream);
  relation->namespace = read_string(stream);
  relation->name = read_string(stream);
  relation->replicate_identity_settings = read_int8(stream);
  relation->number_columns = read_int16(stream);

  relation->columns = malloc(sizeof(char*)*relation->number_columns);
  for(int i=0; i<relation->number_columns; i++) {
    read_int8(stream); // read flag column
    relation->columns[i] = read_string(stream);
    read_int32(stream); // read oid
    read_int32(stream); // atttypmod
  }
  return relation;
}

void delete_relation(relation_t* relation) {
  free(relation->columns);
  free(relation);
}

char* parse_tuple(stream_t* stream) {
  char type = read_char(stream);
  char* tuple;

  switch(type) {
    case 't':
      int32_t size = read_int32(stream);
      tuple = calloc(size+1, sizeof(char));
      for(int i=0; i<size; i++) {
        tuple[i] = read_char(stream);
      }
      tuple[size] = '\0';
      break;
    case 'n':
      tuple = (char*)NULL_STR;
      break;
  }

  return tuple;
}

tuples_t* parse_tuples(stream_t* stream) {
  tuples_t* tuples = malloc(sizeof(tuples_t));
  tuples->size = read_int16(stream);
  tuples->values = calloc(tuples->size, sizeof(char*));
  for(int i=0; i<tuples->size; i++) {
    tuples->values[i] = parse_tuple(stream);
  }

  return tuples;
}

void delete_tuples(tuples_t* tuples) {
  for(int i=0; i<tuples->size; i++) {
    free(tuples->values[i]);
  }

  free(tuples->values);
  free(tuples);
}

update_t* parse_update(stream_t* stream) {
  update_t* update = malloc(sizeof(update_t));
  update->relation_id = read_int32(stream);

  char key_char = read_char(stream);
  if(key_char != 'K' && key_char != 'O') {
    ERROR("unexpected key char %c", key_char);
    return NULL;
  }

  update->from = parse_tuples(stream);
  key_char = read_char(stream);
  if(key_char != 'N') {
    return NULL;
  }

  update->to = parse_tuples(stream);
  return update;
}

void delete_update(update_t* update) {
  delete_tuples(update->from);
  delete_tuples(update->to);
  free(update);
}

delete_t* parse_delete(stream_t* stream) {
  delete_t* del = malloc(sizeof(delete_t));
  del->relation_id = read_int32(stream);

  char key_char = read_char(stream);
  if(key_char != 'K' && key_char != 'O') {
    ERROR("unexpected key char %c", key_char);
    return NULL;
  }

  del->data = parse_tuples(stream);
  return del;
}
void delete_delete(delete_t* del) {
  delete_tuples(del->data);
  free(del);
}
