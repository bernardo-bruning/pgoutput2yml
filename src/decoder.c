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

insert_t* parse_insert(stream_t* stream) {
  insert_t *insert = malloc(sizeof(insert_t));
  insert->relation_id = read_int32(stream);

  char char_tuple = read_char(stream);
  insert->data = parse_tuples(stream);
  return insert;
}

void delete_insert(insert_t* insert) {
  delete_tuples(insert->data);
  free(insert);
}

void print_relation(relation_t* relation, FILE *file) {
  fprintf(file, " - relation_id: %ld:\n", relation->id);
  fprintf(file, "   operation: relation\n");
  fprintf(file, "   namespace: %s\n", relation->namespace);
  fprintf(file, "   name: %s\n", relation->name);
  fprintf(file, "   replica_identity_settings: %d\n", relation->replicate_identity_settings);
  fprintf(file, "   columns:\n");
  for(int i=0; i<relation->number_columns; i++) {
    fprintf(file, "\t - %s\n", relation->columns[i]);
  }
}

void print_tuples(tuples_t *tuples, FILE *file) {
  for(int i=0; i < tuples->size; i++) {
    char* tuple = tuples->values[i];
    fprintf(file, "\t  - %s\n", tuple);
  }
}

void print_update(update_t *update, FILE *file) {
  fprintf(file, " - relation_id: %d\n", update->relation_id);
  fprintf(file, "   operation: update\n");
  fprintf(file, "   from:\n");
  print_tuples(update->from, file);
  fprintf(file, "   to:\n");
  print_tuples(update->to, file);
}

void print_delete(delete_t *del, FILE *file) {
  fprintf(file, " - relation_id: %d\n", del->relation_id);
  fprintf(file, "   operation: delete\n");
  fprintf(file, "   data:\n");
  print_tuples(del->data, file);
}

void print_insert(insert_t *insert, FILE *file) {
  fprintf(file, " - relation_id: %d\n", insert->relation_id);
  fprintf(file, "   operation: insert\n");
  fprintf(file, "   data:\n");
  print_tuples(insert->data, file);
}


