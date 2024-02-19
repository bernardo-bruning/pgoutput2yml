#include <stdio.h>

#include "logging.h"
#include "decoder.h"

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
