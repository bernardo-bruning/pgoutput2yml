#include <libpq-fe.h>
#include <unistd.h>

#include "options.h"
#include "logging.h"
#include "stream.h"

const int ERR_CONNECT = 1;
const int ERR_QUERY = 2;
const int ERR_FORMAT = 3;

const char* START_REPLICATION_QUERY = "START_REPLICATION SLOT \"%s\" LOGICAL 0/0 (proto_version '1', publication_names '%s')";

typedef struct {
  int64_t lsn_commit;
  int64_t lsn_transaction;
  int64_t commit_timestamp;
} commit_t;

void parse_relation(stream_t* buffer, FILE *file) {
  int32_t relation_id;
  int16_t number_columns;

  relation_id = read_int32(buffer);
  fprintf(file, " - relation_id: %d:\n", relation_id);
  fprintf(file, "   operation: relation\n");
  fprintf(file, "   namespace: %s\n", read_string(buffer));
  fprintf(file, "   name: %s\n", read_string(buffer));
  fprintf(file, "   replica_identity_settings: %d\n", read_int8(buffer));

  number_columns = read_int16(buffer);
  fprintf(file, "   columns: \n");
  for(int i=0; i<number_columns; i++) {
    read_int8(buffer); // read flag column
    fprintf(file, "     - %s \n", read_string(buffer));
    read_int32(buffer); // read oid
    read_int32(buffer); // atttypmod
  }

  fprintf(file, "\n");
}

void parse_tuple(stream_t *buffer, FILE* file){
  int16_t columns_size = read_int16(buffer);
  int column_idx = 0;
  while (column_idx < columns_size) {
    char type = read_char(buffer);
    int32_t tuple_size = read_int32(buffer);

    switch (type) {
      case 't':
        fprintf(file, "\t  - ");
        for (int j = 0; j < tuple_size; j++) {
          fprintf(file, "%c", buffer->value[j]);
        }
        buffer->value += tuple_size;
        fprintf(file, "\n");
        break;
      case 'n':
        fprintf(file, "\t - NULL\n");
        buffer->value++;
        break;
      default:
        DEBUG("unknown data tuple: %c", type);
    }

    column_idx++;
  }
}

void parse_update(stream_t *buffer, FILE* file) {
  char key_char;
  fprintf(file, " - relation_id: %d\n", read_int32(buffer));
  fprintf(file, "   operation: update\n");

  key_char = read_char(buffer);
  if(key_char != 'K' && key_char != 'O') {
    ERROR("unexpected key char %c", key_char);
    exit(ERR_FORMAT);
  }

  fprintf(file, "   old_data:\n");
  parse_tuple(buffer, file);

  key_char = read_char(buffer);
  if(key_char != 'N') {
    return;
  }

  fprintf(file, "   new_data:\n");
  parse_tuple(buffer, file);
}

void parse_insert(stream_t *buffer, FILE* file) {
  int32_t relation_id;
  int16_t number_columns;

  relation_id = read_int32(buffer);
  char char_tuple = read_char(buffer);

  fprintf(file, " - relation_id: %d\n", relation_id);
  fprintf(file, "   operation: insert\n");

  fprintf(file, "   data:\n");
  parse_tuple(buffer, file);
  fprintf(file, "\n");
}

commit_t parse_commit(stream_t *buffer, FILE* file) {
  commit_t commit;
  if(read_int8(buffer) != 0) {
    ERROR("flag commit should be zero");
  } 

  commit.lsn_commit = read_int64(buffer);
  commit.lsn_transaction = read_int64(buffer);
  commit.commit_timestamp = read_int64(buffer);
  return commit;
}

int update_status(PGconn *conn, int64_t wal, int64_t timestamp) {
  DEBUG("updating status");
  int err;
  char buffer[1+8+8+8+8+1];

  stream_t* stream = create_buffer(buffer, sizeof(buffer));
  write_char(stream, 'r');
  write_int64(stream, wal+1);
  write_int64(stream, wal+1);
  write_int64(stream, wal+1);
  write_int64(stream, timestamp);
  write_char(stream, 0);
  err = PQputCopyData(conn, buffer, sizeof(buffer));
  if(err != PGRES_COMMAND_OK) {
    char *error = PQerrorMessage(conn);
    ERROR("fatal error: %s", error);
    return ERR_QUERY;
  }
  PQflush(conn);
  delete_buffer(stream);
}

int handle_wal(PGconn *conn, stream_t *stream, FILE* file) {
  DEBUG("handling wal");
  skip_bytes(stream, 24); // Skip reading wal metadata

  int32_t relation_id;
  int16_t number_columns;
  char operation = read_char(stream);
  DEBUG("handling operation %c", operation);
  switch (operation) {
    case 'C':
      commit_t commit = parse_commit(stream, file);
      fflush(file);
      update_status(conn, commit.lsn_commit, commit.commit_timestamp);
      break;
    case 'R':
      parse_relation(stream, file);
      break;
    case 'I':
      parse_insert(stream, file);
      break;
    case 'U':
      parse_update(stream, file);
      break;
  }
}

void handle_keepalive(PGconn *conn, stream_t *stream) {
  DEBUG("handling keep alive");
  int64_t wal = read_int64(stream);
  int64_t timestamp = read_int64(stream);
  char ops = read_char(stream);
  if(ops == 1) {
    update_status(conn, wal, timestamp);
  }
}

int create_connection(PGconn **conn, options_t options){
  char conn_str[1024];
  int conn_str_err = sprintf(conn_str, "replication=database dbname=%s user=%s password=%s host=%s port=%s", options.dbname, options.user, options.password, options.host, options.port);
  if(conn_str_err <= 0) {
    ERROR("failed to format connection");
    return ERR_FORMAT;
  }

  INFO("establishing connection: %s", conn_str);
  *conn = PQconnectdb(conn_str);

  if (PQstatus(*conn) != CONNECTION_OK) {
    ERROR("connection failure");
    return ERR_CONNECT;
  }

  return 0;
}

int install(PGconn *conn) {
  INFO("starting install");
  PGresult* slot_result = PQexec(conn, "SELECT pg_create_logical_replication_slot('cdc', 'pgoutput');");
  char *error = PQresultErrorMessage(slot_result);
  if(error[0] != '\0') {
    ERROR("%s", error);
    return ERR_QUERY;
  }
  PQclear(slot_result);
  INFO("install completed");
  return 0;
}

int uninstall(PGconn *conn) {
  INFO("starting uninstall");
  PGresult* slot_result = PQexec(conn, "SELECT pg_drop_replication_slot('cdc');");
  char *error = PQresultErrorMessage(slot_result);
  if(error[0] != '\0') {
    ERROR("%s", error);
    return ERR_QUERY;
  }
  PQclear(slot_result);
  INFO("uninstall completed");
  return 0;
}

int watch(PGconn *conn, FILE *file, char* slotname, char* publication) {
  int err;
  char query[1024];
  char* buffer;
  int buffer_size;
  PGresult *result;

  INFO("watching changes");
  while (1) {
    err = sprintf(query, START_REPLICATION_QUERY, slotname, publication);
    if(err < 0) {
      ERROR("format query replication error");
      return ERR_FORMAT;
    }

    result = PQexec(conn, query);
    err = PQresultStatus(result);

    DEBUG("query return code: %d", err);

    if(err == PGRES_FATAL_ERROR) {
      char *error = PQerrorMessage(conn);
      ERROR("fatal error: %s", error);
      return ERR_QUERY;
    }

    while(buffer_size = PQgetCopyData(conn, &buffer, 0) > 0) {
      stream_t *stream = create_buffer(buffer, buffer_size);
      switch(read_char(stream)) {
        case 'w':
          handle_wal(conn, stream, file);
          break;
        case 'k':
          handle_keepalive(conn, stream);
          break;
        default:
          DEBUG("buffer input not parsed: %s", buffer);
      }

      delete_buffer(stream);
    }

    result = PQgetResult(conn);
    if(PQendcopy(conn) > 0) {
      ERROR("failed end copy: %s", PQerrorMessage(conn));
      return ERR_QUERY;
    }

    PQclear(result);
    PQfreemem(buffer);
    sleep(1);
  }
}

int main(int argc, char *argv[]) {
  int err;
  FILE *file;
  PGconn *conn;
  options_t options;

  INFO("YAML CDC\n");
  INFO("=======================\n");

  options = parse_options(argc, argv);

  file = fopen(options.file, "a+");

  err = create_connection(&conn, options);
  if(err > 0) {
    return err;
  }

  INFO("database connected");

  if(options.install) {
    return install(conn);
  }

  if(options.uninstall) {
    return uninstall(conn);
  }

  err = watch(conn, file, options.slotname, options.publication);
  PQfinish(conn);
  fclose(file);
  return err;
}
