#include <libpq-fe.h>
#include <unistd.h>

#include "options.h"
#include "logging.h"
#include "stream.h"

const int ERR_CONNECT = 1;
const int ERR_QUERY = 2;
const int ERR_FORMAT = 3;

const char* START_REPLICATION_QUERY = "START_REPLICATION SLOT \"%s\" LOGICAL 0/0 (proto_version '1', publication_names '%s')";

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

int parse_buffer(char* buff, int size, FILE* file) {
  int32_t relation_id;
  int16_t number_columns;
  stream_t *buffer = create_buffer(buff, size);
  switch (read_char(buffer)) {
    case 'R':
      parse_relation(buffer, file);
      break;
    case 'I':
      parse_insert(buffer, file);
      break;
    case 'U':
      parse_update(buffer, file);
      break;
  }

  delete_buffer(buffer);
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
      switch(buffer[0]) {
        case 'w':
          buffer += 25; // Skip reading wal metadata
          DEBUG("receiving wal with command %c", buffer[0]);
          parse_buffer(buffer, buffer_size, file);
          fflush(file);
          break;
        case 'k':
          DEBUG("keeping alive");
          break;
        default:
          DEBUG("buffer input not parsed: %s", buffer);
      }
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
