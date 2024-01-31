#include <libpq-fe.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

#include "options.h"
#include "logging.h"

const int ERR_CONNECT = 1;
const int ERR_QUERY = 2;
const int ERR_FORMAT = 3;

const char* BINARY_CHANGES_SQL = "SELECT data FROM pg_logical_slot_get_binary_changes('%s', NULL, NULL, 'proto_version', '1', 'publication_names', '%s');";

int16_t read_int16(char *bytes) { return (int16_t)bytes[1] | bytes[0] << 8; }

int32_t read_int32(char *bytes) {
  return (int32_t)(bytes[0] << 24) + (bytes[1] << 16) + (bytes[2] << 8) +
  bytes[3];
}

int parse_buffer(char* buffer, int size, FILE* file) {
  switch (buffer[0]) {
    case 'I':
      uint16_t columns_size = read_int16(buffer + 6);
      fprintf(file, "insert:\n");
      buffer += 8; // Skip metadata
      int column_idx = 0;
      while (column_idx < columns_size) {
        char type = buffer[0];
        int32_t tuple_size = read_int32(buffer + 1);
        buffer += 5;

        switch (type) {
          case 't':
            fprintf(file, "\t - ");
            for (int j = 0; j < tuple_size; j++) {
              fprintf(file, "%c", buffer[j]);
            }
            buffer += tuple_size;
            fprintf(file, "\n");
            break;
          case 'n':
            fprintf(file, "\t - NULL\n");
            break;
          default:
            DEBUG("unknown data tuple: %c", type);
        }

        column_idx++;
      }
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
  char* buffer;
  int buffer_size;
  PGresult *result;

  INFO("watching changes");
  while (1) {
    result = PQexec(conn, "START_REPLICATION SLOT \"cdc\" LOGICAL 0/0 (proto_version '1', publication_names 'cdc')");
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
          buffer += 25;
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

  printf("YAML CDC\n");
  printf("=======================\n");

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
