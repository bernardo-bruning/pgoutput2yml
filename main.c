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

void process(FILE *file, PGresult *result) {
  int ntuples = PQntuples(result);
  int columns = PQnfields(result);

  for (int tuple_idx = 0; tuple_idx < ntuples; tuple_idx++) {
    size_t size = PQgetlength(result, tuple_idx, 0);
    char *value = PQgetvalue(result, tuple_idx, 0);
    char *bytes = PQunescapeBytea(value, &size);
    switch (bytes[0]) {
    case 'I':
      uint16_t columns_size = read_int16(bytes + 6);

      fprintf(file, "insert:\n");

      int reader_idx = 8;
      while (reader_idx < size) {
        char type = bytes[reader_idx];
        int32_t tuple_size = read_int32(bytes + reader_idx + 1);
        reader_idx += 5;

        switch (type) {
        case 't':
          fprintf(file, "\t - ");
          for (int j = 0; j < tuple_size; j++) {
            fprintf(file, "%c", bytes[reader_idx + j]);
          }
          reader_idx += tuple_size;
          fprintf(file, "\n");
          break;
        case 'n':
          fprintf(file, "\t - NULL\n");
          break;
        default:
          DEBUG("unknown data tuple %c", type);
        }
      }
    }
    PQfreemem(bytes);
  }
}

int create_connection(PGconn *conn, options_t options){
  char conn_str[1024];
  int conn_str_err = sprintf(conn_str, "dbname=%s user=%s password=%s host=%s port=%s", options.dbname, options.user, options.password, options.host, options.port);
  if(conn_str_err <= 0) {
    ERROR("failed to format connection");
    return ERR_FORMAT;
  }

  INFO("establishing connection: %s", conn_str);
  conn = PQconnectdb(conn_str);

  if (PQstatus(conn) != CONNECTION_OK) {
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
  INFO("watching changes");
  while (1) {
    char binary_changes_query[1024];

    if(sprintf(binary_changes_query, BINARY_CHANGES_SQL, slotname, publication) <= 0) {
      ERROR("failed to format query");
      return ERR_FORMAT;
    }

    PGresult *result = PQexec(
        conn,
        binary_changes_query);

    char *error = PQresultErrorMessage(result);
    if (error[0] != '\0') {
      printf("%s", error);
      return ERR_QUERY;
    }

    process(file, result);

    fflush(file);
    PQclear(result);
    sleep(1);
  }
}

int main(int argc, char *argv[]) {
  int err;
  FILE *file;
  PGconn *conn;

  printf("YAML CDC\n");
  printf("=======================\n");

  options_t options = parse_options(argc, argv);

  file = fopen(options.file, "a+");

  err = create_connection(conn, options);
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
