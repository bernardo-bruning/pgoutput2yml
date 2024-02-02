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

const char* START_REPLICATION_QUERY = "START_REPLICATION SLOT \"%s\" LOGICAL 0/0 (proto_version '1', publication_names '%s')";

int16_t read_int16(char **buffer_ptr) {
  char* buffer = *buffer_ptr;
  int16_t value = (int16_t)buffer[1] | buffer[0] << 8;
  *buffer_ptr += 2;
  return value;
}

int32_t read_int32(char **buffer_ptr) {
  char *buffer = *buffer_ptr;
  int32_t value = (int32_t)(buffer[0] << 24) + (buffer[1] << 16) + (buffer[2] << 8) +
  buffer[3];
  *buffer_ptr += 4;
  return value;
}

char read_char(char **buffer_ptr) {
  char value = (*buffer_ptr)[0];
  *buffer_ptr += 1;
  return value;
}

char* read_string(char **buffer_ptr) {
  char* buffer = *buffer_ptr;
  *buffer_ptr += strlen(buffer)+1;
  return buffer;
}

int parse_buffer(char* buffer, int size, FILE* file) {
  uint32_t relation_id;
  switch (read_char(&buffer)) {
    case 'R':
      relation_id = read_int32(&buffer);
      fprintf(file, " - relation_id: %d:\n", relation_id);
      fprintf(file, "   operation: relation\n");
      fprintf(file, "   namespace: %s\n", read_string(&buffer));
      fprintf(file, "   name: %s\n", read_string(&buffer));
      break;
    case 'I':
      relation_id = read_int32(&buffer);
      char char_tuple = read_char(&buffer);
      uint16_t columns_size = read_int16(&buffer);

      fprintf(file, " - relation_id: %d\n", relation_id);
      fprintf(file, "   operation: insert\n");
      fprintf(file, "   data:\n");
      int column_idx = 0;
      while (column_idx < columns_size) {
        char type = read_char(&buffer);
        int32_t tuple_size = read_int32(&buffer);

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
