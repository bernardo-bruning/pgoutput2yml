#include <libpq-fe.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>

#define INFO(format, ...) printf("INFO: " format "\n", ##__VA_ARGS__)
#define DEBUG(format, ...) printf("DEBUG: " format "\n", ##__VA_ARGS__)
#define ERROR(format, ...) printf("ERROR: " format "\n", ##__VA_ARGS__)

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

// TODO: Move to args.h and args.c
typedef struct {
  char* dbname;
  char* user;
  char* password;
  char* host;
  char* port;
  char* slotname;
  char* publication;
  bool install;
  bool uninstall;
} args_t;

int parse_arg(const char* name, char** value, char argi, char *argv[]) {
  if(strcmp(argv[argi], name) == 0) {
    *value = argv[argi + 1];
    return 1;
  }

  return 0;
}

int parse_has_arg(const char* name, bool* value, char argi, char *argv[]) {
  if(strcmp(argv[argi], name) == 0) {
    *value = true;
    return 1;
  }

  return 0;

}

args_t parse_args(int argc, char *argv[]) {
  args_t args;

  args.dbname = "postgres";
  args.user = "postgres";
  args.password = "postgres";
  args.host = "localhost";
  args.port = "5432";
  args.slotname = "cdc";
  args.publication = "cdc";
  args.install = false;
  args.uninstall = false;

  for(int i=0; i < argc; i++){
    if(parse_arg("--dbname", &args.dbname, i, argv)){ continue; }
    if(parse_arg("--user", &args.user, i, argv)){ continue; }
    if(parse_arg("--password", &args.password, i, argv)){ continue; }
    if(parse_arg("--host", &args.host, i, argv)){ continue; }
    if(parse_arg("--port", &args.port, i, argv)){ continue; }
    if(parse_arg("--slotname", &args.slotname, i, argv)){ continue; }
    if(parse_arg("--publication", &args.slotname, i, argv)){ continue; }
    if(parse_has_arg("--install", &args.install, i, argv)) { continue; }
    if(parse_has_arg("--uninstall", &args.uninstall, i, argv)) { continue; }
  }

  return args;
}

int main(int argc, char *argv[]) {
  printf("YAML CDC\n");
  printf("=======================\n");

  args_t args = parse_args(argc, argv);

  FILE *file = fopen("cdc.yaml", "a+");
  char conn_str[1024];
  int conn_str_err = sprintf(conn_str, "dbname=%s user=%s password=%s host=%s port=%s", args.dbname, args.user, args.password, args.host, args.port);
  if(conn_str_err <= 0) {
    ERROR("failed to format connection");
    return ERR_FORMAT;
  }

  INFO("establishing connection: %s", conn_str);
  PGconn *conn = PQconnectdb(conn_str);

  if (PQstatus(conn) != CONNECTION_OK) {
    ERROR("connection failure");
    return ERR_CONNECT;
  }

  INFO("database connected");

  if(args.install) {
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

  if(args.uninstall) {
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

  INFO("watching changes");
  while (1) {
    char binary_changes_query[1024];

    if(sprintf(binary_changes_query, BINARY_CHANGES_SQL, args.slotname, args.publication) <= 0) {
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
  PQfinish(conn);
  fclose(file);
  return 0;
}
