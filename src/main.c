#include <libpq-fe.h>
#include <unistd.h>

#include "options.h"
#include "logging.h"
#include "stream.h"
#include "decoder.h"

const int ERR_CONNECT = 1;
const int ERR_QUERY = 2;
const int ERR_FORMAT = 3;
const int ERR_HANDLE = 4;

const char* START_REPLICATION_COMMAND = "START_REPLICATION SLOT \"%s\" LOGICAL 0/0 (proto_version '1', publication_names '%s')";
const char* CREATE_REPLICATION_SLOT_COMMAND = "SELECT pg_create_logical_replication_slot('%s', 'pgoutput');";
const char* DROP_REPLICATION_SLOT_COMMAND = "SELECT pg_drop_replication_slot('%s');";

int update_status(PGconn *conn, int64_t wal, int64_t timestamp) {
  DEBUG("updating status");
  int err;
  char buffer[1+8+8+8+8+1];

  stream_t* stream = create_stream(buffer, sizeof(buffer));
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
  delete_stream(stream);
}

int handle_wal(PGconn *conn, stream_t *stream, FILE* file) {
  int err;

  DEBUG("handling wal");
  skip_bytes(stream, 24); // Skip reading wal metadata

  int32_t relation_id;
  int16_t number_columns;
  char operation = read_char(stream);
  DEBUG("handling operation %c", operation);
  switch (operation) {
    case 'C':
      commit_t* commit = parse_commit(stream);
      if(commit == NULL) {
        return ERR_HANDLE;
      }

      fflush(file);
      update_status(conn, commit->lsn, commit->timestamp);
      delete_commit(commit);
      break;
    case 'R':
      relation_t* relation = parse_relation(stream);
      print_relation(relation, file);
      delete_relation(relation);
      break;
    case 'I':
      insert_t* insert = parse_insert(stream);
      print_insert(insert, file);
      delete_insert(insert);
      break;
    case 'U':
      update_t* update = parse_update(stream);
      print_update(update, file);
      delete_update(update);
      break;
    case 'D':
      delete_t* delete = parse_delete(stream);
      print_delete(delete, file);
      delete_delete(delete);
      break;
    default:
      DEBUG("unknown operation: %c", operation);
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


int install(PGconn *conn, char* slotname) {
  INFO("starting install");
  char command[1024];
  int err;

  err = sprintf(command, CREATE_REPLICATION_SLOT_COMMAND, slotname);
  if(err < 0) {
    ERROR("format create replication error");
    return ERR_FORMAT;
  }

  PGresult* slot_result = PQexec(conn, command);
  char *error = PQresultErrorMessage(slot_result);
  if(error[0] != '\0') {
    ERROR("%s", error);
    return ERR_QUERY;
  }

  PQclear(slot_result);
  INFO("install completed");
  return 0;
}

int uninstall(PGconn *conn, char* slotname) {
  INFO("starting uninstall");
  char command[1024];
  int err;

  err = sprintf(command, DROP_REPLICATION_SLOT_COMMAND, slotname);
  if(err < 0) {
    ERROR("format drop replication slot failed");
    return ERR_FORMAT;
  }

  PGresult* slot_result = PQexec(conn, command);
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
    err = sprintf(query, START_REPLICATION_COMMAND, slotname, publication);
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
      stream_t *stream = create_stream(buffer, buffer_size);
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

      delete_stream(stream);
      PQfreemem(buffer);
    }

    result = PQgetResult(conn);
    if(PQendcopy(conn) > 0) {
      ERROR("failed end copy: %s", PQerrorMessage(conn));
      return ERR_QUERY;
    }

    PQclear(result);
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
    return install(conn, options.slotname);
  }

  if(options.uninstall) {
    return uninstall(conn, options.slotname);
  }

  err = watch(conn, file, options.slotname, options.publication);

  PQfinish(conn);
  fclose(file);
  return err;
}
