#pragma once
#include <stdbool.h>

typedef struct {
  char* file;
  char* dbname;
  char* user;
  char* password;
  char* host;
  char* port;
  char* slotname;
  char* publication;
  bool install;
  bool uninstall;
} options_t;

options_t parse_options(int argc, char *argv[]);
