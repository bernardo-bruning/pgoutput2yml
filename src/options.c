#include <string.h>
#include <stdbool.h>
#include "options.h"

int parse_option(const char* name, char** value, char argi, char *argv[]) {
  if(strcmp(argv[argi], name) == 0) {
    *value = argv[argi + 1];
    return 1;
  }

  return 0;
}

int parse_has_option(const char* name, bool* value, char argi, char *argv[]) {
  if(strcmp(argv[argi], name) == 0) {
    *value = true;
    return 1;
  }

  return 0;

}

options_t parse_options(int argc, char *argv[]) {
  options_t options;

  options.file = "cdc.yaml";
  options.dbname = "postgres";
  options.user = "postgres";
  options.password = "postgres";
  options.host = "localhost";
  options.port = "5432";
  options.slotname = "cdc";
  options.publication = "cdc";
  options.install = false;
  options.uninstall = false;

  for(int i=0; i < argc; i++){
    if(parse_option("--file", &options.file, i, argv)){ continue; }
    if(parse_option("--dbname", &options.dbname, i, argv)){ continue; }
    if(parse_option("--user", &options.user, i, argv)){ continue; }
    if(parse_option("--password", &options.password, i, argv)){ continue; }
    if(parse_option("--host", &options.host, i, argv)){ continue; }
    if(parse_option("--port", &options.port, i, argv)){ continue; }
    if(parse_option("--slotname", &options.slotname, i, argv)){ continue; }
    if(parse_option("--publication", &options.slotname, i, argv)){ continue; }
    if(parse_has_option("--install", &options.install, i, argv)) { continue; }
    if(parse_has_option("--uninstall", &options.uninstall, i, argv)) { continue; }
  }

  return options;
}

