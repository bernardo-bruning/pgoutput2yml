#include <stdlib.h>
#include <check.h>
#include "../src/stream.h"
#include "../src/options.h"
#include "../src/decoder.h"

START_TEST(read_char_test) 
{
  char buffer[1];
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));
  stream_t* stream_write = create_stream(buffer, sizeof(buffer));
  write_char(stream_write, 'a');
  ck_assert_int_eq(read_int8(stream_read), 97);
  ck_assert_int_eq(stream_pos(stream_read), 1);
}
END_TEST

START_TEST(read_int8_test)
{
  char buffer[1];
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));
  stream_t* stream_write = create_stream(buffer, sizeof(buffer));
  write_int8(stream_write, 91);
  ck_assert_int_eq(read_int8(stream_read), 91);
  ck_assert_int_eq(stream_pos(stream_read), 1);
}
END_TEST

START_TEST(read_int16_test)
{
  char buffer[2];
  stream_t* stream_write = create_stream(buffer, sizeof(buffer));
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));
  write_int16(stream_write, 50);
  ck_assert_int_eq(read_int16(stream_read), 50);
  ck_assert_int_eq(stream_pos(stream_read), 2);
}
END_TEST

START_TEST(read_int32_test)
{
  char buffer[4];
  stream_t* stream_write = create_stream(buffer, sizeof(buffer));
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));
  write_int32(stream_write, 50);
  ck_assert_int_eq(read_int32(stream_read), 50);
  ck_assert_int_eq(stream_pos(stream_read), 4);
}
END_TEST

START_TEST(read_int64_test)
{
  char buffer[8];
  stream_t* stream_write = create_stream(buffer, sizeof(buffer));
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));
  write_int64(stream_write, 50);

  ck_assert_int_eq(read_int64(stream_read), 50);
  ck_assert_int_eq(stream_pos(stream_read), 8);
}
END_TEST

START_TEST(read_string_test)
{
  char buffer[] = "ola\0mundo";
  stream_t* stream_read = create_stream(buffer, sizeof(buffer));

  ck_assert_str_eq(read_string(stream_read), "ola");
  ck_assert_int_eq(stream_pos(stream_read), 4);
}
END_TEST

START_TEST(test_parse_options)
{
  int argc = 0;
  char** argv;
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.file, "cdc.yaml");
  ck_assert_str_eq(options.dbname, "postgres");
  ck_assert_str_eq(options.user, "postgres");
  ck_assert_str_eq(options.password, "postgres");
  ck_assert_str_eq(options.host, "localhost");
  ck_assert_str_eq(options.port, "5432");
  ck_assert_str_eq(options.slotname, "cdc");
  ck_assert_str_eq(options.publication, "cdc");
  ck_assert_int_eq(options.install, false);
  ck_assert_int_eq(options.uninstall, false);
}
END_TEST

START_TEST(test_parse_options_file)
{
  int argc = 2;
  char* argv[] = { "--file", "test.yaml" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.file, "test.yaml");
}
END_TEST

START_TEST(test_parse_options_dbname)
{
  int argc = 2;
  char* argv[] = { "--dbname", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.dbname, "test");
}
END_TEST

START_TEST(test_parse_options_user)
{
  int argc = 2;
  char* argv[] = { "--user", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.user, "test");
}
END_TEST

START_TEST(test_parse_options_password)
{
  int argc = 2;
  char* argv[] = { "--password", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.password, "test");
}
END_TEST

START_TEST(test_parse_options_host)
{
  int argc = 2;
  char* argv[] = { "--host", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.host, "test");
}
END_TEST

START_TEST(test_parse_options_port)
{
  int argc = 2;
  char* argv[] = { "--port", "1234" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.port, "1234");
}
END_TEST

START_TEST(test_parse_options_slotname)
{
  int argc = 2;
  char* argv[] = { "--slotname", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.slotname, "test");
}
END_TEST

START_TEST(test_parse_options_publication)
{
  int argc = 2;
  char* argv[] = { "--publication", "test" };
  options_t options = parse_options(argc, argv);

  ck_assert_str_eq(options.publication, "test");
}
END_TEST

START_TEST(test_parse_options_install)
{
  int argc = 1;
  char* argv[] = { "--install" };
  options_t options = parse_options(argc, argv);

  ck_assert_int_eq(options.install, true);
}
END_TEST

START_TEST(test_parse_options_uninstall)
{
  int argc = 1;
  char* argv[] = { "--uninstall" };
  options_t options = parse_options(argc, argv);

  ck_assert_int_eq(options.uninstall, true);
}
END_TEST

START_TEST(test_parse_commit_success)
{
  commit_t* commit;
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int8(writer, 0);
  write_int64(writer, 1);
  write_int64(writer, 2);
  write_int64(writer, 3);

  commit = parse_commit(reader);

  ck_assert_int_eq(commit->lsn, 1);
  ck_assert_int_eq(commit->transaction, 2);
  ck_assert_int_eq(commit->timestamp, 3);

  delete_stream(writer);
  delete_stream(reader);
  delete_commit(commit);
}
END_TEST

START_TEST(test_parse_commit_failed)
{
  commit_t* commit;
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int8(writer, 2);

  commit = parse_commit(reader);

  ck_assert_ptr_null(commit);

  delete_stream(writer);
  delete_stream(reader);
  delete_commit(commit);
}
END_TEST

START_TEST(test_parse_relation_success)
{
  relation_t* relation;
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);
  write_string(writer, "test-namespace");
  write_string(writer, "test-name");
  write_int8(writer, 1);
  write_int16(writer, 2);

  //Write column a
  write_int8(writer, 1);
  write_string(writer, "column-a");
  write_int32(writer, 1);
  write_int32(writer, 1);  

  //Write column b
  write_int8(writer, 1);
  write_string(writer, "column-b");
  write_int32(writer, 1);
  write_int32(writer, 1);

  relation = parse_relation(reader);

  ck_assert_int_eq(relation->id, 1);
  ck_assert_str_eq(relation->namespace, "test-namespace");
  ck_assert_str_eq(relation->name, "test-name");
  ck_assert_int_eq(relation->replicate_identity_settings, 1);
  ck_assert_int_eq(relation->number_columns, 2);

  ck_assert_str_eq(relation->columns[0], "column-a");
  ck_assert_str_eq(relation->columns[1], "column-b");


  delete_stream(writer);
  delete_stream(reader);
  delete_relation(relation);
}
END_TEST

START_TEST(parse_tuples_single_NULL_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int16(writer, 1);
  write_char(writer, 'n');
  tuples_t *tuples = parse_tuples(reader);

  ck_assert_int_eq(tuples->size, 1);
  ck_assert_str_eq(tuples->values[0], "NULL");
}
END_TEST

START_TEST(parse_tuples_single_text_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 4);
  write_string(writer, "test12345");

  tuples_t *tuples = parse_tuples(reader);

  ck_assert_int_eq(tuples->size, 1);
  ck_assert_str_eq(tuples->values[0], "test");
}
END_TEST

START_TEST(parse_update_success_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);

  write_char(writer, 'O');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  write_char(writer, 'N');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "new tuple");

  update_t* update = parse_update(reader);

  ck_assert_int_eq(update->relation_id, 1);
  ck_assert_ptr_nonnull(update->from);
  ck_assert_ptr_nonnull(update->to);
}
END_TEST

START_TEST(parse_update_success_key_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);

  write_char(writer, 'K');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  write_char(writer, 'N');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "new tuple");

  update_t* update = parse_update(reader);

  ck_assert_int_eq(update->relation_id, 1);
  ck_assert_ptr_nonnull(update->from);
  ck_assert_ptr_nonnull(update->to);
}
END_TEST


START_TEST(parse_update_failed_key_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);

  write_char(writer, 'P');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  write_char(writer, 'N');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "new tuple");

  update_t* update = parse_update(reader);
  ck_assert_ptr_null(update);
}
END_TEST

START_TEST(parse_update_without_n_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);

  write_char(writer, 'O');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  write_char(writer, 'O');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "new tuple");

  update_t* update = parse_update(reader);
  ck_assert_ptr_null(update);
}
END_TEST

START_TEST(parse_delete_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);

  write_char(writer, 'O');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  delete_t* delete = parse_delete(reader);
  ck_assert_int_eq(delete->relation_id, 1);
  ck_assert_ptr_nonnull(delete->data);
}
END_TEST

START_TEST(parse_insert_test)
{
  char buffer[1024];

  stream_t* writer = create_stream(buffer, sizeof(buffer));
  stream_t* reader = create_stream(buffer, sizeof(buffer));

  write_int32(writer, 1);
  write_char(writer, 'O');

  //create tuple
  write_int16(writer, 1);
  write_char(writer, 't');
  write_int32(writer, 10);
  write_string(writer, "old tuple");

  insert_t* insert = parse_insert(reader);
  ck_assert_int_eq(insert->relation_id, 1);
  ck_assert_ptr_nonnull(insert->data);
}
END_TEST


Suite* create_suite(void) {
  Suite *s;
  TCase *tc_core;

  s = suite_create("pgoutput2yml");

  tc_core = tcase_create("Core");

  tcase_add_test(tc_core, read_char_test);
  tcase_add_test(tc_core, read_int8_test);
  tcase_add_test(tc_core, read_int16_test);
  tcase_add_test(tc_core, read_int32_test);
  tcase_add_test(tc_core, read_int64_test);
  tcase_add_test(tc_core, read_string_test);

  tcase_add_test(tc_core, test_parse_options);
  tcase_add_test(tc_core, test_parse_options_file);
  tcase_add_test(tc_core, test_parse_options_dbname);
  tcase_add_test(tc_core, test_parse_options_user);
  tcase_add_test(tc_core, test_parse_options_password);
  tcase_add_test(tc_core, test_parse_options_host);
  tcase_add_test(tc_core, test_parse_options_port);
  tcase_add_test(tc_core, test_parse_options_slotname);
  tcase_add_test(tc_core, test_parse_options_publication);
  tcase_add_test(tc_core, test_parse_options_install);
  tcase_add_test(tc_core, test_parse_options_uninstall);

  tcase_add_test(tc_core, test_parse_commit_success);
  tcase_add_test(tc_core, test_parse_commit_failed);
  tcase_add_test(tc_core, test_parse_relation_success);

  tcase_add_test(tc_core, parse_tuples_single_NULL_test);
  tcase_add_test(tc_core, parse_tuples_single_text_test);
  tcase_add_test(tc_core, parse_update_success_test);
  tcase_add_test(tc_core, parse_update_success_key_test);
  tcase_add_test(tc_core, parse_update_failed_key_test);
  tcase_add_test(tc_core, parse_update_without_n_test);

  tcase_add_test(tc_core, parse_delete_test);

  suite_add_tcase(s, tc_core);
  return s;
}

int main(){
  int number_failed;
  Suite *s;
  SRunner *runner;

  s = create_suite();
  runner = srunner_create(s);

  srunner_run_all(runner, CK_NORMAL);
  number_failed = srunner_ntests_failed(runner);
  srunner_free(runner);
  return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
