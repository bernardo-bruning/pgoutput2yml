#include <stdlib.h>
#include <check.h>
#include "../src/stream.h"

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
