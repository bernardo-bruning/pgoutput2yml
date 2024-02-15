#include <stdlib.h>
#include <check.h>
#include "../src/stream.h"

START_TEST(read_char_test) 
{
  char buffer[1] = "a";
  stream_t* stream = create_stream(buffer, sizeof(buffer));
  ck_assert_msg(read_char(stream) == 'a', "Read char are expected a but not received");
}
END_TEST

Suite* create_suite(void) {
  Suite *s;
  TCase *tc_core;

  s = suite_create("pgoutput2yml");

  tc_core = tcase_create("Core");

  tcase_add_test(tc_core, read_char_test);
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
