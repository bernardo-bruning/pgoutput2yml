#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


#ifdef ERROR_LEVE
#define ERROR(format, ...) fprintf(stderr,"ERROR: " format "\n", ##__VA_ARGS__)
#else
#define ERROR(format, ...)
#endif // ERROR

#ifdef INFO_LEVEL
#define INFO(format, ...) fprintf(stderr, "INFO: " format "\n", ##__VA_ARGS__)
#else
#define INFO(format, ...) 
#endif // INFO


#ifdef DEBUG_LEVEL
#define DEBUG(format, ...) fprintf(stderr, "DEBUG: " format "\n", ##__VA_ARGS__)
#else
#define DEBUG(format, ...) 
#endif // DEBUG



