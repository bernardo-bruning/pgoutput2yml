#pragma once

#ifdef ERROR_LEVEL
#define ERROR(format, ...) printf("ERROR: " format "\n", ##__VA_ARGS__)
#else
#define ERROR(format, ...)
#endif // ERROR

#ifdef INFO_LEVEL
#define INFO(format, ...) printf("INFO: " format "\n", ##__VA_ARGS__)
#else
#define INFO(format, ...) 
#endif // INFO


#ifdef DEBUG_LEVEL
#define DEBUG(format, ...) printf("DEBUG: " format "\n", ##__VA_ARGS__)
#else
#define DEBUG(format, ...) 
#endif // DEBUG



