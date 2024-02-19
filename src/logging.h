#pragma once

#define INFO(format, ...) printf("INFO: " format "\n", ##__VA_ARGS__)

#ifdef DEBUG_ON
#define DEBUG(format, ...) printf("DEBUG: " format "\n", ##__VA_ARGS__)
#else
#define DEBUG(format, ...) 
#endif // DEBUG

#define ERROR(format, ...) printf("ERROR: " format "\n", ##__VA_ARGS__)


