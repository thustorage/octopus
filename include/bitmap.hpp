/*** Bitmap header. ***/

/** Version 1 + modification for table class. **/

/** Redundance check. **/
#ifndef BITMAP_HEADER
#define BITMAP_HEADER

/** Included files. **/
#include <stdint.h>                     /* Standard integers. E.g. uint8_t */
#include <stdlib.h>                     /* Standard library for memory allocation and exit function. */
#include <stdio.h>                      /* Standard I/O operations. E.g. fprintf() */            
#include <string.h>                     /* Header for memory operations. E.g. memset() */

/** Classes. **/
class Bitmap
{
private:
    uint8_t *bytes;                     /* Byte array to hold bitmap. */
    uint64_t varCountFree;              /* Count variable of free bits. */
    uint64_t varCountTotal;             /* Count variable of total bits. */

public:
    bool get(uint64_t pos, bool *status); /* Get status of a bit. */
    bool set(uint64_t pos);             /* Set a bit. */
    bool clear(uint64_t pos);           /* Clear a bit. */
    bool findFree(uint64_t *pos);       /* Find first free bit. */
    uint64_t countFree();               /* Count of free bits. */
    uint64_t countTotal();              /* Count of total bits. */
    Bitmap(uint64_t count, char *buffer); /* Constructor of bitmap. Read buffer to initialize current status. */
    ~Bitmap();                          /* Destructor of bitmap. Do not free buffer. */
};

/** Redundance check. **/
#endif