/*** Bitmap class. ***/

/** Version 1 + modification for table class. **/

/** Included files. **/
#include "bitmap.hpp"

/** Implemented functions. **/
/* Get status of a bit.
   @param   pos     The position of selected bit. Start from 0.
   @param   status  The returned status of selected bit. If bit is set then return true, otherwise return false.
   @return          If operation succeeds then return true, otherwise return false.
                    E.g. if position is out of bitmap's range or status pointer is null then return false. */
bool Bitmap::get(uint64_t pos, bool *status)
{
    if ((pos < varCountTotal) && (status != NULL)) { /* Judge if position and status variable are valid. */
        uint64_t index = pos / 8;       /* Index of byte array. */
        uint64_t offset = pos % 8;      /* Offset in one byte. */
        if ((bytes[index] & (1 << (7 - offset))) != 0) /* Judge if bit is set. */
            *status = true;             /* Bit is set. */
        else
            *status = false;            /* Bit is cleared. */

        return true;                    /* Succeed in getting status. */
    } else
        return false;                   /* Fail in getting status. */
}

/* Set a bit.
   @param   pos     The position of selected bit. Start from 0.
   @return          If operation succeeds then return true, otherwise return false.
                    E.g. if position is out of bitmap's range then return false. */
bool Bitmap::set(uint64_t pos)
{
    if (pos < varCountTotal) {          /* Judge if position is valid. */
        uint64_t index = pos / 8;       /* Index of byte array. */
        uint64_t offset = pos % 8;      /* Offset in one byte. */
        if ((bytes[index] & (1 << (7 - offset))) != 0) /* Judge if bit is set. */
            ;                           /* Do nothing. */
        else {
            bytes[index] |= (1 << (7 - offset)); /* Set bit in position. */
            varCountFree--;             /* Count of free bits decreases. */
        }
        return true;                    /* Succeed in setting bit. */
    } else
        return false;                   /* Fail in setting bit. */
}

/* Clear a bit.
   @param   pos     The position of selected bit. Start from 0.
   @return          If operation succeeds then return true, otherwise return false.
                    E.g. if position is out of bitmap's range then return false. */
bool Bitmap::clear(uint64_t pos)
{
    if (pos < varCountTotal) {          /* Judge if position is valid. */
        uint64_t index = pos / 8;       /* Index of byte array. */
        uint64_t offset = pos % 8;      /* Offset in one byte. */
        if ((bytes[index] & (1 << (7 - offset))) != 0) { /* Judge if bit is set. */
            bytes[index] &= ~(1 << (7 - offset)); /* Clear bit in position. */
            varCountFree++;             /* Count of free bits increases. */
        } else
            ;                           /* Do nothing. */
        return true;                    /* Succeed in clearing bit. */
    } else
        return false;                   /* Fail in clearing bit. */
}

/* Find first free bit. (That is the first cleared bit since position 0.)
   @param   pos     If a first free bit is found, then it contains position of the first free bit.
   @return          If operation succeeds then return true, otherwise return false.
                    E.g. if no free bits or null position pointer then return false. */
bool Bitmap::findFree(uint64_t *pos)    /* Find first free bit. */
{
    if (pos != NULL) {                  /* Judge if position is valid. */
        if (varCountFree > 0) {         /* Judge if free bit exists. */
            for (unsigned int index = 0; index < varCountTotal / 8; index++) { /* Search in all bytes in array. */
                for (int offset = 0; offset <= 7; offset++) { /* Search in all offsets in byte. */
                    if ((bytes[index] & (1 << (7 - offset))) == 0) { /* Judge if bit is cleared. */
                        *pos = index * 8 + offset; /* Return position of cleared bit. */
                        return true;
                    }
                }
            }
            return false;               /* Should not reach here. */
        } else
            return false;               /* Fail in finding first free bit due to no free bits. */
    } else
        return false;                   /* Fail in finding first free bit due to null position pointer. */
}

/* Count of free bits.
   @return          Return the count of free bits. */
uint64_t Bitmap::countFree()
{
    return varCountFree;                /* Return count of free bits. */
}

/* Count of total bits.
   @return          Return the count of total bits. */
uint64_t Bitmap::countTotal()
{
    return varCountTotal;               /* Return count of total bits. */
}

/* Constructor of bitmap. Here use existed buffer as bitmap and initialize other parameter based on buffer.
   (If fail in constructing, error information will be printed in standard error output.)
   @param   count   The count of total bits in the bitmap.
   @param   buffer  The buffer to contain bitmap. */
Bitmap::Bitmap(uint64_t count, char *buffer) /* Constructor of bitmap. */
{
    if (count % 8 == 0) {
        if (buffer != NULL) {           /* Judge if buffer is null or not. */
            bytes = (uint8_t *)buffer;  /* Assign byte array of bitmap. */
            varCountTotal = count;      /* Initialize count of total bits. */
            varCountFree = 0;           /* Initialize count of free bits. */
            for (unsigned int index = 0; index < varCountTotal / 8; index++) { /* Search in all bytes in array. */
                for (int offset = 0; offset <= 7; offset++) { /* Search in all offsets in byte. */
                    if ((bytes[index] & (1 << (7 - offset))) == 0) { /* Judge if bit is cleared. */
                        varCountFree++; /* Increment of free bit. */
                    }
                }
            }
        } else {
            fprintf(stderr, "Bitmap: buffer pointer is null.\n");
            exit(EXIT_FAILURE);         /* Fail due to null buffer pointer. */
        }
    } else {
        fprintf(stderr, "Bitmap: count should be times of eight.\n");
        exit(EXIT_FAILURE);             /* Fail due to count alignment. */
    }
}

/* Deconstructor of bitmap. Here do nothing due to persistence. */
Bitmap::~Bitmap()                       /* Destructor of bitmap. */
{
    ;                                   /* Do nothing. */
}