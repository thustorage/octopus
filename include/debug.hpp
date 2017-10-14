/*** Debug header. ***/

/** Version 1 + Functional Model Modification **/
/** Redundance check. **/
#ifndef DEBUG_HEADER
#define DEBUG_HEADER

/** Included files. **/
#include <stdio.h>                      /* Standard I/O operations. E.g. vprintf() */
#include <stdarg.h>                     /* Standard argument operations. E.g. va_list */
#include <sys/time.h>                   /* Time functions. E.g. gettimeofday() */

/** Defninitions. **/
#define MAX_FORMAT_LEN 255
#define DEBUG false
#define TITLE false
#define TIMER false
#define CUR  false
/** Classes. **/
class Debug
{
private:
    static long startTime;              /* Last start time in milliseconds. */

public:
    static void debugTitle(const char *str); /* Print debug title string. */
    static void debugItem(const char *format, ...); /* Print debug item string. */
    static void debugCur(const char *format, ...); /* Print debug item string. */
    static void notifyInfo(const char *format, ...); /* Print normal notification. */
    static void notifyError(const char *format, ...); /* Print error information. */
    static void startTimer(const char*); /* Start timer and display information. */
    static void endTimer(); /* End timer and display information. */
};

/** Redundance check. **/
#endif
