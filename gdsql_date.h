#ifndef GDSQL_DATE_H_
#define GDSQL_DATE_H_

/*
 * Functions to compute Julian dates.
 */

// Constants identifying the days of the week.
#define GDSQL_DOW_MON 0
#define GDSQL_DOW_TUE 1
#define GDSQL_DOW_WED 2
#define GDSQL_DOW_THU 3
#define GDSQL_DOW_FRI 4
#define GDSQL_DOW_SAT 5
#define GDSQL_DOW_SUN 6

// Convert a YMD date to a Julian Date.
double gdsql_cal2jul(int Y, int M, int D,
                     int h, int m, int s);

// Convert a Julian Date to a YMD date.
double gdsql_jul2cal(double jul,
                     int* Y, int* M, int* D,
                     int* h, int* m, int* s);

// Convert a string like "1966-11-11 13:40:37" to a Julian Date.
double gdsql_str2jul(const char* str);



// Get a number indicating day of week for a Julian Date.
int gdsql_get_dow(double jul);

// Get the current day/time into its separate components.
unsigned int gdsql_get_now(int* Y, int* M, int* D,
                           int* h, int* m, int* s,
                           int utc);

// Get a given day/time into its separate components.
unsigned int gdsql_get_time(unsigned int when,
                            int* Y, int* M, int* D,
                            int* h, int* m, int* s,
                            int utc);

// Build a day/time with the specified separate components.
unsigned int gdsql_set_time(int Y, int M, int D,
                            int h, int m, int s,
                            int utc);

// Is this a valid date? If not, return 0; if yes, return the number
// of days in that month.
int gdsql_valid_date(int y, int m, int d);

// Is this a leap year?
int gdsql_leap_year(int y);

// Return the number of days in the given month.
int gdsql_days_in_month(int y, int m);

#endif
