#include <ctype.h>
#include <time.h>
#include <gdsql_date.h>

#define HMS2S(h, m, s) (((h)*60.0+(m))*60.0+(s))
#define DAY_IN_SECONDS HMS2S(24.0,0,0)

static int getnum(const char* str,
                  int* p);
static void skip2num(const char* str,
                     int* p);


double gdsql_cal2jul(int Y, int M, int D,
                     int h, int m, int s)
{
    int x = (M - 14) / 12;
    double jul = (unsigned long) (D - 32075 +
                                  1461 * (Y + 4800 + x) / 4 +
                                  367 * (M - 2 - x * 12) / 12 -
                                  3 * ((Y + 4900 + x) / 100) / 4);
    jul += HMS2S(h,m,s) / DAY_IN_SECONDS - 0.5;
    return jul;
}

double gdsql_jul2cal(double jul,
                     int* Y, int* M, int* D,
                     int* h, int* m, int* s)
{
    int i, j, k;
    int p, q, r;
    int u, v;

    double tmp = jul + 0.5;
    unsigned long dd = (unsigned long) (tmp);
    unsigned long tt = (unsigned long) (DAY_IN_SECONDS * (tmp - dd) + 0.5);

    u = dd + 68569;
    v = 4 * u / 146097;
    u = u - (146097 * v + 3) / 4;
    i = 4000 * (u + 1) / 1461001;
    u = u - 1461 * i / 4 + 31;
    j = 80 * u / 2447;
    k = u - 2447 * j / 80;
    u = j / 11;
    j = j + 2 - 12 * u;
    i = 100 * (v - 49) + i + u;

    if (Y != 0)
        *Y = i;
    if (M != 0)
        *M = j;
    if (D != 0)
        *D = k;

    r = tt % 60;
    tt /= 60;
    q = tt % 60;
    tt /= 60;
    p = tt % 24;

    if (h != 0)
        *h = p;
    if (m != 0)
        *m = q;
    if (s != 0)
        *s = r;

    // Return a double that, when printed, looks like this:
    // YYMMDD.HHMMSS
    return (1.0 * (((i*100)+j)*100+k) +
            1.0 * (((p*100)+q)*100+r) / 1000000.0);
}

double gdsql_str2jul(const char* str)
{
    int p = 0;

    skip2num(str, &p);
    int Y = getnum(str, &p);
    skip2num(str, &p);
    int M = getnum(str, &p);
    skip2num(str, &p);
    int D = getnum(str, &p);
    skip2num(str, &p);
    
    int h = getnum(str, &p);
    skip2num(str, &p);
    int m = getnum(str, &p);
    skip2num(str, &p);
    int s = getnum(str, &p);

    return gdsql_cal2jul(Y, M, D, h, m, s);
}

int gdsql_get_dow(double jul)
{
    unsigned long dd = (unsigned long) jul;
    return (dd % 7);
}

unsigned int gdsql_get_now(int* Y, int* M, int* D,
                           int* h, int* m, int* s,
                           int utc)
{
    unsigned int t = (unsigned int) time(0);
    return gdsql_get_time(t, Y, M, D, h, m, s, utc);
}

unsigned int gdsql_get_time(unsigned int when,
                            int* Y, int* M, int* D,
                            int* h, int* m, int* s,
                            int utc)
{
    struct tm* tm;
    time_t t = when;

    if (utc)
        tm = gmtime(&t);
    else
        tm = localtime(&t);

    if (Y != 0)
        *Y = tm->tm_year + 1900;
    if (M != 0)
        *M = tm->tm_mon + 1;
    if (D != 0)
        *D = tm->tm_mday;
    if (h != 0)
        *h = tm->tm_hour;
    if (m != 0)
        *m = tm->tm_min;
    if (s != 0)
        *s = tm->tm_sec;

    return when;
}

unsigned int gdsql_set_time(int Y, int M, int D,
                            int h, int m, int s,
                            int utc)
{
    time_t t = 0; // 1-Jan-1970
    struct tm* tm;

    if (utc)
        tm = gmtime(&t);
    else
        tm = localtime(&t);

    if (Y >= 0)
        tm->tm_year = Y - 1900;

    if (M >= 0)
        tm->tm_mon = M - 1;

    if (D >= 0)
        tm->tm_mday = D;

    if (h >= 0)
        tm->tm_hour = h;

    if (m >= 0)
        tm->tm_min = m;

    if (s >= 0)
        tm->tm_sec = s;

    t = mktime(tm);
    return (unsigned int) t;
}

int gdsql_valid_date(int Y, int M, int D)
{
    int t;

    t = gdsql_days_in_month(Y, M);
    if (t <= 0)
        return 0;

    if (D < 1 || D > t)
        return 0;

    return t;
}

int gdsql_leap_year(int Y)
{
    int l = ((Y % 4) == 0 && ((Y % 100 != 0) || (Y % 400 == 0)));
    return l;
}

int gdsql_days_in_month(int Y, int M)
{
    int t = 0;

    if (Y < 1)
        return 0;

    if (M < 1 || M > 12)
        return 0;

    if (M == 1 || M == 3 || M == 5 || M == 7 ||
        M == 8 || M == 10 || M == 12)
        t = 31;
    else if (M != 2)
        t = 30;
    else if (gdsql_leap_year(Y))
        t = 29;
    else
        t = 28;

    return t;
}


static int getnum(const char* str,
                  int* p)
{
    int s = 1;
    int n = 0;
    int state = 0;
    while (1) {
        char c = str[(*p)++];
        if (c == '\0')
            break;
        else if (c == '+') {
            if (state != 0) {
                break;
            }
            state = 1;
            s = 1;
        }
        else if (c == '-') {
            if (state != 0) {
                break;
            }
            state = 1;
            s = -1;
        }
        else if (isdigit((int) c)) {
            state = 2;
            n = n * 10 + c - '0';
        }
        else {
            break;
        }
    }
    return (s*n);
}

static void skip2num(const char* str,
                     int* p)
{
    char c;
    for (c = str[*p]; ! isdigit((int) c) && c != '\0'; ++*p)
        ;
}

#if 0
static int easter(int Y, int* M, int* D)
{
    int a, b, c, d, e, f, g, h, i, k, l, m;
    int p, q;

    if (Y < 1)
        return 0;

    a = Y % 19;
    b = Y / 100;
    c = Y % 100;
    d = b / 4;
    e = b % 4;
    f = (b + 8) / 25;
    g = (b - f + 1) / 3;
    h = (19 * a + b - d - g + 15) % 30;
    i = c / 4;
    k = c % 4;
    l = (32 + 2 * e + 2 * i - h - k) % 7;
    m = (a + 11 * h + 22 * l) / 451;
    q = (h + l - 7 * m + 114) / 31;
    p = ((h + l - 7 * m + 114) % 31) + 1;

    if (M != 0)
        *M = q;
    if (D != 0)
        *D = p;
    
    return ((Y*100 + q)*100 + p);
}
#endif
