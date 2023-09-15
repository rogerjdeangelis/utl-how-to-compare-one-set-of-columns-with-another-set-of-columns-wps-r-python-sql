%let pgm=utl-how-to-compare-one-set-of-columns-with-another-set-of-columns-wps-r-python-sql;

How to compare one set of columns with another set of columns wps r python sql

https://stackoverflow.com/questions/77113453/how-to-compare-one-set-of-columns-with-another-set-of-columns

I have taken some liberties in normalizing the input.
Normalization sometimes provides a more robust solution?

   SOLUTIONS

         1 wps sql
         2 r no sql
         3 wps r sql
         3 wps python sql

github
https://tinyurl.com/yck5ty2v
https://github.com/rogerjdeangelis/utl-how-to-compare-one-set-of-columns-with-another-set-of-columns-wps-r-python-sql

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
 input
   jan feb mar  apr may jun;
 array mons jan--jun;
 rec=_n_;
 grp =1;
 do over mons;
    if trim(vname(mons)) = 'APR' then grp=2;
    nam = vname(mons);
    val = mons;
    output;
 end;
 drop jan--jun;
cards4;
10 20 30 5 0 10
7 9 6 5 9 8
1 2 3 4 5 6
;;;;
run;quit;

/**************************************************************************************************************************/
/*                             |                                 |                                                        */
/* INPUT                       |   RULES                         |  OUTPUT                                                */
/*                             |                                 |                                                        */
/* SD1.HAVE total obs=18       |                                 |                                                        */
/*                             |                                 |                                                        */
/*   REC    GRP    NAM    VAL  |                                 |   REC    L_MONTH    R_MONTH    R_VAL    L_VAL    FLG   */
/*                             |                                 |                                                        */
/*    1      1     JAN     10* |   10 in rec 1 is in both groups |    1       JAN        JUN        10       10      1    */
/*    1      1     FEB     20  |                                 |    2       FEB        MAY         9        9      1    */
/*    1      1     MAR     30  |                                 |                                                        */
/*                             |                                 |                                                        */
/*    1      2     APR      5  |                                 |                                                        */
/*    1      2     MAY      0  |                                 |                                                        */
/*    1      2     JUN     10* |   10 in rec 1 is in both groups |                                                        */
/*                             |                                 |                                                        */
/*                             |                                 |                                                        */
/*    2      1     JAN      7  |                                 |                                                        */
/*    2      1     FEB      9* |   10 in rec 1 is in both groups |                                                        */
/*    2      1     MAR      6  |                                 |                                                        */
/*                             |                                 |                                                        */
/*    2      2     APR      5  |                                 |                                                        */
/*    2      2     MAY      9* |   10 in rec 1 is in both groups |                                                        */
/*    2      2     JUN      8  |                                 |                                                        */
/*                             |                                 |                                                        */
/*                             |                                 |                                                        */
/*    3      1     JAN      1  |   No common values              |                                                        */
/*    3      1     FEB      2  |                                 |                                                        */
/*    3      1     MAR      3  |                                 |                                                        */
/*                             |                                 |                                                        */
/*    3      2     APR      4  |                                 |                                                        */
/*    3      2     MAY      5  |                                 |                                                        */
/*    3      2     JUN      6  |                                 |                                                        */
/*                             |                                 |                                                        */
/**************************************************************************************************************************/

/*                                  _
/ | __      ___ __  ___   ___  __ _| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | |
| |  \ V  V /| |_) \__ \ \__ \ (_| | |
|_|   \_/\_/ | .__/|___/ |___/\__, |_|
             |_|                 |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('

options validvarname=any;
libname sd1 "d:/sd1";

proc sql;
  create
     table sd1.want as
  select
     l.rec
     ,l.nam as l_month
     ,r.nam as r_month
     ,r.val as r_val
     ,l.val as l_val
    ,(l.val = r.val) as flg
  from
      sd1.have as l, sd1.have as r
  where
           l.grp = 1
      and  r.grp = 2
      and  l.val = r.val
      and  l.rec = r.rec
;quit;

proc print;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    REC    l_month    r_month    r_val    l_val    flg                                                              */
/*                                                                                                                        */
/*  1      1       JAN        JUN        10       10      1                                                               */
/*  2      2       FEB        MAY         9        9      1                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                   _
|___ \   _ __   _ __   ___    ___  __ _| |
  __) | | `__| | `_ \ / _ \  / __|/ _` | |
 / __/  | |    | | | | (_) | \__ \ (_| | |
|_____| |_|    |_| |_|\___/  |___/\__, |_|
                                     |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
submit;
df <- data.frame(jan=c(10,7,1),
           feb=c(20,9,2),
           mar=c(30,6,3),
           apr=c(5,5,4),
           may=c(0,9,5),
           jun=c(10,8,6)
) ;
df$flag <- sapply(1:nrow(df), \(x)
     ifelse(any(unlist(df[x, 1:3]) %in% unlist(df[x, 4:6])), "Y", "N"));
endsubmit;
import data=sd1.want r=df;
run;quit;
proc print data=sd1.want width=min;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    JAN    FEB    MAR    APR    MAY    JUN    FLAG                                                                  */
/*                                                                                                                        */
/*  1      10     20     30     5      0      10     Y                                                                    */
/*  2       7      9      6     5      9       8     Y                                                                    */
/*  3       1      2      3     4      5       6     N                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                    _
|___ /   _ __   ___  __ _| |
  |_ \  | `__| / __|/ _` | |
 ___) | | |    \__ \ (_| | |
|____/  |_|    |___/\__, |_|
                       |_|
*/

%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want <- sqldf("
  select
     l.rec
     ,l.nam as l_month
     ,r.nam as r_month
     ,r.val as r_val
     ,l.val as l_val
    ,(l.val = r.val) as flg
  from
      have as l inner join have as r
  on
           l.grp = 1
      and  r.grp = 2
      and  l.val = r.val
      and  l.rec = r.rec
  ");
want;
endsubmit;
import data=sd1.want r=want;
proc print;
run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*    REC l_month r_month R_VAL L_VAL FLG                                                                                 */
/*                                                                                                                        */
/*  1   1     JAN     JUN    10    10   1                                                                                 */
/*  2   2     FEB     MAY     9     9   1                                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                      _   _                             _
| || |   __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                  |_|         |_|    |___/                                |_|
*/

%utl_submit_wps64x("
options validvarname=any lrecl=32756;
libname sd1 'd:/sd1';
proc sql;select max(cnt) into :_cnt from (select count(nam) as cnt from sd1.have group by nam);quit;
%array(_unq,values=1-&_cnt);
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
  select
     l.rec
     ,l.nam as l_month
     ,r.nam as r_month
     ,r.val as r_val
     ,l.val as l_val
    ,(l.val = r.val) as flg
  from
      have as l inner join have as r
  on
           l.grp = 1
      and  r.grp = 2
      and  l.val = r.val
      and  l.rec = r.rec
''');
endsubmit;
import data=sd1.want python=want;
proc print;
run;quit;
"));

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*   REC    L_MONTH    R_MONTH    R_VAL    L_VAL    FLG                                                                   */
/*                                                                                                                        */
/*    1       JAN        JUN        10       10      1                                                                    */
/*    2       FEB        MAY         9        9      1                                                                    */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
