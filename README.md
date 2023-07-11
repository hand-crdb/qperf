# qperf
Python script to test query performance using Psycopg3.

Highlights:
* has warm-up repititions
* has pause between queries
* allows test query to be executed in an explicit transaction (`BEGIN`...`COMMIT`) or an implicit transaction
* produces multiple summary statistics:
  * count
  * mean
  * standard deviation
  * minimum time
  * quartiles
  * maximum time

```
Usage: qperf [OPTIONS]

Options:
  --logging TEXT                  Set logging to the specified level: DEBUG,
                                  INFO, WARNING, ERROR, CRITICAL.
                                  Default=INFO
  --url TEXT                      JDBC URL.  Default=DATABASE_URL environment
                                  variable
  --db_app_name TEXT              Database application name.  Default=qperf
  --query TEXT                    SQL query.  Default=SHOW DATABASES
  --print_query_results BOOLEAN   Whether to print the results of the SQL
                                  query.  Default=False
  --query_repetitions INTEGER     How many times to run the SQL query to
                                  gather performance timings.  Default=10
  --warmup_repetitions INTEGER    For warmup, how many times to run the SQL
                                  query before starting to gather performance
                                  timings.  Default=5
  --pause_between FLOAT           How long to pause between queries (including
                                  warm-up queries).  Default=0.1
  --explicit_tx BOOLEAN           Whether to execute the test query inside an
                                  explicit transaction (BEGIN...COMMIT).
                                  Default=False
  --explicit_tx_setting TEXT      Optional transaction setting SQL
                                  statement(s).  Ignored for implicit
                                  transactions.  Default=
  --show_individual_timings BOOLEAN
                                  Whether to show individual timings for each
                                  execution of the test SQL.  Default=False
  --help                          Show this message and exit.
```

Sample execution using [CockroachDB](https://www.cockroachlabs.com) with the `ycsb` test database:

```
▶ ./qperf --logging=INFO --url='postgres://root@52.14.78.15:26257/ycsb?sslmode=disable' --db_app_name qperf --query_repetitions=20 --warmup_repetitions=5 --pause_between=0.1 --show_individual_timings=true --explicit_tx=false --query="SELECT MAX(length(ycsb_key)) AS maxlen FROM usertable AS OF SYSTEM TIME '-10s' WHERE ycsb_key BETWEEN 'user10' AND 'user99'"
0: elapsed time: 0.11422 sec
1: elapsed time: 0.07716 sec
2: elapsed time: 0.14373 sec
3: elapsed time: 0.07666 sec
4: elapsed time: 0.08109 sec
5: elapsed time: 0.08363 sec
6: elapsed time: 0.12376 sec
7: elapsed time: 0.08040 sec
8: elapsed time: 0.08012 sec
9: elapsed time: 0.09691 sec
10: elapsed time: 0.17747 sec
11: elapsed time: 0.09432 sec
12: elapsed time: 0.07899 sec
13: elapsed time: 0.08941 sec
14: elapsed time: 0.07654 sec
15: elapsed time: 0.13839 sec
16: elapsed time: 0.07857 sec
17: elapsed time: 0.07665 sec
18: elapsed time: 0.07698 sec
19: elapsed time: 0.12823 sec
Statistics:
       elapsed_time
count     20.000000
mean       0.098662
std        0.029080
min        0.076545
25%        0.078220
50%        0.082362
75%        0.116606
max        0.177471
```
