#!/usr/bin/env python3

# Measure query performance

from loguru import logger
import sys
import click
import os
import time
import enum
import pandas as pd
# ref: https://pypi.org/project/ascii_graph/
# from ascii_graph import Pyasciigraph

import psycopg
from psycopg.rows import namedtuple_row
from psycopg import DatabaseError

# Command-line option defaults
logging_default = 'INFO'

# For information on supported connection string formats, see
# https://www.cockroachlabs.com/docs/stable/connect-to-the-database.html
url_default = os.environ.get("DATABASE_URL")

db_app_name_default = 'qperf'
query_default = 'SHOW DATABASES'
print_query_results_default = False
query_repetitions_default = 10
warmup_repetitions_default = 5
pause_between_default = 0.1
explicit_tx_default = False
explicit_tx_setting_default = ''
show_individual_timings_default = False


'''
Execute phase of test
'''
def execute_test_phase(phase_warmup, conn, query, print_query_results, query_repetitions, 
                       warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
                       show_individual_timings):
    logger.debug('entered execute_test_phase')
    logger.debug(f'{phase_warmup=}')

    head = 'warmup' if phase_warmup else 'test'

    if not phase_warmup:
        times_df = pd.DataFrame(columns=('obs', 'elaspsed_time'))

    # Execute either the warm-up or the actual test
    for i in range(warmup_repetitions if phase_warmup else query_repetitions):

        # record start time
        if not phase_warmup:
            tic = time.perf_counter()

        # BEGIN
        if explicit_tx:
            cursor = None
            try:
                logger.debug(f'{i}: {head}: executing BEGIN')
                cursor = conn.execute('BEGIN')
                logger.debug(f'{i}: {head}: executed BEGIN')
            finally:
                if cursor is not None:
                    logger.debug(f'{i}: {head}: closing cursor')
                    cursor.close()

        rollback = False

        # Set any explicit transaction setting(s)
        if explicit_tx and len(explicit_tx_setting) > 0 and not rollback:
            cursor = None
            try:
                logger.debug(f'{i}: {head}: executing explicit tx setting(s)')
                cursor = conn.execute(explicit_tx_setting)
                logger.debug(f'{i}: {head}: executed explicit tx setting(s)')
            except DatabaseError:
                rollback = True
            finally:
                if cursor is not None:
                    logger.debug(f'{i}: {head}: closing cursor')
                    cursor.close()
        else:
            if rollback:
                logger.debug(f'{i}: {head}: skipping executing the explicit transaction setting(s) due to previous error')

        # Execute the query
        if not rollback:
            cursor = None
            try:
                logger.debug(f'{i}: {head}: executing query')
                cursor = conn.execute(query)
                logger.debug(f'{i}: {head}: executed query')
                if (not phase_warmup) and print_query_results:
                    print(f'{i}: query results:')
                for record in cursor:
                    if (not phase_warmup) and print_query_results:
                        print(record)
                    else:
                        pass
            except DatabaseError:
                rollback = True
            finally:
                if cursor is not None:
                    logger.debug(f'{i}: {head}: closing cursor')
                    cursor.close()
        else:
            logger.debug(f'{i}: {head}: skipping executing the query due to previous error')

        # COMMIT or ROLLBACK
        if explicit_tx:
            cursor = None
            stmt = 'ROLLBACK' if rollback else 'COMMIT'
            try:
                logger.debug(f'{i}: {head}: executing {stmt}')
                cursor = conn.execute(stmt)
                logger.debug(f'{i}: {head}: executed {stmt}')
            finally:
                if cursor is not None:
                    logger.debug(f'{i}: {head}: closing cursor')
                    cursor.close()

        # record stop time
        if not phase_warmup:
            toc = time.perf_counter()
            elapsed_time = toc - tic
            new_row = pd.DataFrame({'obs':i, 'elapsed_time':elapsed_time}, index=[0])
            times_df = pd.concat([new_row, times_df.loc[:]]).reset_index(drop=True)
            if show_individual_timings:
                print(f'{i}: elapsed time: {elapsed_time:0.5f} sec')

        if pause_between > 0.0:
            logger.debug(f'{i}: {head}: pausing {pause_between} sec')
            time.sleep(pause_between)

    # show statistics
    if not phase_warmup:
        #logger.info(times_df)
        print('Statistics:')
        print(times_df.describe())
#       graph = Pyasciigraph()
#       for line in  graph.graph('histogram', times_df):
#           print(line)    

    logger.debug('leaving execute_test')


'''
Execute test
'''
def execute_test(conn, query, print_query_results, query_repetitions, 
                 warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
                 show_individual_timings):
    logger.debug('entered execute_test')

    execute_test_phase(True,  conn, query, print_query_results, query_repetitions, 
                       warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
                       show_individual_timings)

    execute_test_phase(False, conn, query, print_query_results, query_repetitions, 
                       warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
                       show_individual_timings)

    logger.debug('leaving execute_test')


'''
The main coordinator function
'''
@click.command()
@click.option(
    '--logging',
    type=str,
    default=logging_default,
    help=f'Set logging to the specified level: DEBUG, INFO, WARNING, ERROR, CRITICAL.  Default={logging_default}'
)
@click.option(
    '--url',
    type=str,
    default=url_default,
    help=f'JDBC URL.  Default=DATABASE_URL environment variable'
)
@click.option(
    '--db_app_name',
    type=str,
    default=db_app_name_default,
    help=f'Database application name.  Default={db_app_name_default}'
)
@click.option(
    '--query',
    type=str,
    default=query_default,
    help=f'SQL query.  Default={query_default}'
)
@click.option(
    '--print_query_results',
    type=bool,
    default=print_query_results_default,
    help=f'Whether to print the results of the SQL query.  Default={print_query_results_default}'
)
@click.option(
    '--query_repetitions',
    type=int,
    default=query_repetitions_default,
    help=f'How many times to run the SQL query to gather performance timings.  Default={query_repetitions_default}'
)
@click.option(
    '--warmup_repetitions',
    type=int,
    default=warmup_repetitions_default,
    help=f'For warmup, how many times to run the SQL query before starting to gather performance timings.  Default={warmup_repetitions_default}'
)
@click.option(
    '--pause_between',
    type=float,
    default=pause_between_default,
    help=f'How long to pause between queries (including warm-up queries).  Default={pause_between_default}'
)
@click.option(
    '--explicit_tx',
    type=bool,
    default=explicit_tx_default,
    help=f'Whether to execute the test query inside an explicit transaction (BEGIN...COMMIT).  Default={explicit_tx_default}'
)
@click.option(
    '--explicit_tx_setting',
    type=str,
    default=explicit_tx_setting_default,
    help=f'Optional transaction setting SQL statement(s).  Ignored for implicit transactions.  Default={explicit_tx_setting_default}'
)
@click.option(
    '--show_individual_timings',
    type=bool,
    default=show_individual_timings_default,
    help=f'Whether to show individual timings for each execution of the test SQL.  Default={show_individual_timings_default}'
)
def cli_main(logging, url, db_app_name, query, print_query_results, query_repetitions, 
             warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
             show_individual_timings):
    logger.debug('entered main()')

    if logging != 'WARNING':
        # Remove default logger to reset logging level from the previously-set level of WARNING to
        # something else per https://github.com/Delgan/loguru/issues/51
        logger.remove(loguru_handler_id)
        logger.add(sys.stderr, level=logging)

    logger.debug('entered cli_main()')
    logger.debug(f'parameters:')
    logger.debug(f' {logging=}')
    logger.debug(f' {url=}')
    logger.debug(f' {db_app_name=}')
    logger.debug(f' {query=}')
    logger.debug(f' {print_query_results=}')
    logger.debug(f' {query_repetitions=}')
    logger.debug(f' {warmup_repetitions=}')
    logger.debug(f' {pause_between=}')
    logger.debug(f' {explicit_tx=}')
    logger.debug(f' {explicit_tx_setting=}')
    logger.debug(f' {show_individual_timings=}')

    conn = None

    try:
        logger.debug('connecting to DB')
        logger.debug(f'{url=}')
        conn = psycopg.connect(url,
                               application_name=db_app_name,
                               row_factory=namedtuple_row,
                               autocommit=True)                 # yes, apparently even when using explicit BEGIN...COMMIT
        logger.debug('connected to DB')

        execute_test(conn, query, print_query_results, query_repetitions, 
                     warmup_repetitions, pause_between, explicit_tx, explicit_tx_setting, 
                     show_individual_timings)

    finally:
        if conn is not None:
            logger.debug('closing connection')
            conn.close()

    logger.debug('leaving cli_main()')


'''
Global start for the app
''' 
if __name__ == '__main__':
    try:
        # Remove default logger to reset logging level from the default of DEBUG to something else
        # per https://github.com/Delgan/loguru/issues/51
        logger.remove(0)
        global loguru_handler_id
        loguru_handler_id = logger.add(sys.stderr, level='WARNING')

        cli_main()
    finally:
        logger.debug(f'finishing {__name__}')

