#! /usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
import copy
import logging
import itertools
from multiprocessing import Pool as ProcessPool
from multiprocessing.dummy import Pool as ThreadPool

import redis
import psycopg2

from helpers import Template, get_databases_tables
from utils import load_config, parse_command_line_args, kill_process, is_convertible


CONFIG = load_config()

# CONFIG['postgres']['host'] = '127.0.0.1'

CONFIG['logging']['filename'] = 'postgres.log'
logging.basicConfig(**CONFIG['logging'])

template = Template()
NAME_TEMPLATE = template.name

DATABASES, TABLES = get_databases_tables()

PROCESSES = THREADS = 1


class CreateDataTables:
    def __init__(self, processes, threads):
        self.processes = processes
        self.threads = threads

    @staticmethod
    def create_database(database_name):
        config_postgres = copy.deepcopy(CONFIG['postgres'])
        config_postgres['database'] = 'postgres'
        _connection = psycopg2.connect(**config_postgres)

        sql_create_database = 'CREATE DATABASE {};'.format(database_name)
        print(sql_create_database)

        _connection.set_isolation_level(0)
        _connection.cursor().execute(sql_create_database)
        _connection.close()

        config_postgres = copy.deepcopy(CONFIG['postgres'])
        config_postgres['database'] = database_name
        _connection = psycopg2.connect(**config_postgres)

        return _connection

    @staticmethod
    def _thread_method(connection, table):
        sql_create_table = '''
            CREATE TABLE {table}
            (
                id serial NOT NULL,
                info_hash character(40) NOT NULL,
                score bigint NOT NULL,
                collected_time timestamp with time zone NOT NULL,
                is_invalid boolean DEFAULT TRUE NOT NULL,
                CONSTRAINT {table}_pkey_id PRIMARY KEY (id),
                CONSTRAINT {table}_key_info_hash UNIQUE (info_hash)
            );
        '''.format(**{'table': table})
        print(sql_create_table)

        connection.cursor().execute(sql_create_table)
        connection.commit()

    def _process_method(self, database):
        connection = self.create_database(database)

        thread_pool = ThreadPool(processes=self.threads)
        thread_pool.starmap(self._thread_method, zip(itertools.repeat(connection), TABLES))
        thread_pool.close()
        thread_pool.join()

        connection.close()

    def start(self):
        process_pool = ProcessPool(processes=self.processes)
        process_pool.map(self.create_database, DATABASES)
        process_pool.close()
        process_pool.join()


class AlterPostgres:
    def __init__(self, processes, threads):
        self.processes = processes
        self.threads = threads

    @staticmethod
    def _thread_method(connection, database, table):
        sql_alter = '''
            ALTER TABLE {table} RENAME status to is_invalid;
            ALTER TABLE {table} RENAME update_time to collected_time;
        '''.format(table=table)
        logging.info(database + sql_alter)

        cursor = connection.cursor()
        cursor.execute(sql_alter)
        connection.commit()

    def _process_method(self, database):
        config_postgres = copy.deepcopy(CONFIG['postgres'])
        config_postgres['database'] = database
        connection = psycopg2.connect(**config_postgres)

        thread_pool = ThreadPool(processes=self.threads)
        thread_pool.starmap(
            self._thread_method, zip(itertools.repeat(connection), itertools.repeat(database), TABLES)
        )
        thread_pool.close()
        thread_pool.join()

        connection.close()

    def start(self):
        process_pool = ProcessPool(processes=self.processes)
        process_pool.map(self._process_method, DATABASES)
        process_pool.close()
        process_pool.join()


class Transfer:
    def __init__(self, processes, threads):
        self.processes = processes
        self.threads = threads

        self.redis_client = redis.Redis(**CONFIG['redis'])

    def _thread_method(self, connection, database, table):
        cursor = connection.cursor()

        name = NAME_TEMPLATE.format(database, table)
        pipeline = self.redis_client.pipeline()
        pipeline.zrange(name=name, start=0, end=-1, withscores=True)
        pipeline.delete(name)
        data, _ = pipeline.execute()

        for info_hash, score in data:
            info_hash = info_hash.decode('utf-8')
            sql_insert = '''
                INSERT INTO {table}
                (info_hash, score, collected_time)
                VALUES
                ('{info_hash}', {score}, current_timestamp);
            '''.format(table=table, info_hash=info_hash, score=score)

            try:
                cursor.execute(sql_insert)
            except psycopg2.IntegrityError:
                connection.rollback()
            except Exception as e:
                logging.error(e)
            else:
                logging.info('{}, {}'.format(info_hash, score))
                connection.commit()

    def _process_method(self, database):
        config_postgres = copy.deepcopy(CONFIG['postgres'])
        config_postgres['database'] = database
        connection = psycopg2.connect(**config_postgres)

        thread_pool = ThreadPool(processes=self.threads)
        thread_pool.starmap(
            self._thread_method, zip(itertools.repeat(connection), itertools.repeat(database), TABLES)
        )
        thread_pool.close()
        thread_pool.join()

        connection.close()

    def start(self):
        process_pool = ProcessPool(processes=self.processes)
        process_pool.map(self._process_method, DATABASES)

        process_pool.close()
        process_pool.join()


class CountFromPostgres:
    def __init__(self, processes, threads):
        self.processes = processes
        self.threads = threads

    @staticmethod
    def _thread_method(connection, table):
        sql_get_count = '''SELECT id from {table} ORDER BY id DESC LIMIT 1;'''.format(table=table)

        cursor = connection.cursor()
        cursor.execute(sql_get_count)
        connection.commit()

        count_from_table = cursor.fetchone()
        count_from_table = count_from_table[0] if count_from_table else 0

        logging.info(count_from_table)
        return count_from_table

    def _process_method(self, database):
        config_postgres = copy.deepcopy(CONFIG['postgres'])
        config_postgres['database'] = database
        connection = psycopg2.connect(**config_postgres)

        thread_pool = ThreadPool(processes=self.threads)
        results = thread_pool.starmap(self._thread_method, zip(itertools.repeat(connection), TABLES))
        thread_pool.close()
        thread_pool.join()

        connection.close()

        return sum(results)

    def start(self):
        count_from_database = 0

        process_pool = ProcessPool(processes=self.processes)
        results = process_pool.map(self._process_method, DATABASES)
        process_pool.close()
        process_pool.join()

        count_from_database += sum(results)

        return count_from_database


class CountFromRedis:
    def __init__(self, processes, threads):
        self.processes = processes
        self.threads = threads
        self.redis_client = redis.Redis(**CONFIG['redis'])

    def _thread_method(self, database, table):
        name = NAME_TEMPLATE.format(database, table)
        return self.redis_client.zcard(name)

    def _process_method(self, database):
        thread_pool = ThreadPool(processes=self.threads)
        results = thread_pool.starmap(self._thread_method, zip(itertools.repeat(database), TABLES))
        thread_pool.close()
        thread_pool.join()

        return sum(results)

    def start(self):
        process_pool = ProcessPool(processes=self.processes)
        results = process_pool.map(self._process_method, DATABASES)

        process_pool.close()
        process_pool.join()

        return sum(results)


if __name__ == '__main__':
    command_line_args = parse_command_line_args(sys.argv, u='uuid1', a='action', c='class', p='processes', t='threads')

    include_names = [os.path.basename(__file__)]
    uuid1 = command_line_args.get('uuid1')
    if uuid1:
        include_names.append(uuid1)

    action = command_line_args.get('action', '')
    if action in {'stop', 'start'}:
        kill_process(include_names=include_names, exclude_names=('screen', 'SCREEN'))

        if action == 'start':
            p = command_line_args.get('processes')
            if is_convertible(p, int):
                PROCESSES = int(p)

            t = command_line_args.get('threads')
            if is_convertible(t, int):
                THREADS = int(t)

            c = command_line_args.get('class')
            c = locals().get(c) if c else None

            if not c:
                print('class `{}` not found'.format(action))
            else:
                result = c(processes=PROCESSES, threads=THREADS).start()
                print(result)
