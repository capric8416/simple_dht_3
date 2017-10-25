#! /usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
import copy
import json
import time
import threading
import logging
from multiprocessing import Pool as ProcessPool

import redis
import psycopg2
import websocket
from elasticsearch import Elasticsearch

from helpers import Template
from utils import load_config, parse_command_line_args, kill_process, shell, is_convertible

CONFIG = load_config()

CONFIG['logging']['filename'] = 'torrent.log'
logging.basicConfig(**CONFIG['logging'])

REDIS_CLIENT = redis.Redis(**CONFIG['redis'])
ES_CLIENT = Elasticsearch(**CONFIG['elastic_search'])

ACTION = CONFIG['web_socket']['keys']['action']
DATA = CONFIG['web_socket']['keys']['data']
POSTFIX = CONFIG['web_socket']['keys']['postfix']
POSTFIX_DATABASE = CONFIG['web_socket']['postfixes']['database']
POSTFIX_TABLE = CONFIG['web_socket']['postfixes']['table']
TASK_OPEN = CONFIG['web_socket']['actions']['task_open']
TASK_INFO = CONFIG['web_socket']['actions']['task_info']
TASK_CLOSE = CONFIG['web_socket']['actions']['task_close']

template = Template()
DATABASE_TEMPLATE = template.database
TABLE_TEMPLATE = template.table

PROCESSES = 0


def connect_postgres(database):
    config_postgres = copy.deepcopy(CONFIG['postgres'])
    config_postgres['database'] = database
    connection = psycopg2.connect(**config_postgres)
    cursor = connection.cursor()

    return connection, cursor


def format_database_table(postfix):
    database = DATABASE_TEMPLATE % int(postfix[POSTFIX_DATABASE], 16)
    table = TABLE_TEMPLATE % int(postfix[POSTFIX_TABLE], 16)
    return database, table


class WebSocketTasks:
    def __init__(self, port, info_hashes=()):
        self.port = port
        self.info_hashes = info_hashes

    def add_info_hashes(self, info_hashes):
        self.info_hashes = info_hashes

    @staticmethod
    def on_message(ws, message):
        body = json.loads(s=message, encoding='utf-8')
        if body[ACTION] == TASK_INFO:
            print(json.dumps(body[DATA], ensure_ascii=False, indent=4), end='\n' * 2)

            info_hash = body[DATA]['info_hash']
            ws.valid_info_hashes.append(info_hash)

            index_data = copy.deepcopy(CONFIG['es_index_type']['torrent_info'])
            index_data.update({'id': body[DATA]['info_hash'], 'body': body[DATA]})
            print(index_data)
            ES_CLIENT.index(**index_data)
        elif body[ACTION] == TASK_CLOSE:
            postfix = body[POSTFIX]
            database, table = format_database_table(postfix)
            connection, cursor = connect_postgres(database)

            valid_info_hashes = ','.join(["'{}'".format(item) for item in ws.valid_info_hashes if item])
            if valid_info_hashes:
                sql_update = '''
                    UPDATE {} SET is_invalid = FALSE WHERE info_hash IN ({});
                '''.format(table, valid_info_hashes)
                print(sql_update)
                # cursor.execute(sql_update)
                # connection.commit()

            connection.close()
            ws.close()

    @staticmethod
    def on_error(ws, error):
        _, _ = ws, error

    @staticmethod
    def on_close(ws):
        _ = ws

    def run_client(self):
        def on_open(ws):
            def send_info_hashes():
                ws.valid_info_hashes = []
                message = {ACTION: TASK_OPEN, DATA: self.info_hashes}
                ws.send(json.dumps(obj=message, ensure_ascii=False))

            thread = threading.Thread(target=send_info_hashes)
            thread.start()

        websocket.WebSocketApp(
            url='ws://localhost:{}/'.format(self.port),
            on_open=on_open, on_error=self.on_error,
            on_close=self.on_close, on_message=self.on_message
        ).run_forever()

    def run_server(self):
        def node_js():
            shell(s='node torrent.js --port={}'.format(self.port))

        thread = threading.Thread(target=node_js)
        thread.start()


def get_torrents(port):
    while True:
        database = REDIS_CLIENT.lpop(CONFIG['redis_keys']['torrent_database_tasks'])
        if not database:
            break

        database = database.decode('utf-8')

        while True:
            table = REDIS_CLIENT.lpop(CONFIG['redis_keys']['torrent_table_tasks'].format(database))
            if not table:
                break

            table = table.decode('utf-8')

            print('\n\n** database: {} \t table: {}\n\n'.format(database, table))

            connection, cursor = connect_postgres(database)

            sql_select = '''SELECT id, info_hash FROM {} WHERE is_invalid = TRUE ORDER BY id;'''.format(table)
            cursor.execute(sql_select)
            connection.commit()
            info_hashes = cursor.fetchall()

            kill_process(port=port)

            tasks = WebSocketTasks(port=port)
            tasks.run_server()
            time.sleep(1)

            tasks.add_info_hashes(info_hashes=[info_hash for _, info_hash in info_hashes])
            tasks.run_client()

            connection.close()


if __name__ == '__main__':
    command_line_args = parse_command_line_args(sys.argv, s='sha1', a='action', p='processes')

    include_names = [os.path.basename(__file__)]
    sha1 = command_line_args.get('sha1')
    if sha1:
        include_names.append(sha1)

    action = command_line_args.get('action', '')
    if action in {'stop', 'start'}:
        kill_process(include_names=include_names, exclude_names=('screen', 'SCREEN'))

        if action == 'start':
            processes = command_line_args.get('processes')
            if is_convertible(processes, int):
                PROCESSES = int(processes)

            if PROCESSES > 0:
                ports = range(CONFIG['torrent']['port'], CONFIG['torrent']['port'] + PROCESSES)

                process_pool = ProcessPool(processes=PROCESSES)
                process_pool.map(get_torrents, ports)
                process_pool.close()
                process_pool.join()
