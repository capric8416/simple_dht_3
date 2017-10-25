#! /usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
from multiprocessing.dummy import Pool as ThreadPool

import redis

from helpers import Template, get_databases_tables
from utils import load_config, parse_command_line_args, is_convertible, kill_process


CONFIG = load_config()

REDIS_CLIENT = redis.Redis(**CONFIG['redis'])

DATABASES, TABLES = get_databases_tables()

THREADS = 1


def create_torrent_tasks(database):
    name = CONFIG['redis_keys']['torrent_table_tasks'].format(database)
    REDIS_CLIENT.delete(name)
    REDIS_CLIENT.rpush(name, *TABLES)


if __name__ == '__main__':
    command_line_args = parse_command_line_args(
        sys.argv, t='title', a='action', h='threads'
    )
    include_names = [os.path.basename(__file__)]
    title = command_line_args.get('title')
    if title:
        include_names.append(title)

    action = command_line_args.get('action', '')
    if action in {'stop', 'start'}:
        kill_process(include_names=include_names, exclude_names=('screen', 'SCREEN'))

        if action == 'start':
            threads = command_line_args.get('threads')
            if is_convertible(threads, int):
                THREADS = int(threads)

            REDIS_CLIENT.delete(CONFIG['redis_keys']['torrent_database_tasks'])
            REDIS_CLIENT.rpush(CONFIG['redis_keys']['torrent_database_tasks'], *DATABASES)

            thread_pool = ThreadPool(processes=THREADS)
            thread_pool.map(create_torrent_tasks, DATABASES)
