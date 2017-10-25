#! /usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
import time
import logging
from datetime import datetime
from multiprocessing import Pool as ProcessPool

import redis

from dht import DHT
from utils import load_config, kill_process, parse_command_line_args, is_convertible


CONFIG = load_config()

CONFIG['logging']['filename'] = 'magnet.log'
logging.basicConfig(**CONFIG['logging'])

PROCESSES = 0


class MagnetCrawler(DHT):
    def __init__(self, *args, **kwargs):
        super(MagnetCrawler, self).__init__(*args, **kwargs)

        self.redis_client = redis.Redis(**CONFIG['redis'])

        self.name_template = '{}{{}}+{}{{}}'.format(
            CONFIG['redis_postgres']['database_prefix'],
            CONFIG['redis_postgres']['table_prefix']
        )
        self.db_prefix_length = len(hex(CONFIG['redis_postgres']['databases'] - 1).partition('0x')[-1])
        self.tb_prefix_length = len(hex(CONFIG['redis_postgres']['tables'] - 1).partition('0x')[-1])

        self.second_to_microsecond = pow(10, 6)

    def _get_score(self):
        dt = datetime.now()
        score = time.mktime(dt.timetuple()) * self.second_to_microsecond + dt.microsecond
        return score

    def _get_name(self, info_hash):
        name = self.name_template.format(info_hash[:self.db_prefix_length], info_hash[self.db_prefix_length:])
        return name

    def _redis(self, info_hash):
        score = self._get_score()
        name = self._get_name(info_hash[:self.db_prefix_length + self.tb_prefix_length])
        if self.redis_client.zadd(name, info_hash, score):
            logging.info(info_hash)

    async def handler(self, info_hash, address):
        self._redis(info_hash)


def get_magnets(port):
    crawler = MagnetCrawler()
    crawler.run(port)


if __name__ == '__main__':
    command_line_args = parse_command_line_args(sys.argv, s='sha1', a='action', p='processes')

    include_names = [os.path.basename(__file__)]
    sha1 = command_line_args.get('uuid1')
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
                ports = range(CONFIG['magnet']['port'], CONFIG['magnet']['port'] + PROCESSES)

                process_pool = ProcessPool(processes=PROCESSES)
                process_pool.map(get_magnets, ports)
                process_pool.close()
                process_pool.join()
