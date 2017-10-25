#! /usr/bin/python
# -*- coding: utf-8 -*-


from utils import load_config

CONFIG = load_config()


class Template:
    @property
    def database(self):
        database_template = '{}%0{}x'.format(
            CONFIG['redis_postgres']['database_prefix'],
            len(hex(CONFIG['redis_postgres']['databases'] - 1).partition('0x')[-1])
        )
        return database_template

    @property
    def table(self):
        table_template = '{}%0{}x'.format(
            CONFIG['redis_postgres']['table_prefix'],
            len(hex(CONFIG['redis_postgres']['tables'] - 1).partition('0x')[-1])
        )
        return table_template

    @property
    def name(self):
        name_template = '{}+{}'
        return name_template


def get_databases_tables():
    template = Template()
    db_template = template.database
    table_template = template.table

    databases = [db_template % database_index for database_index in range(0, CONFIG['redis_postgres']['databases'])]
    tables = [table_template % table_index for table_index in range(0, CONFIG['redis_postgres']['tables'])]

    return databases, tables
