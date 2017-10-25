#! /usr/bin/python
# -*- coding: utf-8 -*-


import os
import sys
import json
import codecs
# import logging
import subprocess
from getopt import getopt


def load_config():
    fp = codecs.open(filename='config.json', encoding='utf-8')
    return json.load(fp=fp, encoding='utf-8')


def parse_command_line_args(argv, *args, **kwargs):
    args_values = {}

    short_opts = []
    long_opts = []

    if not kwargs:
        short_opts = [str(item) for item in args]
        long_opts = short_opts
    else:
        for k, v in kwargs.items():
            short_opts.append(str(k))
            long_opts.append(str(v))

    try:
        opts, args = getopt(
            argv[1:],
            shortopts=''.join([item + ':' for item in short_opts]),
            longopts=[item + '=' for item in long_opts]
        )
        for opt, arg in opts:
            opt = opt.lstrip('-')

            try:
                short_index = short_opts.index(opt)
            except ValueError:
                short_index = -1

            try:
                long_index = long_opts.index(opt)
            except ValueError:
                long_index = -1

            index = short_index if short_index > -1 else long_index

            if index > -1:
                args_values[long_opts[index]] = arg
    except Exception as e:
        _ = e

    return args_values


def kill_process(port=None, include_names=(), exclude_names=()):
    if port:
        pid, _ = subprocess.Popen(
            """lsof -i | grep :%r | awk '{print $2}'""" % port,
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()
    else:
        if 'grep' not in exclude_names:
            exclude_names = list(exclude_names)
            exclude_names.append('grep')

        includes = ' | '.join(['grep {}'.format(name) for name in include_names])
        excludes = ' | '.join(['grep -v {}'.format(name) for name in exclude_names])

        pid, _ = subprocess.Popen(
            """ps -aux | {} | {} | awk '{{print $2}}'""".format(includes, excludes),
            shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()

    if isinstance(pid, bytes):
        pid = pid.decode('utf-8')

    self_pid = os.getpid()

    for item in pid.strip().split('\n'):
        item = item.strip()
        item = int(item) if is_convertible(item, int) else None

        if item and item != self_pid:
            subprocess.Popen('kill -9 {}'.format(item), shell=True).communicate()


def is_convertible(value, t):
    try:
        t(value)
    except Exception as e:
        _ = e
        return False
    else:
        return True


def shell(s, capture_mode=False, quite_mode=False, test_mode=False):
    std_out = []

    line_separator = '-' * 80

    has_std_out = False

    pid = os.getpid()

    if not quite_mode:
        print('{0}\n#{1}# {2}\n{0}'.format(line_separator, pid, s))

    if not test_mode:
        process = subprocess.Popen(s, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

        for std_out_line in iter(process.stdout.readline, b''):
            std_out_line = std_out_line.strip()
            if std_out_line:
                has_std_out = True

                if not quite_mode:
                    print('#{}# {}'.format(pid, std_out_line.decode('utf-8')))

                if capture_mode:
                    std_out.append(std_out_line)

    if not quite_mode:
        print('{}\n\n'.format(line_separator if has_std_out else ''))

    return std_out


def unit_test():
    x = parse_command_line_args(sys.argv, 'o', 't', 'f')
    y = parse_command_line_args(sys.argv, o='one', t='two', f='four')
    z = parse_command_line_args(sys.argv, 'o', 't', 'f', o='one', t='two', f='four')

    print(json.dumps(obj=x, ensure_ascii=False, indent=4))
    print(json.dumps(obj=y, ensure_ascii=False, indent=4))
    print(json.dumps(obj=z, ensure_ascii=False, indent=4))


# if __name__ == '__main__':
#     unit_test()
