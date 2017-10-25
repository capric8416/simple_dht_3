#! /usr/bin/python
# -*- coding: utf-8 -*-


from multiprocessing.pool import Pool as ProcessPool
from multiprocessing import Manager


manager = Manager()
DATA = manager.d


def f(a):
    DATA.append(a)
    print(a, DATA)


if __name__ == '__main__':
    p = ProcessPool(5)
    p.map(f, range(0, 5))
    p.close()
    p.join()
