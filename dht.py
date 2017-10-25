#!/usr/bin/env python
# encoding: utf-8


import os
import signal
import asyncio
import binascii
from struct import unpack
from socket import inet_ntoa

import bencoder

from utils import load_config


CONFIG = load_config()
BOOTSTRAP_NODES = tuple([tuple(item) for item in CONFIG['bootstrap_nodes']])


class DHT(asyncio.DatagramProtocol):
    def __init__(self, loop=None, bootstrap_nodes=BOOTSTRAP_NODES, interval=1):
        self.node_id = self.random_node_id()
        self.transport = None
        self.loop = loop or asyncio.get_event_loop()
        self.bootstrap_nodes = bootstrap_nodes
        self.is_running = False
        self.interval = interval

    def stop(self):
        self.is_running = False
        self.loop.call_later(self.interval, self.loop.stop)

    def run(self, port):
        datagram_endpoint = self.loop.create_datagram_endpoint(
            lambda: self, local_addr=('0.0.0.0', port)
        )
        transport, _ = self.loop.run_until_complete(datagram_endpoint)

        for signal_name in ('SIGINT', 'SIGTERM'):
            self.loop.add_signal_handler(getattr(signal, signal_name), self.stop)

        for node in self.bootstrap_nodes:
            # Bootstrap
            self.find_node(address=node, node_id=self.node_id)

        asyncio.ensure_future(self.auto_find_nodes(), loop=self.loop)
        self.loop.run_forever()
        self.loop.close()

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.is_running = False
        self.transport.close()

    def datagram_received(self, data, address):
        try:
            msg = bencoder.bdecode(data)
        except Exception as e:
            _ = e
            return

        try:
            self.handle_message(msg, address)
        except Exception as e:
            self.send_message(
                data={
                    't': msg['t'],
                    'y': 'e',
                    'e': [202, 'Server Error']
                },
                address=address
            )
            raise e

    def handle_message(self, msg, address):
        msg_type = msg.get(b'y', b'e')

        return {
            b'e': None,
            b'r': self.handle_response(msg, address=address),
            b'q': asyncio.ensure_future(self.handle_query(msg, address=address), loop=self.loop)
        }.get(msg_type, None)

    def handle_response(self, msg, address):
        _ = address

        args = msg[b'r']
        if b'nodes' in args:
            for node_id, ip, port in self.split_nodes(args[b'nodes']):
                self.ping(address=(ip, port))

    async def handle_get_peers(self, info_hash, address):
        await self.handler(info_hash, address)

    async def handle_announce_peer(self, info_hash, address):
        await self.handler(info_hash, address)

    async def handler(self, info_hash, address):
        raise NotImplementedError()

    async def handle_query(self, msg, address):
        args = msg[b'a']
        node_id = args[b'id']
        query_type = msg[b'q']
        if query_type == b'get_peers':
            info_hash = args[b'info_hash']
            info_hash = self.decode_info_hash(info_hash)
            token = info_hash[:2]
            self.send_message(
                data={
                    't': msg[b't'],
                    'y': 'r',
                    'r': {
                        'id': self.fake_node_id(node_id),
                        'nodes': '',
                        'token': token
                    }
                },
                address=address
            )
            await self.handle_get_peers(info_hash, address)
        elif query_type == b'announce_peer':
            info_hash = args[b'info_hash']
            tid = msg[b't']
            self.send_message(
                data={
                    't': tid,
                    'y': 'r',
                    'r': {
                        'id': self.fake_node_id(node_id)
                    }
                },
                address=address
            )
            await self.handle_announce_peer(self.decode_info_hash(info_hash), address)
        elif query_type == b'find_node':
            tid = msg[b't']
            self.send_message(
                data={
                    't': tid,
                    'y': 'r',
                    'r': {
                        'id': self.fake_node_id(node_id),
                        'nodes': ''
                    }
                },
                address=address
            )
        elif query_type == b'ping':
            self.send_message(
                data={
                    't': b'tt',
                    'y': 'r',
                    'r': {
                        'id': self.fake_node_id(node_id)
                    }
                },
                address=address
            )
        self.find_node(address=address, node_id=node_id)

    async def auto_find_nodes(self):
        self.is_running = True
        while self.is_running:
            await asyncio.sleep(self.interval)
            for node in self.bootstrap_nodes:
                self.find_node(address=node)

    def ping(self, address, node_id=None):
        self.send_message(
            data={
                'y': 'q',
                't': 'pg',
                'q': 'ping',
                'a': {
                    'id': self.fake_node_id(node_id)
                }
            },
            address=address
        )

    def send_message(self, data, address):
        data.setdefault('t', b'tt')
        self.transport.sendto(bencoder.bencode(data), address)

    def fake_node_id(self, node_id=None):
        if node_id:
            return node_id[:-1] + self.node_id[-1:]
        return self.node_id

    def find_node(self, address, node_id=None, target=None):
        if not target:
            target = self.random_node_id()
        self.send_message(
            data={
                't': b'fn',
                'y': 'q',
                'q': 'find_node',
                'a': {
                    'id': self.fake_node_id(node_id),
                    'target': target
                }
            },
            address=address
        )

    @staticmethod
    def decode_info_hash(info_hash):
        if isinstance(info_hash, bytes):
            # Convert bytes to hex
            info_hash = binascii.hexlify(info_hash).decode('utf-8')

        return info_hash.lower()

    @staticmethod
    def random_node_id(size=20):
        return os.urandom(size)

    @staticmethod
    def split_nodes(nodes):
        length = len(nodes)
        if (length % 26) != 0:
            return

        for i in range(0, length, 26):
            nid = nodes[i:i + 20]
            ip = inet_ntoa(nodes[i + 20:i + 24])
            port = unpack('!H', nodes[i + 24:i + 26])[0]
            yield nid, ip, port
