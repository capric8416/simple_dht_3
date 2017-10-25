#!/usr/bin/env python3

import os
import sys
import json
import struct
import logging
import asyncio
import hashlib
import ipaddress
from random import randint
from binascii import hexlify
from base64 import b16decode
from logging import StreamHandler
from socket import inet_aton, inet_ntoa
from concurrent.futures import FIRST_COMPLETED, CancelledError


import aiodns
from bencodepy import encode as bencode, decode as bdecode, DecodingError


HANDSHAKE = 1
MESSAGE_LEN = 2
MESSAGE_TYPE = 3
MESSAGE_PAYLOAD = 4

TIMEOUT = 5
RETRIES = 2

nodes = None
all_peers = set()
metadata_size = 0
metadata = set()
full_metadata = b''
keep_running = False
get_peers_in_progress = 0
get_metadatas_in_progress = 0

logger = logging.getLogger('ih2torrent')
handler = StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('*%(levelname)s* [%(asctime)s] %(message)s')
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)


class SetQueue(asyncio.Queue):
    def _init(self, maxsize):
        self._queue = set()

    def _put(self, item):
        self._queue.add(item)

    def _get(self):
        return self._queue.pop()


values = SetQueue()


# A SortedQueue is constructed from an info_hash, internally it removes
# duplicates, sorts items put into it based on their distance to the
# given info_hash and yields the closer ones to the info_hash first when
# asked.
class SortedQueue(asyncio.Queue):
    def __init__(self, info_hash):
        super(SortedQueue, self).__init__()
        self.info_hash = info_hash

    def _init(self, maxsize):
        self._queue = []

    def _put(self, item):
        if item not in self._queue:
            self._queue.append(item)
            self._queue.sort(key=lambda i: -distance(i, self.info_hash))

    def _get(self):
        return self._queue.pop()


class BitTorrentProtocol:
    def __init__(self, info_hash, peer_id):
        self.handshake_complete = asyncio.Event()
        self.extended_handshake_complete = asyncio.Event()
        self.metadata_block_received = asyncio.Event()
        self.error = asyncio.Event()

        self.info_hash = info_hash
        self.peer_id = peer_id

        self.state = HANDSHAKE
        self.field_len = 68
        self.field = b''
        self.leftover = b''
        self.metadata_size = 0
        self.metadata_block = b''

        self.message_types = {
            0: 'CHOKE',
            1: 'UNCHOKE',
            2: 'INTERESTED',
            3: 'NOT INTERESTED',
            4: 'HAVE',
            5: 'BITFIELD',
            6: 'REQUEST',
            7: 'PIECE',
            8: 'CANCEL',
            9: 'PORT',
            13: 'SUGGEST PIECE',
            14: 'HAVE ALL',
            15: 'HAVE NONE',
            16: 'REJECT REQUEST',
            17: 'ALLOWED FAST',
            20: 'EXTENDED'
        }
        
        self.transport = None
        self.message_len = None
        self.message_type = None
        self.extended_message_types = None

    def connection_made(self, transport):
        self.transport = transport
        self.transport.write(
            b'\x13BitTorrent protocol'
            b'\x00\x00\x00\x00\x00\x10\x00\x04' +
            self.info_hash + self.peer_id
        )

    def data_received(self, data):
        data = self.leftover + data
        len_field_data = len(self.field) + len(data)

        if len_field_data < self.field_len:
            self.field += data
            self.leftover = b''
        elif len_field_data == self.field_len:
            self.field += data
            self.leftover = b''
            self.parse_field()
        else:
            n = self.field_len - len(self.field)
            self.field += data[:n]
            self.leftover = data[n:]
            self.parse_field()

            if len(self.leftover) >= self.field_len and not self.error.is_set():
                self.data_received(b'')

    def eof_received(self):
        logger.debug('EOF received.')
        self.error.set()

    def connection_lost(self, exc):
        logger.debug('Connection lost: {}'.format(exc))
        self.error.set()

    def parse_field(self):
        if self.state == HANDSHAKE:
            if not self.field[:20] == b'\x13BitTorrent protocol':
                logger.debug('Peer does not support BitTorrent protocol.')
                self.error.set()
                return

            if int.from_bytes(self.field[20:28], byteorder='big') & 0x0000000000100000 == 0:
                logger.debug('Peer does not support extension protocol.')
                self.error.set()
                return

            if int.from_bytes(self.field[20:28], byteorder='big') & 0x0000000000000004 == 0:
                logger.debug('Peer does not support fast protocol.')
                self.error.set()
                return

            self.state = MESSAGE_LEN
            self.field_len = 4
            self.handshake_complete.set()

            extended_handshake = bencode({
                'm': {b'ut_metadata': 2},
                'v': 'S.P.E.W.'
            })
            self.write_extended_message(0, extended_handshake)
            logger.debug('Sent extended handshake.')
        elif self.state == MESSAGE_LEN:
            self.message_len = int.from_bytes(self.field, byteorder='big')
            if self.message_len == 0:
                self.state = MESSAGE_LEN
                self.field = 4
            else:
                self.state = MESSAGE_TYPE
                self.field_len = 1
        elif self.state == MESSAGE_TYPE:
            self.message_type = int.from_bytes(self.field, byteorder='big')
            if self.message_len == 1:
                self.state = MESSAGE_LEN
                self.field = 4
            else:
                self.message_len -= 1
                self.field_len = self.message_len
                self.state = MESSAGE_PAYLOAD
        elif self.state == MESSAGE_PAYLOAD:
            self.parse_message()
            self.field_len = 4
            self.state = MESSAGE_LEN
        else:
            logger.error('Invalid state.')
            self.error.set()

        self.field = b''

    def parse_message(self):
        logger.debug(self.message_types.get(self.message_type, 'UNKNOWN MESSAGE'))
        if self.message_type == 20:
            self.parse_extended_message()

    def parse_extended_message(self):
        extended_message_type = self.field[0]
        message = self.field[1:]

        if extended_message_type == 0:
            message = self.try_decode_message(message)
            if not message:
                return

            if b'm' not in message:
                logger.debug('"m" not in extended handshake.')
                self.error.set()
                return

            self.extended_message_types = message[b'm']

            if b'ut_metadata' not in self.extended_message_types:
                logger.debug('Peer does not support metadata protocol.')
                self.error.set()
                return

            if b'metadata_size' not in message:
                logger.debug('Peer did not send "metadata_size" in extended handshake.')
                self.error.set()
                return

            self.metadata_size = message[b'metadata_size']
            logger.info('metadata size: {}'.format(self.metadata_size))
            self.extended_handshake_complete.set()

            self.write_message(15, b'')  # have none
            logger.debug('Sent HAVE NONE.')
            self.write_message(0, b'')  # choke
            logger.debug('Sent CHOKE.')
            self.write_message(3, b'')  # not interested
            logger.debug('Sent NOT INTERESTED.')
        elif extended_message_type == self.extended_message_types[b'ut_metadata']:
            original_message = message
            message = self.try_decode_message(message)
            if not message:
                return

            if message[b'msg_type'] == 0:
                reply = {
                    'msg_type': 2,
                    'piece': message[b'piece']
                }
                _ = reply
            elif message[b'msg_type'] == 1:
                size = len(original_message) - len(bencode(message))
                logger.debug('Got a metadata block of size: {}'.format(size))

                self.metadata_block = original_message[-size:]
                self.metadata_block_received.set()
            elif message[b'msg_type'] == 2:
                logger.debug('Request for metadata rejected.')
                return

    def try_decode_message(self, message):
        try:
            message = bdecode(message)
        except DecodingError:
            self.error.set()
            message = None

        return message

    def get_metadata_block(self, n):
        logger.info('Requesting piece {} of metadata.'.format(n))

        msg = bencode({
            'msg_type': 0,
            'piece': n
        })
        self.write_extended_message(self.extended_message_types[b'ut_metadata'], msg)

    def write_message(self, msg_type, msg):
        msg_len = 1 + len(msg)
        self.transport.write(msg_len.to_bytes(length=4, byteorder='big') + bytes([msg_type]) + msg)

    def write_extended_message(self, ex_type, msg):
        self.write_message(20, bytes([ex_type]) + msg)


class DHTProtocol:
    def __init__(
        self, query_type, node_id,
        target=None, info_hash=None,
        implied_port=None, port=None,
        token=None, loop=None
    ):
        self.query_type = query_type
        self.node_id = node_id
        self.target = target
        self.info_hash = info_hash
        self.implied_port = implied_port
        self.port = port
        self.token = token

        self.tid = struct.pack('!H', randint(0, 65535))
        self.reply_received = asyncio.Event(loop=loop)
        
        self.info_hash = info_hash
        
        self.transport = None
        self.reply = None

    def construct_message(self):
        args = {
            'ping': {
                'id': self.node_id
            },
            'find_node': {
                'id': self.node_id,
                'target': self.target
            },
            'get_peers': {
                'id': self.node_id,
                'info_hash': self.info_hash
            },
            'announce_peer': {
                'id': self.node_id,
                'implied_port': self.implied_port,
                'info_hash': self.info_hash,
                'port': self.port,
                'token': self.token
            }
        }.get(self.query_type, None)

        if not args:
            raise RuntimeError('Invalid DHT query type: {}'.format(self.query_type))

        return bencode({
            't': self.tid,
            'y': 'q',
            'q': self.query_type,
            'a': args
        })

    def connection_made(self, transport):
        self.transport = transport
        self.send_message()

    def send_message(self):
        logger.debug('Sending DHT query.')

        message = self.construct_message()
        self.transport.sendto(message)

    def datagram_received(self, data, address):
        message = self.try_decode_message(data)
        if not message:
            logger.debug('Received invalid bencoding in reply. Discarded.')
            return

        if b't' not in message:
            logger.debug('Received invalid reply. Discarded')
            return

        if message[b't'] != self.tid:
            logger.debug('Received reply with invalid transaction ID. Discarded.')
            return

        if b'r' not in message or b'id' not in message[b'r']:
            logger.debug('Received invalid reply. Discarded.')
            return

        logger.debug('Received DHT reply from {}:{} with node ID {}.'.format(
            address[0], address[1], hexlify(message[b'r'][b'id']).decode())
        )

        self.reply = message[b'r']
        self.reply_received.set()

    @staticmethod
    def try_decode_message(message):
        try:
            message = bdecode(message)
        except DecodingError:
            message = None

        return message

    def error_received(self, exc):
        pass

    def connection_lost(self, exc):
        pass

    def retry(self):
        logger.debug('Retrying...')
        self.send_message()


@asyncio.coroutine
def ping(loop, node_id, host, port):
    transport, protocol = None, None
    
    try:
        transport, protocol = yield from loop.create_datagram_endpoint(
            lambda: DHTProtocol('ping', node_id=node_id, loop=loop), remote_addr=(host, port)
        )
    except OSError as e:
        logger.debug('Error opening socket for "ping": {}'.format(e))

    for i in range(RETRIES):
        try:
            yield from asyncio.wait_for(protocol.reply_received.wait(), timeout=TIMEOUT)
        except asyncio.TimeoutError:
            protocol.retry()
        else:
            break

    transport.close()
    if protocol.reply_received.is_set():
        logger.debug('Reply:', protocol.reply)
        logger.debug('Done.')
    else:
        logger.debug('No reply received.')
    return protocol.reply_received.is_set()


@asyncio.coroutine
def get_peers(loop, node_id, host, port, info_hash):
    global get_peers_in_progress
    get_peers_in_progress += 1

    try:
        try:
            transport, protocol = yield from loop.create_datagram_endpoint(
                lambda: DHTProtocol('get_peers', node_id=node_id, info_hash=info_hash), remote_addr=(host, port)
            )
        except OSError as e:
            logger.debug('Error opening socket for get_peers: {}'.format(e))
            return

        for i in range(RETRIES):
            try:
                yield from asyncio.wait_for(protocol.reply_received.wait(), timeout=TIMEOUT)
            except asyncio.TimeoutError:
                protocol.retry()
            else:
                break

        transport.close()
        if not protocol.reply_received.is_set():
            logger.debug('get_peers: No reply received.')
            return

        if b'values' in protocol.reply:
            peers = protocol.reply[b'values']
            for p in peers:
                if len(p) != 6:
                    logger.debug('Invalid peer "{}". Ignored.'.format(repr(p)))
                else:
                    all_peers.add(p)
                    yield from values.put(p)
        elif b'nodes' in protocol.reply:
            peers = protocol.reply[b'nodes']
            peers = [peers[i:i + 26] for i in range(0, len(peers), 26)]
            for p in peers:
                yield from nodes.put(p[20:])
    finally:
        get_peers_in_progress -= 1


@asyncio.coroutine
def dns_resolve(resolver, name):
    logger.info('Resolving: {}'.format(name))
    try:
        result = yield from resolver.query(name, 'A')
    except aiodns.error.DNSError:
        logger.error('Could not resolve name: {}'.format(name))
        return None

    return result[0].host


@asyncio.coroutine
def get_metadata(loop, node_id, host, port, info_hash):
    global metadata, metadata_size, keep_running, full_metadata, get_metadatas_in_progress

    if not keep_running:
        return True

    get_metadatas_in_progress += 1

    try:
        logger.info('Getting metadata from: {}:{}'.format(host, port))

        try:
            transport, protocol = yield from loop.create_connection(
                lambda: BitTorrentProtocol(info_hash, node_id), host, port
            )
        except OSError as e:
            logger.debug('Connection error: {}'.format(e))
            return False

        logger.debug('Connected to peer: {}:{}'.format(host, port))

        done, pending = yield from asyncio.wait(
            [protocol.handshake_complete.wait(), protocol.error.wait()],
            return_when=FIRST_COMPLETED,
            timeout=TIMEOUT
        )

        for task in pending:
            task.cancel()

        if not done or protocol.error.is_set():
            logger.debug('Error communicating with the peer while waiting for the handshake.')
            transport.close()
            return False

        done, pending = yield from asyncio.wait(
            [protocol.extended_handshake_complete.wait(), protocol.error.wait()],
            return_when=FIRST_COMPLETED,
            timeout=TIMEOUT
        )

        for task in pending:
            task.cancel()

        if not done or protocol.error.is_set():
            logger.debug('Error communicating with the peer while waiting for the extended handshake.')
            transport.close()
            return False

        if metadata_size > 0 and metadata_size != protocol.metadata_size:
            logger.warning('Inconsistent metadata size received.')

        metadata_size = protocol.metadata_size
        metadata_nblocks = int(metadata_size / (16 * 1024))
        metadata_nblocks += 0 if metadata_size % (16 * 1024) == 0 else 1

        while keep_running:
            protocol.metadata_block_received.clear()

            try:
                i = next(i for i in range(metadata_nblocks) if i not in [m[0] for m in metadata])
            except StopIteration:
                transport.close()
                return True

            protocol.get_metadata_block(i)

            done, pending = yield from asyncio.wait(
                [protocol.metadata_block_received.wait(),
                 protocol.error.wait()],
                return_when=FIRST_COMPLETED,
                timeout=TIMEOUT)

            for task in pending:
                task.cancel()

            if not done or protocol.error.is_set():
                logger.debug('Error communicating with the peer while waiting for metadata block.')
                transport.close()
                return False

            metadata.add((i, protocol.metadata_block))

            if {m[0] for m in metadata} == set(range(metadata_nblocks)):
                # metadata complete. hash check.
                m = hashlib.sha1()
                full_metadata = b''
                for i, b in sorted(metadata, key=lambda m: m[0]):
                    full_metadata += b
                    m.update(b)
                if m.digest() != info_hash:
                    logger.debug('Invalid metadata received. Hash does not checkout. Discarding.')
                    metadata_size = 0
                    metadata = set()
                    return False

                logger.info('Metadata received.')
                full_metadata = bdecode(full_metadata)
                keep_running = False
    finally:
        get_metadatas_in_progress -= 1

    return True


@asyncio.coroutine
def get_metadata_with_retries(loop, node_id, host, port, info_hash):
    for i in range(RETRIES):
        ret = yield from get_metadata(loop, node_id, host, port, info_hash)
        if ret:
            break

        logger.debug('Retrying get_metadata...')


def distance(i, ih):
    def byte_distance(x, y):
        return bin(x ^ y).count('1')
    
    return sum(byte_distance(b1, b2) for b1, b2 in zip(ih, i))


def get_closest_nodes(k, info_hash):
    return sorted(all_peers, key=lambda x: distance(x, info_hash))[:k]


@asyncio.coroutine
def info_hash_to_torrent(loop, node_id, info_hash):
    global keep_running

    logger.info('Using node ID: {}'.format(hexlify(node_id).decode()))

    # Add bootstrapping nodes.
    bootstrap = [
        ("router.bittorrent.com", 6881),
        ("router.utorrent.com", 6881),
        ("router.bitcomet.com", 6881),
        ("dht.transmissionbt.com", 6881)
    ]
    unresolved = []
    for host, port in bootstrap:
        try:
            parsed = ipaddress.ip_address(host)
            if type(parsed) != ipaddress.IPv4Address:
                raise ValueError('Bootstrap node {} not an IPv4 address or hostname.'.format(host))
            yield from nodes.put(inet_aton(host) + port.to_bytes(2, byteorder='big'))
        except ValueError:
            unresolved.append((host, port))

    if len(unresolved) > 0:
        logger.info('Resolving {} host name(s).'.format(len(unresolved)))

        resolver = aiodns.DNSResolver(loop=loop)
        tasks = [dns_resolve(resolver, host) for host, port in bootstrap]
        ips = [ip for ip in (yield from asyncio.gather(*tasks)) if ip]
        for ip, (host, port) in zip(ips, unresolved):
            yield from nodes.put(inet_aton(ip) + port.to_bytes(2, byteorder='big'))

    # Recursively search for peers.
    keep_running = True
    while keep_running:
        if values.qsize() > 0:
            while values.qsize() > 0:
                peer = yield from values.get()
                host, port = inet_ntoa(peer[:4]), struct.unpack('!H', peer[4:])[0]
                loop.create_task(get_metadata_with_retries(loop, node_id, host, port, info_hash))
        elif get_peers_in_progress < 100 and get_metadatas_in_progress < 100 and nodes.qsize() > 0:
            peer = yield from nodes.get()
            host, port = inet_ntoa(peer[:4]), struct.unpack('!H', peer[4:])[0]
            loop.create_task(get_peers(loop, node_id, host, port, info_hash))
        else:
            yield

            if get_peers_in_progress == 0 and get_metadatas_in_progress == 0 \
                    and nodes.qsize() == 0 and values.qsize() == 0:
                logger.info('Nothing more to do. Quitting.')
                keep_running = False

    if full_metadata:
        name = full_metadata[b'name'].decode()

        if b'length' in full_metadata:
            size = full_metadata[b'length']
            files = {'path': name, 'size': size}
        else:
            size = 0
            files = []
            for f in full_metadata[b'files']:
                size += f[b'length']
                files.append({'path': b'/'.join(f[b'path']).decode(), 'size': f[b'length']})

        print(json.dumps(obj={'name': name, 'files': files, 'size': size, 'peers': len(all_peers), 'released': None}, ensure_ascii=False, indent=4))
        return {'name': name, 'files': files, 'size': size, 'peers': len(all_peers), 'released': None}

    return None


def node_type(value):
    if len(value.split(':')) != 2:
        raise ValueError()

    host, port = value.split(':')
    port = int(port)

    if port < 0 or port > 0xffff:
        raise ValueError()

    if len(host) == 0:
        raise ValueError()

    return host, port


def main():
    global nodes

    info_hash = b16decode('309FD95E605CDBE180D85B55CB6AE140E9839440')

    nodes = SortedQueue(info_hash)

    loop = asyncio.get_event_loop()

    node_id = os.urandom(20)

    loop.run_until_complete(info_hash_to_torrent(loop, node_id, info_hash))

    pending = asyncio.Task.all_tasks()
    for task in pending:
        task.cancel()

    try:
        loop.run_until_complete(asyncio.gather(*pending))
    except CancelledError:
        pass

    loop.close()


if __name__ == '__main__':
    main()
