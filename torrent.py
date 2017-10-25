#! /usr/bin/python
# -*- coding: utf-8 -*-


import requests
import libtorrent

from utils import load_config


CONFIG = load_config()


class Torrent:
    def __init__(self):
        self.session = requests.session()
        self.headers = {'User-Agent': CONFIG['requests']['user_agent']}
        self.timeout = CONFIG['requests']['timeout']

    def retrieve(self, info_hash):
        return self.torcache(info_hash=info_hash)

    @staticmethod
    def parse_torrent(content):
        # content = libtorrent.bdecode(content)
        torrent_info = libtorrent.torrent_info(content)

        name = torrent_info.name()

        files = []
        for index, entry in enumerate(torrent_info.files()):
            try:
                file = entry.path
            except UnicodeDecodeError:
                file = content[b'info'][b'files'][index]
            #         'files': [
            #     {'path': entry.path.replace(name + '/', '', 1), 'size': entry.size}
            #     for entry in torrent_info.files()
            # ],

        info = {
            'name': name,
            'info_hash': str(torrent_info.info_hash()).lower(),
            'num_files': torrent_info.num_files(),
            'total_size': torrent_info.total_size(),
            'num_pieces': torrent_info.num_pieces(),
            'piece_length': torrent_info.piece_length(),
            'metadata_size': torrent_info.metadata_size(),
            'creation_date': content.get(b'creation date', None),
            'peers': 0
        }

        return info

    def torcache(self, info_hash):
        url = CONFIG['torrent_cache_services']['torcache'].format(info_hash.upper())

        resp = self.session.get(url=url, headers=self.headers)
        if resp.status_code == 200:
            return self.parse_torrent(resp.content)

        return None

    def btbox(self, info_hash):
        info_hash = info_hash.upper()
        url = CONFIG['torrent_cache_services']['btbox'].format(info_hash[:2], info_hash[-2:], info_hash)

        resp = self.session.get(url=url, headers=self.headers)
        if resp.status_code == 200:
            return self.parse_torrent(resp.content)

        return None


if __name__ == '__main__':
    # t = Torrent().btbox('f8181597b51c157fb470e5ee236e364c6fbc2af2')
    # print(t)

    Torrent().parse_torrent('x.torrent')