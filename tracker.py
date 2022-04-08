import random
from urllib.parse import urlencode
import socket
import aiohttp
from bencoding import Decoder
from struct import unpack


class TrackerResponse:

    def __init__(self, data):
        self._response = Decoder(data).decode()

    @property
    def failure(self):
        if b'failure reason' in self._response:
            return self._response.get(b'failure reason').decode('utf-8')
        return None

    @property
    def interval(self):
        return self._response.get(b'interval')

    @property
    def incomplete(self):
        return self._response.get(b'incomplete')

    @property
    def complete(self):
        return self._response.get(b'complete')

    @property
    def peers(self):
        peers = self._response.get(b'peers')

        if type(peers) == list:
            return [(peer.get(b'ip').decode(), peer.get(b'port')) for peer in peers]

        elif type(peers) == bytes:
            '''one peers is 6 bytes,4 bytes addres - 2 bytes port '''
            peers = [peers[i:i + 6] for i in range(0, len(peers), 6)]

            '''> is for big-endian or network order
            H is for unsigned short'''

            return [(socket.inet_ntoa(p[:4]), unpack(">H", p[4:])[0])
                    for p in peers]

        return None

    def __str__(self):
        pass


class Tracker:
    def __init__(self, torrent):
        self.downloaded = 0
        self.uploaded = 0
        self.first = True
        self.torrent = torrent
        self.peer_id = self._create_peer_id()
        self.http_client = aiohttp.ClientSession()

    async def connect(self):

        params = self._build_tracker_request_params()

        if self.first:
            params["event"] = "started"

        url = self.torrent.announce + '?' + urlencode(params)
        print(url)

        async with self.http_client.get(url) as response:
            if not response.status == 200:
                raise ConnectionError('Unable to connect to tracker')
            data = await response.read()

            return TrackerResponse(data)

    @staticmethod
    def _create_peer_id():
        '''Creates unique id for client'''
        '''TD is for name of the client,could be anything you want'''
        return "-TD0001-" + "".join([str(random.randint(0, 9)) for _ in range(0, 12)])

    def _build_tracker_request_params(self):
        return {'info_hash': self.torrent.info_hash,
                "peer_id": self.peer_id,
                'uploaded': self.uploaded,
                'downloaded': self.downloaded,
                'left': self.torrent.torrent_size - self.downloaded,
                'port': 6889,
                'compact': 1
                }

    async def close_connection(self):
        await self.http_client.close()
