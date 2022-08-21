import asyncio
import random
import socket
import struct
import time
from collections import namedtuple
from enum import IntEnum
from struct import unpack
from typing import Optional
from urllib.parse import urlencode, urlparse

import aiohttp

from bencoding import Decoder


class DatagramReaderProtocol(asyncio.DatagramProtocol):

    def __init__(self):
        self._buffer = bytearray()
        self._waiter = None  # type: Optional[asyncio.Future]
        self._connection_lost = False
        self._exception = None  # type Exception

    def connection_made(self, transport: asyncio.DatagramTransport):
        pass

    async def recv(self) -> bytes:
        if self._waiter is not None:
            raise RuntimeError('Another coroutine is already waiting for incoming data')

        if self._exception is None and not self._connection_lost and not self._buffer:
            self._waiter = asyncio.Future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        if self._exception is not None:
            exc = self._exception
            self._exception = None
            raise exc
        if self._connection_lost:
            raise ConnectionResetError('Connection lost')

        buffer = self._buffer
        self._buffer = bytearray()
        return buffer

    def _wakeup_waiter(self):
        if self._waiter is not None:
            self._waiter.set_result(None)

    def datagram_received(self, data: bytes, addr: tuple):
        self._buffer.extend(data)
        self._wakeup_waiter()

    def error_received(self, exc: Exception):
        self._exception = exc
        self._wakeup_waiter()

    def connection_lost(self, exc: Exception):
        self._connection_lost = True
        self._exception = exc
        self._wakeup_waiter()


class ActionType(IntEnum):
    connect = 0
    announce = 1
    scrape = 2
    error = 3


class UdpTrackerClient:
    MAGIC_CONNECTION_ID = 0x41727101980
    RESPONSE_HEADER_LEN = struct.calcsize('!iiq')
    REQUEST_TIMEOUT = 10

    def __init__(self, announce_url, params):
        self.url = announce_url
        self.params = params

    async def request(self):
        parsed = urlparse(self.url)
        socket_address = (socket.gethostbyname(parsed.hostname), parsed.port)

        transport, protocol = await asyncio.get_event_loop().create_datagram_endpoint(
            DatagramReaderProtocol, remote_addr=socket_address)

        try:
            # Get connection ID
            connect_request, transaction_id = self._create_connection_request()
            transport.sendto(connect_request)
            buffer = await asyncio.wait_for(protocol.recv(), UdpTrackerClient.REQUEST_TIMEOUT)

            connection_id = self._check_response(buffer, transaction_id, ActionType.connect)

            # Get peers from announce
            announce_req, transaction_id = self._create_announce_request(connection_id)
            transport.sendto(announce_req)
            buffer = await asyncio.wait_for(protocol.recv(), UdpTrackerClient.REQUEST_TIMEOUT)

            return self._parse_announce_response(buffer, transaction_id)
        finally:
            transport.close()

    @staticmethod
    def _create_connection_request():
        transaction_id = int(random.randrange(0, 255))
        buf = struct.pack(">QII", UdpTrackerClient.MAGIC_CONNECTION_ID, ActionType.connect, transaction_id)
        return buf, transaction_id

    def _create_announce_request(self, connection_id):
        transaction_id = int(random.randrange(0, 255))
        announce_pack = struct.pack(">qII20s20sQQQIIIiH",
                                    connection_id,
                                    ActionType.announce,
                                    transaction_id,
                                    self.params['info_hash'],
                                    self.params['peer_id'].encode('utf-8'),
                                    self.params['downloaded'],
                                    self.params['left'],
                                    self.params['downloaded'],
                                    0, 0, 0, -1,
                                    self.params['port'])
        return announce_pack, transaction_id

    @staticmethod
    def _parse_announce_response(buffer, sent_transaction_id):
        announce_ret_intro = '!IIIII'
        action, transaction_id, interval, leechers, seeders = struct.unpack_from(announce_ret_intro, buffer)
        compact_peer_list = buffer[struct.calcsize(announce_ret_intro):]

        if transaction_id != sent_transaction_id:
            raise ValueError('Unexpected transaction ID')

        response_dict = {b'interval': interval,
                         b'incomplete': leechers,
                         b'complete': seeders,
                         b'peers': compact_peer_list}
        return TrackerResponse(response_dict)

    @staticmethod
    def _check_response(buf, sent_transaction_id, expected_action):
        action, transaction_id, connection_id = struct.unpack_from('!iiq', buf)

        if transaction_id != sent_transaction_id:
            raise ValueError('Unexpected transaction ID')

        if expected_action == ActionType.error:
            message = buf[UdpTrackerClient.RESPONSE_HEADER_LEN:]
            raise ConnectionError(message.decode())

        if expected_action != expected_action:
            raise ValueError('Unexpected action ID (expected {}, got {})'.format(
                expected_action.name, expected_action.name))
        return connection_id


class HttpTrackerClient:
    def __init__(self, announce_url, params):
        self.params = params
        self.announce_url = announce_url

    async def request(self):
        url = self.announce_url + '?' + urlencode(self.params)

        async with aiohttp.ClientSession() as client_session:
            async with client_session.get(url) as response:
                if not response.status == 200:
                    raise ConnectionError('Unable to connect to tracker: Response status is not 200')

                data = await response.read()
            return TrackerResponse(data)


class TrackerResponse:

    def __init__(self, data):
        if isinstance(data, bytes):
            self._response = Decoder(data).decode()
        if isinstance(data, dict):
            self._response = data

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

        if isinstance(peers, list):
            return [(peer.get(b'ip').decode(), peer.get(b'port')) for peer in peers]

        elif isinstance(peers, bytes) or isinstance(peers, bytearray):
            '''one peers is 6 bytes,4 bytes addres - 2 bytes port '''
            peers = [peers[i:i + 6] for i in range(0, len(peers), 6)]

            '''> is for big-endian or network order
            H is for unsigned short'''

            return [(socket.inet_ntoa(p[:4]), unpack(">H", p[4:])[0])
                    for p in peers]

        return None


NextAnnounce = namedtuple('NextAnnounce', ['responded', 'previous_announce', 'interval'])


class TrackerManager:
    def __init__(self, torrent):
        self.peer_id = self._create_peer_id()
        self._torrent = torrent
        # Map urls and announce times
        self._trckr_responses = {}


    @staticmethod
    def _create_peer_id():
        """Creates unique id for client,
        TD is for name of the client,could be anything you want
        """
        return "-TD0001-" + "".join([str(random.randint(0, 9)) for _ in range(0, 12)])

    # FIXME maybe do not remove duplicates in peers ?
    async def connect(self, downloaded, uploaded, peer_queue):
        """
        Contacts all trackers to get available peers
        Removes duplicates of (ip, port)
        Fills queue passed to it  with peers
        """
        tracker_responses = await asyncio.gather(*self._create_tracker_requests(downloaded, uploaded),
                                                 return_exceptions=True)
        new_peers = set()
        for response_index, response in enumerate(tracker_responses):
            # To map results from tasks,each result corresponds to index of url in url list
            url = self._torrent.announce_list[response_index]

            if isinstance(response, TrackerResponse) and response.failure is None:
                self._trckr_responses[url] = NextAnnounce(True, time.time(), response.interval)
                for peer in response.peers:
                    new_peers.add(peer)
            else:
                self._trckr_responses[url] = NextAnnounce(False, None, None)

        for peer in new_peers:
            peer_queue.put_nowait(peer)

    def _create_tracker_requests(self, downloaded, uploaded):
        request_params = self._build_request_params(downloaded, uploaded)
        tracker_requests = []
        current_time = time.time()

        for url in self._torrent.announce_list:

            if url not in self._trckr_responses or \
                    (self._trckr_responses[url].responded and current_time >
                     self._trckr_responses[url].previous_announce + self._trckr_responses[url].interval):

                if url[0:3] == 'udp':
                    tracker_requests.append(UdpTrackerClient(url, request_params).request())
                elif url[0:4] == 'http':
                    tracker_requests.append(HttpTrackerClient(url, request_params).request())

        return tracker_requests

    def _build_request_params(self, downloaded, uploaded):
        return {'info_hash': self._torrent.info_hash,
                'peer_id': self.peer_id,
                'uploaded': uploaded,
                'downloaded': downloaded,
                'left': self._torrent.size - downloaded,
                'port': 6889,
                'compact': 1
                }
