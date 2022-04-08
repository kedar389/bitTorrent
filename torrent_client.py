import asyncio

from torrent import Torrent
from tracker import Tracker
from asyncio import Queue
from time import time
from protocol import PeerConnection

MAXIMUM_PEER_CONNECTIONS = 30


class TorrentClient:

    def __init__(self, torrent_path):
        self._torrent_info = Torrent(torrent_path)
        self.tracker = Tracker(self._torrent_info)
        self.available_peers = Queue()

        self.peer_conections = []

        self.aborted = False

    async def start(self):

        "TODO try to put it in constructor"
        self.peer_conections = [PeerConnection(available_peers=self.available_peers,
                                               info_hash=self.tracker.torrent.info_hash,
                                               client_id=self.tracker.peer_id,
                                               piece_manager=None,
                                               on_block_retrieved=None)
                                for _ in range(MAXIMUM_PEER_CONNECTIONS)]

        '''Base interval'''
        interval = 60 * 15
        previous_announce = None

        while True:
            "TODO if downloaded"
            if self.aborted:
                break

            current_time = time.time()

            '''TODO  update first,uploaded ,downloaded'''
            if previous_announce is None or current_time > previous_announce + interval:

                tracker_response = await  self.tracker.connect()

                if tracker_response:
                    previous_announce = current_time
                    interval = tracker_response.interval
                    self._clear_queue()
                    for peer in tracker_response.peers:
                        self.available_peers.put_nowait(peer)
                    self.tracker.first = False

            else:
                await asyncio.sleep(5)

        await self._stop()

    def _clear_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    async def _stop(self):
        self.abort = True
        await self.tracker.close_connection()
