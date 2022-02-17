import asyncio

from torrent import Torrent
from tracker import Tracker
from queue import Queue
from time import time


MAXIMUM_PEERS = 30

class TorrentClient:


    def __init__(self, torrent_path):
        self._torrent_info = Torrent(torrent_path)
        self.tracker = Tracker(self._torrent_info)
        self.available_peers = Queue()
        self.peer_list = []
        self.aborted = False

    async def start(self):

        '''Base interval'''
        interval = 60 * 15
        previous_announce = None

        while True:
            if (self.aborted):
                break

            current_time = time.time()


            '''TODO  update first,uploaded ,downloaded'''
            if previous_announce == None or current_time > previous_announce + interval:

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


    def _clear_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()