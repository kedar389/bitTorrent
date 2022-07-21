import asyncio
import time
import logging

from torrent import Torrent
from tracker import Tracker
from asyncio import Queue
from protocol import PeerConnection
from PieceManager import PieceManager


class TorrentClient:
    maximum_peer_connections = 35

    def __init__(self, torrent_path):
        self._torrent_info = Torrent(torrent_path)
        self.tracker = Tracker(self._torrent_info)
        # When we call the tracker we get list of availabe peers that we can connect to
        self.available_peers = Queue()
        # These are active connections that we are connected to.
        self.peer_conections = []
        self.piece_manager = PieceManager(self._torrent_info)

        self.aborted = False

    async def start(self):

        logging.info("starting...")
        self.peer_conections = [PeerConnection(available_peers=self.available_peers,
                                               info_hash=self.tracker.torrent.info_hash,
                                               client_id=self.tracker.peer_id,
                                               piece_manager=self.piece_manager,
                                               on_block_retrieved=self._on_block_retrieved,
                                               id = num)
                                for num in range(TorrentClient.maximum_peer_connections)]
        # Base interval
        interval = 60 * 15
        previous_announce = None

        while True:
            # TODO break if downloaded,maybe continue to seed further?
            if self.aborted:
                break

            current_time = time.time()

            #TODO  update first,uploaded ,downloaded
            if previous_announce is None or current_time > previous_announce + interval:

                tracker_response = await self.tracker.connect()

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
        self.aborted = True
        self.piece_manager.close()
        for peer_con in self.peer_conections:
            peer_con.stop()
        await self.tracker.close_connection()

    # TODO remove later
    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
        Callback function called by the `PeerConnection` when a block is
        retrieved from a peer.
        :param peer_id: The id of the peer the block was retrieved from
        :param piece_index: The piece index this block is a part of
        :param block_offset: The block offset within its piece
        :param data: The binary data retrieved
        """
        self.piece_manager.block_received(
            peer_id=peer_id, piece_index=piece_index,
            block_offset=block_offset, data=data)

