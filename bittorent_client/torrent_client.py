import asyncio
import logging

from asyncio import Queue
from bittorent_client.protocol import PeerConnection
from bittorent_client.PieceManager import PieceManager
from bittorent_client.torrent import Torrent
from bittorent_client.tracker import TrackerManager


class TorrentClient:
    MAXIMUM_PEER_CONNECTIONS = 40

    def __init__(self, torrent_path):
        self._torrent_info = Torrent(torrent_path)
        self.tracker = TrackerManager(self._torrent_info)
        # When we call the tracker we get list of available peers that we can connect to
        self.available_peers = Queue()
        # These are active connections that we are connected to.
        self.peer_connections = []
        self.piece_manager = PieceManager(self._torrent_info)

        self.aborted = False

    async def start(self):

        logging.info("starting...")
        self.peer_connections = [PeerConnection(available_peers=self.available_peers,
                                                info_hash=self._torrent_info.info_hash,
                                                client_id=self.tracker.peer_id,
                                                piece_manager=self.piece_manager,
                                                on_block_retrieved=self._on_block_retrieved,
                                                con_id=num)
                                 for num in range(TorrentClient.MAXIMUM_PEER_CONNECTIONS)]

        while True:

            if self.piece_manager.complete:
                logging.info("Torrent finished")
                break

            if self.available_peers.qsize() == 0:
                await self.tracker.connect(self.piece_manager.downloaded, self.piece_manager.uploaded,
                                           self.available_peers)
            logging.info("Number of available peers: " + str(self.available_peers.qsize()))
            await asyncio.sleep(30)

        await self._stop()

    async def _stop(self):
        self.aborted = True

        for peer_con in self.peer_connections:
            peer_con.stop()

    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
        Callback function called by the `PeerConnection` when a block is
        retrieved
        """
        if self.piece_manager.block_received(
            peer_id=peer_id, piece_index=piece_index,
            block_offset=block_offset, data=data):

            for peer in self.peer_connections:
                peer.have_update_queue.put(piece_index)



