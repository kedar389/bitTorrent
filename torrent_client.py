import asyncio
import logging

from torrent import Torrent
from tracker import TrackerManager
from asyncio import Queue
from protocol import PeerConnection
from PieceManager import PieceManager


class TorrentClient:
    maximum_peer_connections = 40

    def __init__(self, torrent_path):
        self._torrent_info = Torrent(torrent_path)
        self.tracker = TrackerManager(self._torrent_info)
        # When we call the tracker we get list of availabe peers that we can connect to
        self.available_peers = Queue()
        # These are active connections that we are connected to.
        self.peer_conections = []
        self.piece_manager = PieceManager(self._torrent_info)

        self.aborted = False

    async def start(self):

        logging.info("starting...")
        self.peer_conections = [PeerConnection(available_peers=self.available_peers,
                                               info_hash=self._torrent_info.info_hash,
                                               client_id=self.tracker.peer_id,
                                               piece_manager=self.piece_manager,
                                               on_block_retrieved=self._on_block_retrieved,
                                               id=num)
                                for num in range(TorrentClient.maximum_peer_connections)]

        while True:
            # TODO break if downloaded,maybe continue to seed further?
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
        # self.piece_manager.close()
        for peer_con in self.peer_conections:
            peer_con.stop()

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
