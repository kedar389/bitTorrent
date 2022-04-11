import asyncio
import time
import os

from typing import Optional
from torrent import Torrent
from tracker import Tracker
from asyncio import Queue
from protocol import PeerConnection, REQUEST_SIZE
from hashlib import sha1
from math import ceil


class TorrentClient:
    maximum_peer_connections = 30

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

        # TODO try to put it in constructor
        self.peer_conections = [PeerConnection(available_peers=self.available_peers,
                                               info_hash=self.tracker.torrent.info_hash,
                                               client_id=self.tracker.peer_id,
                                               piece_manager=self.piece_manager,
                                               on_block_retrieved=None)
                                for _ in range(TorrentClient.maximum_peer_connections)]

        '''Base interval'''
        interval = 60 * 15
        previous_announce = None

        while True:
            # TODO break if downloaded,maybe continue to seed further?
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


class Block:
    """
    Subset of a piece,
    :param
    piece represent zero based index of piece
    offset represents zero based index of block in piece
    length is the length of the block
    """
    Missing = 0
    Pending = 1
    Retrieved = 2

    def __init__(self, piece: int, offset: int, length: int):
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data = None


class Piece:

    def __init__(self, index: int, hash_value, blocks: [Block]):
        self.hash = hash_value
        self.index = index
        self.blocks = blocks

    def next_request(self) -> Optional[Block]:
        """
        Get the next Block to be requested
        """
        for block in self.blocks:
            if block.status == Block.Missing:
                block.status = Block.Pending
                return block

        return None

    def reset(self):
        for b in self.blocks:
            b.status = Block.Missing

    def receive_block(self, offset: int, data: bytes):
        """
        Finds right block with offset within a  piece, if exists ,updates block with data and sets status to retrieved
        :param offset: offset in piece
        :param data: data representation of block
        :return:
        """
        matches = [b for b in self.blocks if b.offset == offset]
        block = matches[0] if matches else None

        if block:
            block.data = data
            block.status = Block.Retrieved

    @property
    def complete(self):
        """
        Checks if all blocks are downloaded and thus piece is complete
        :return: True or False
        """
        for block in self.blocks:
            if block.status == Block.Missing:
                return False

        return True

    @property
    def data(self):
        sorted_blocks = sorted(self.blocks, key=lambda b: b.offset)
        blocks_data = [b.data for b in sorted_blocks]
        return b''.join(blocks_data)


    def is_hash_correct(self):
        if sha1(self.data).digest() == self.hash:
            return True

        return False


class PieceManager:

    def __init__(self, torrent):

        self.torrent = torrent
        self.total_pieces = len(torrent.pieces)
        self.peers = {}
        self.have_pieces = []

        self.missing_piece = self._initialize_pieces()
        self.fd = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)

    def _initialize_pieces(self):
        pieces = []
        blocks_per_piece = ceil(self.torrent.piece_length / REQUEST_SIZE)

        for index, piece_hash_value in enumerate(self.torrent.pieces):

            if index - 1 < self.total_pieces:
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(blocks_per_piece)]

            else:
                last_piece_length = self.torrent.torrent_size % self.torrent.piece_length
                blocks_per_last_piece = ceil(last_piece_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(blocks_per_last_piece)]

                last_block_length = last_piece_length % REQUEST_SIZE

                if last_block_length > 0:
                    blocks[-1].length = last_block_length

            pieces.append(Piece(index, piece_hash_value, blocks))

        return pieces

    def close(self):
        if self.fd:
            os.close(self.fd)

    @property
    def complete(self):
        """
        Checks whether or not the all pieces are downloaded for this torrent.
        :return: True if all pieces are fully downloaded else False
        """
        return len(self.have_pieces) == self.total_pieces



    def add_peer(self, peer_id, bitfield):
        """
        Adds a peer and the bitfield representing the pieces the peer has.
        """
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        """
        Updates the information about which pieces a peer has (reflects a Have
        message).
        """
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        """
        Tries to remove a previously added peer (e.g. used if a peer connection
        is dropped)
        """
        if peer_id in self.peers:
            del self.peers[peer_id]



    def next_request(self):
        pass