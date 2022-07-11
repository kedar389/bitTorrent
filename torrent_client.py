import asyncio
import time
import os
from collections import namedtuple, defaultdict

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

        self.peer_conections = [PeerConnection(available_peers=self.available_peers,
                                               info_hash=self.tracker.torrent.info_hash,
                                               client_id=self.tracker.peer_id,
                                               piece_manager=self.piece_manager,
                                               on_block_retrieved=self._on_block_retrieved)
                                for _ in range(TorrentClient.maximum_peer_connections)]

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


PendingRequest = namedtuple('PendingRequest', ['block', 'added'])


class PieceManager:

    def __init__(self, torrent):

        self.torrent = torrent
        self.total_pieces = len(torrent.pieces)
        self.peers = {}
        self.have_pieces = []
        self.ongoing_pieces = []
        self.pending_blocks = []
        self.max_pending_time = 300 * 1000  # 5 minutes
        self.missing_pieces = self._initialize_pieces()
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
        Tries to remove a previously added peer.
         (e.g. used if a peer connection is dropped)
        """
        if peer_id in self.peers:
            del self.peers[peer_id]

    def next_request(self, peer_id) -> Optional[Block]:
        """
        Get the next Block that should be requested from the given peer.
        If there are no more blocks left to retrieve or if this peer does not
        have any of the missing pieces None is returned
        """
        # The algorithm implemented for which piece to retrieve is a simple
        # one. This should preferably be replaced with an implementation of
        # "rarest-piece-first" algorithm instead.
        #
        # The algorithm tries to download the pieces in sequence and will try
        # to finish started pieces before starting with new pieces.
        #
        # 1. Check any pending blocks to see if any request should be reissued
        #    due to timeout
        # 2. Check the ongoing pieces to get the next block to request
        # 3. Check if this peer have any of the missing pieces not yet started
        if peer_id not in self.peers:
            return None

        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                block = self._get_rarest_piece(peer_id).next_request()
        return block

    def block_received(self, peer_id, piece_index, block_offset, data):
        """
        This method must be called when a block has successfully been retrieved
        by a peer.
        Once a full piece have been retrieved, a SHA1 hash control is made. If
        the check fails all the pieces blocks are put back in missing state to
        be fetched again. If the hash succeeds the partial piece is written to
        disk and the piece is indicated as Have.
        """

        # Remove from pending requests
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece == piece_index and \
                    request.block.offset == block_offset:
                del self.pending_blocks[index]
                break

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.receive_block(block_offset, data)
            if piece.complete():
                if piece.is_hash_correct():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces -
                                len(self.missing_pieces) -
                                len(self.ongoing_pieces))

    def _expired_requests(self, peer_id) -> Optional[Block]:
        """
        Go through previously requested blocks, if any one have been in the
        requested state for longer than `MAX_PENDING_TIME` return the block to
        be re-requested.
        If no pending blocks exist, None is returned
        """
        current = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current:
                    # Reset expiration timer
                    request.added = current
                    return request.block
        return None

    def _next_ongoing(self, peer_id) -> Optional[Block]:
        """
        Go through the ongoing pieces and return the next block to be
        requested or None if no block is left to be requested.
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                # Is there any blocks left to request in this piece?
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(
                        PendingRequest(block, int(round(time.time() * 1000))))
                    return block
        return None

    def _get_rarest_piece(self, peer_id):
        """
        Given the current list of missing pieces, get the
        rarest one first (i.e. a piece which fewest of its
        neighboring peers have)
        """
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                if self.peers[p][piece.index]:
                    piece_count[piece] += 1

        rarest_piece = min(piece_count, key=lambda p: piece_count[p])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _next_missing(self, peer_id) -> Optional[Block]:
        """
        Go through the missing pieces and return the next block to request
        or None if no block is left to be requested.
        This will change the state of the piece from missing to ongoing - thus
        the next call to this function will not continue with the blocks for
        that piece, rather get the next missing piece.
        """
        for index, piece in enumerate(self.missing_pieces):
            if self.peers[peer_id][piece.index]:
                # Move this piece from missing to ongoing
                piece = self.missing_pieces.pop(index)
                self.ongoing_pieces.append(piece)
                # The missing pieces does not have any previously requested
                # blocks (then it is ongoing).
                return piece.next_request()
        return None

    def _write(self, piece):
        """
        Write the given piece to disk
        """
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)
