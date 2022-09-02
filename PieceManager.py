import logging
import os
import time
from collections import namedtuple
from hashlib import sha1
from math import ceil
from pathlib import Path
from typing import Optional

from protocol import REQUEST_SIZE


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

    def is_complete(self):
        """
        Checks if all blocks are downloaded and thus piece is complete
        :return: True or False
        """
        for block in self.blocks:
            if block.status is not block.Retrieved:
                return False

        return True

    @property
    def data(self):
        retrieved = sorted(self.blocks, key=lambda b: b.offset)
        blocks_data = [b.data for b in retrieved]
        return b''.join(blocks_data)

    def is_hash_correct(self):
        return sha1(self.data).digest() == self.hash


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
        self.downloaded = 0
        self.uploaded = 0
        # self.fd = os.open(self.torrent.torrent_name, os.O_RDWR | os.O_CREAT)
        self.fd = None

    def _initialize_pieces(self):
        pieces = []
        blocks_per_piece = ceil(self.torrent.piece_length / REQUEST_SIZE)
        last_piece_length = self.torrent.size % self.torrent.piece_length

        for index, piece_hash_value in enumerate(self.torrent.pieces):

            if index == self.total_pieces - 1 and last_piece_length > 0:
                blocks_per_last_piece = ceil(last_piece_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(blocks_per_last_piece)]

                last_block_length = last_piece_length % REQUEST_SIZE

                if last_block_length > 0:
                    blocks[-1].length = last_block_length

            else:
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                          for offset in range(blocks_per_piece)]

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
        if peer_id in self.peers and index < len(self.peers[peer_id]):
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
                block = self._next_missing(peer_id)
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

        self.downloaded += len(data)

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.receive_block(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_correct():
                    self._write(piece.data,piece.index)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces -
                                len(self.missing_pieces) -
                                len(self.ongoing_pieces))

                    logging.info(
                        '{complete} / {total} pieces downloaded {per:.3f} %'
                            .format(complete=complete,
                                    total=self.total_pieces,
                                    per=(complete / self.total_pieces) * 100))
                else:
                    logging.info('Discarding corrupt piece {index}'
                                 .format(index=piece.index))
                    piece.reset()
        else:
            logging.warning('Trying to update piece that is not ongoing!')

    def _expired_requests(self, peer_id) -> Optional[Block]:
        """
        Go through previously requested blocks, if any one have been in the
        requested state for longer than `MAX_PENDING_TIME` return the block to
        be re-requested.
        If no pending blocks exist, None is returned
        """
        current = int(round(time.time() * 1000))
        for index, request in enumerate(self.pending_blocks):
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current:
                    # Reset expiration timer
                    self.pending_blocks[index] = PendingRequest(request.block, current)
                    return self.pending_blocks[index].block
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

    def _write(self, data,piece_index):
        """
        Write the given piece to disk
        """
        # Find position in stream of pieces
        absolute_pos = piece_index * self.torrent.piece_length

        size_counter = 0
        file_index = 0

        #Find out to which file does piece belong
        for index,file in enumerate(self.torrent.files):
            #If size  of all previous files plus next file is bigger then abs pos  ,piece starts in this file
            if size_counter + file.length > absolute_pos:
                file_index = index
                break

            size_counter += file.length

        #Find position in file
        file_pos = absolute_pos - size_counter

        while len(data) > 0:
            output_file = Path(self.torrent.files[file_index].name)
            output_file.parent.mkdir(exist_ok=True, parents=True)
            fd = os.open(self.torrent.files[file_index].name, os.O_RDWR|os.O_CREAT)
            os.lseek(fd, file_pos, os.SEEK_SET)

            os.write(fd, data[:self.torrent.files[file_index].length - file_pos - 1])
            os.close(fd)

            data = data[self.torrent.files[file_index].length - file_pos:]
            file_pos = 0
            file_index += 1


            #52 kb is last block
            #we are lacking 12,288 bytes of data somewhere prob last block





