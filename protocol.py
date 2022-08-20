import asyncio
import struct
import bitstring
import logging

from enum import IntEnum
from concurrent.futures import CancelledError

# size is specified by Bittorent specification and is agreed upon by all implementers of Bittorent protocol.
REQUEST_SIZE = 2 ** 14


class ProtocolError(BaseException):
    pass


class PeerConnection:

    def __init__(self, available_peers, client_id, info_hash, piece_manager, on_block_retrieved, id):
        self.avaialabe_peers = available_peers
        self.client_id = client_id
        self.info_hash = info_hash
        self.my_state = set()
        self.peer_state = set()
        self.writer = None
        self.reader = None
        self.remote_id = None
        self.piece_manager = piece_manager
        self.on_blk = on_block_retrieved
        self.future = asyncio.ensure_future(self._start())
        self.id = id

    # TODO drop connection
    async def _start(self):
        while "stop" not in self.my_state:
            try:
                ip, port = await self.avaialabe_peers.get()
                logging.debug('Con {id} Got assigned peer with: {ip}'.format(id=self.id, ip=ip))

                self.reader, self.writer = await asyncio.open_connection(ip, port)
                logging.debug('Con {id} Connection open to peer: {ip}'.format(id=self.id, ip=ip))

                # await handshake
                buffer = await self._do_handshake()
                self.my_state.add("choked")

                await self._send_interested()
                self.my_state.add('interested')

                async for msg in PeerStreamIterator(self.reader, buffer):
                    logging.debug('Con number {id} Got message {message}'.format(id = self.id,message=msg.__str__()))
                    if "stop" in self.my_state:
                        break

                    if type(msg) is Bitfield:
                        #if len(msg.bitfield) != self.piece_manager.total_pieces:
                            #raise ProtocolError('Received bitfield with different size')
                        self.piece_manager.add_peer(self.remote_id, msg.bitfield)

                    elif type(msg) is Choke:
                        self.my_state.add("choked")

                    elif type(msg) is Unchoke:
                        if "choked" in self.my_state:
                            self.my_state.remove("choked")

                    elif type(msg) is Interested:
                        self.peer_state.add("interested")

                    elif type(msg) is NotInterested:
                        if "interested" in self.peer_state:
                            self.peer_state.remove("interested")

                    elif type(msg) is KeepAlive:
                        pass

                    elif type(msg) is Have:
                        self.piece_manager.update_peer(self.remote_id, msg.index)

                    elif type(msg) is Piece:
                        self.my_state.remove('pending_request')

                        self.on_blk(self.remote_id, msg.piece, msg.offset, msg.data)

                    elif type(msg) is Request:
                        # TODO Add support for sending data
                        pass

                    elif type(msg) is Cancel:
                        # TODO Add support for sending data
                        pass

                    if 'choked' not in self.my_state:
                        if 'interested' in self.my_state:
                            if 'pending_request' not in self.my_state:
                                self.my_state.add('pending_request')
                                await self._request_piece()

            except ProtocolError as e:
                logging.exception('Protocol error')
            except (ConnectionRefusedError, TimeoutError):
                logging.warning('Unable to connect to peer')
            except (ConnectionResetError, CancelledError):
                logging.warning('Connection closed')
            except Exception(BaseException) as e:
                logging.exception('An error occurred')

            logging.debug('Con {id} dropped connection'.format(id=self.id))
            await self._close_connection()
        logging.debug('Task dropped'.format(id=self.id))

    async def _request_piece(self):
        block_to_request = self.piece_manager.next_request(self.remote_id)
        if block_to_request:
            message = Request(block_to_request.piece, block_to_request.offset, block_to_request.length).encode()

            # logging.debug('Requesting block {block} for piece {piece} '
            #             'of {length} bytes from peer {peer}'.format(
            #  piece=block_to_request.piece,
            #  block=block_to_request.offset,
            #  length=block_to_request.length,
            #  peer=self.remote_id))

            self.writer.write(message)
            await self.writer.drain()

    async def _do_handshake(self):
        """Send handshake which contains info about peer_id and info_hash, wait for handshake to return,
        info_hash must be equal"""

        handshake_message = Handshake(self.info_hash, self.client_id).encode()
        self.writer.write(handshake_message)
        # await drain to not overwrite data that we sent
        await self.writer.drain()

        buffer = b''
        tries = 0

        # TODO make it time based
        while len(buffer) < Handshake.length and tries < 20:
            tries += 1
            buffer = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buffer[:Handshake.length])

        # TODO validate peer_id
        if not response:
            raise ProtocolError('Unable receive and parse a handshake')
        if not response.info_hash == self.info_hash:
            raise ProtocolError('Handshake with invalid info_hash')

        self.remote_id = response.client_id

        logging.debug('Con {id} Handshake with peer was successful'.format(id=self.id))
        # return not used part of message
        return buffer[Handshake.length:]

    async def _send_interested(self):
        self.writer.write(Interested().encode())
        await self.writer.drain()

    async def _close_connection(self):
        logging.debug('Con {idc} Closing peer {id}'.format(id=self.remote_id, idc=self.id))

        # TODO send cancel message
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    def stop(self):
        """
        Stop this connection from the current peer (if a connection exist) and
        from connecting to any new peer.
        """

        self.my_state.add('stop')
        if not self.future.done():
            self.future.cancel()


class PeerStreamIterator:
    """Type of async iterator that iterates over messages that peer sends,
        Every next returns type of peerMessage.

        If it fails or connection will be closed,
        Raises StopAsyncIteration and iteration will end.
    """

    CHUNK_SIZE = 10 * 1024

    def __init__(self, reader, not_used_message):
        self.reader = reader
        self.buffer = not_used_message if not_used_message else b''

    def __aiter__(self):
        return self

    async def __anext__(self):

        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()

            except ConnectionResetError:
                raise StopAsyncIteration()

            except CancelledError:
                raise StopAsyncIteration()

            except Exception:
                raise StopAsyncIteration()

    def parse(self):
        """
        Tries to parse the message and return type of PeerMessage

        Each message is structured as:
         <length prefix><message ID><payload>
        The `length prefix` is a four byte big-endian value (header)
        The `message ID` is a decimal byte
        The `payload` is the value of `length prefix`
         """
        length_header = 4

        if len(self.buffer) > length_header:
            message_length = struct.unpack(">I", self.buffer[0:4])[0]

            if message_length == 0:
                self.buffer = self.buffer[length_header:]
                return KeepAlive()

            if len(self.buffer) >= message_length:
                message_id = struct.unpack(">b", self.buffer[4:5])[0]

                def _consume():
                    """Consume the read message from buffer"""
                    self.buffer = self.buffer[message_length + length_header:]

                def _data():
                    """Returns data of message that was read"""
                    return self.buffer[:message_length + length_header]

                if message_id == PeerMessages.Choke:
                    _consume()
                    return Choke()

                elif message_id == PeerMessages.Unchoke:
                    _consume()
                    return Unchoke()

                elif message_id == PeerMessages.Interested:
                    _consume()
                    return Interested()

                elif message_id == PeerMessages.NotInterested:
                    _consume()
                    return NotInterested()

                elif message_id == PeerMessages.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)

                elif message_id == PeerMessages.BitField:
                    data = _data()
                    _consume()
                    return Bitfield.decode(data)

                elif message_id == PeerMessages.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)

                elif message_id == PeerMessages.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)

                elif message_id == PeerMessages.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                else:
                    logging.info('Unsupported message!'
                                 'Message id is {id}'.format(id=message_id))
        return None


class Handshake:
    """Handshake is not really part of PeerMessages, it is more like start of connection,message used only once
     49 is length of message and 19 is currently the size for pstr which is name of Protocol used"""

    length = 49 + 19

    def __init__(self, info_hash, client_id):
        self.pstr = b'BitTorrent protocol'
        self.pstrlen = len(self.pstr)

        if isinstance(info_hash, str):
            info_hash = info_hash.encode('utf-8')
        if isinstance(client_id, str):
            client_id = client_id.encode('utf-8')

        self.info_hash = info_hash
        self.client_id = client_id

    # Handshake message represented in bytes (ready to be transmitted)

    def encode(self):
        return struct.pack('>B19s8x20s20s', self.pstrlen, self.pstr, self.info_hash, self.client_id)

    @classmethod
    def decode(cls, response: bytes):
        """
        Decodes handshake from user,
        if length is correct tries to parse it and return handshake object , otherwise None
        """
        if len(response) < Handshake.length:
            return None

        segments = struct.unpack('>B19s8x20s20s', response)
        return cls(info_hash=segments[2], client_id=segments[3])


class PeerMessages(IntEnum):
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9


class KeepAlive:
    """
    The Keep-Alive message has no payload and length is set to zero.
    Message format:
        <len=0000>
    """

    def __str__(self):
        return 'KeepAlive'


class Choke:
    """
    The choke message is used to tell the other peer to stop send request
    messages until unchoked.
    Message format:
        <len=0001><id=0>
    """

    def __str__(self):
        return 'Choke'


class Unchoke:
    """
    Unchoking a peer enables that peer to start requesting pieces from the
    remote peer.
    Message format:
        <len=0001><id=1>
    """

    def __str__(self):
        return 'Unchoke'


class Interested:
    """Interested message: Format <len=0001><id=2>,
    We send number 2 in big endian as bytes."""

    def encode(self):
        return struct.pack('>Ib', 1, PeerMessages.Interested)

    def __str__(self):
        return 'Interested'


class NotInterested:
    """
    The not interested message is fix length and has no payload other than the
    message identifier. It is used to notify each other that there is no
    interest to download pieces.
    Message format:
        <len=0001><id=3>
    """

    def __str__(self):
        return 'NotInterested'


class Have:
    """
    Have message is sent when other peer notifies us about piece they got ready for transmiting.
    Message format:
        <len=0005><id=4><index>

        Where index is payload that is zero based index of piece that peer has downloaded.
    """

    def __init__(self, index):
        self.index = index

    def encode(self):
        return struct.pack('>IbI', 5, PeerMessages.Have, self.index)

    @classmethod
    def decode(cls, data: bytes):
        index = struct.unpack('<IbI', data)[2]
        return cls(index)

    def __str__(self):
        return 'Have'


class Bitfield:
    """
    The BitField message payload contains a sequence of bytes that when read binary each bit will represent one piece.
    If the bit is 1 that means that the peer have the piece with that index, while 0 means that the peer lacks that piece.
    I.e. Each byte in the payload represent up to 8 pieces with any spare bits set to 0.

    Message format:
        <len=0001+X><id=5><bitfield>
    """

    def __init__(self, data: bytes):
        self.bitfield = bitstring.BitArray(data)

    def encode(self):
        bits_length = len(self.bitfield)
        return struct.pack('>Ib' + str(bits_length) + 's',
                           1 + bits_length,
                           PeerMessages.BitField,
                           self.bitfield)

    @classmethod
    def decode(cls, data: bytes):
        message_length = struct.unpack('>I', data[0:4])[0]
        bitfield_data = struct.unpack('>Ib' + str(message_length - 1) + 's', data)[2]

        return cls(bitfield_data)

    def __str__(self):
        return "Bitfield"


class Request:
    """
    The request message is fixed length, and is used to request a block. The payload contains the following information:

    index: integer specifying the zero-based piece index
    begin: integer specifying the zero-based byte offset within the piece
    length: integer specifying the requested length.

    Message format:
        <len=0013><id=6><index><begin><length>
    """

    def __init__(self, index, begin, length):
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack('>IbIII',
                           13,
                           PeerMessages.Request,
                           self.index,
                           self.begin,
                           self.length)

    @classmethod
    def decode(cls, data: bytes):
        message = struct.unpack('>IbIII', data)
        return cls(message[2], message[3], message[4])

    def __str__(self):
        return "Request"


class Piece:
    """
    The piece message is variable length, where X is the length of the block.
    The payload contains the following information:

        piece: integer specifying the zero-based piece index
        offset: integer specifying the zero-based byte offset within the piece
        data: block of data, which is a subset of the piece specified by index.

    Message format:
         <len=0009+X><id=7><index><begin><block>
    """

    base_length = 9

    def __init__(self, piece, offset, data):
        self.piece = piece
        self.offset = offset
        self.data = data

    def encode(self):
        message_length = Piece.base_length + len(self.data)
        return struct.pack('>IbII' + str(len(self.data)) + 's',
                           message_length,
                           PeerMessages.Piece,
                           self.piece,
                           self.offset,
                           self.data
                           )

    @classmethod
    def decode(cls, data: bytes):
        block_length = struct.unpack('>I', data[:4])[0]
        message = struct.unpack('>IbII' + str(block_length - Piece.base_length) + 's', data)

        return cls(message[2], message[3], message[4])

    def __str__(self):
        return "Piece"


class Cancel:
    """
    The cancel message is fixed length, and is used to cancel block requests.
    The payload is identical to that of the "request" message.

    Message format:
        <len=0013><id=8><index><begin><length>
    """

    def __init__(self, index, begin, block):
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self):
        return struct.pack('>IbIII',
                           13,
                           PeerMessages.Cancel,
                           self.index,
                           self.begin,
                           self.block)

    @classmethod
    def decode(cls, data: bytes):
        message = struct.unpack('>IbIII', data)
        return cls(message[2], message[3], message[4])

    def __str__(self):
        return "Cancel"
