import asyncio
import struct
from enum import Enum
from concurrent.futures import CancelledError
import bitstring


#size is specified by bittorent specification and is agreed upon by all implementers of bittorent protocol.
REQUEST_SIZE = 2 ** 14


#TODO errors for _start, plus request piece plus messages, cancel and stop,import bitstring
class PeerConnection:



    def __init__(self, available_peers, client_id, info_hash, piece_manager, on_block_retrieved):
        self.avaialabe_peers = available_peers
        self.client_id = client_id
        self.info_hash = info_hash
        self.my_state = set()
        self.peer_state = set()
        self.writer = None
        self.reader = None
        self.remote_id = None

        self.future = asyncio.ensure_future(self._start())

    async def _start(self):
        while "stop" not in self.my_state:
            ip, port = await self.avaialabe_peers.get()

            self.writer, self.reader = await asyncio.open_connection(ip, port)

            #await handshake
            buffer = await self._do_handshake()
            self.my_state.add("choked")

            await self._send_interested()
            self.my_state.add('interested')

            async for msg in PeerStreamIterator(self.reader, buffer):
                if("stop") in self.my_state:
                    break

                if type(msg) is Bitfield:
                    pass

                elif type(msg) is Choke:
                    self.my_state.add("choked")

                elif type(msg) is Unchoke:
                    if "choked" in self.peer_state:
                        self.my_state.remove("choked")

                elif type(msg) is Interested:
                    self.peer_state.add("interested")

                elif type(msg) is NotInterested:
                    if "interested" in self.peer_state:
                        self.peer_state.remove("interested")

                elif type(msg) is KeepAlive:
                    pass

                elif type(msg) is Have:
                    pass
                elif type(msg) is Piece:
                    pass

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

    #TODO make request for piece that u want
    async def _request_piece(self):
        pass




    async def _do_handshake(self):
        '''Send handshake which contains info about peer_id and info_hash, wait for handshake to return,
        info_hash must be equal'''

        self.writer.write(Handshake(info_hash=self.info_hash, client_id=self.client_id).encode())
        '''await drain so we do not overwrite data that we sent'''
        await self.writer.drain()

        buffer = b''
        tries = 0

        while tries < 10 and len(buffer) < Handshake.length:
            tries += 1
            buffer = await self.reader(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buffer)

        if not response:
            """TODO later"""
            raise RuntimeError("Could not establish connection with peer")
        if not response.info_hash == self.info_hash:
            """TODO later"""
            raise RuntimeError("Info hash is not correct")

        self.remote_id = response.peer_id

        "return not used part of message"
        return response[Handshake.length:]

    async def _send_interested(self):
        self.writer.write(Interested().encode())
        await self.writer.drain()


class PeerStreamIterator:
    '''Type of async iterator that iterates over messages that peer sends,
        Every next returns type of peerMessage.

        If it fails or connection will be closed,
        Raises StopAsyncIteration and iteration will end.
    '''

    CHUNK_SIZE = 10 * 1024

    def __init__(self, reader, not_used_message):
        self.reader = reader
        self.buffer = not_used_message if not_used_message else b''

    def __aiter__(self):
        return self

    def __anext__(self):

        """refractor"""
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
                return KeepAlive()

            if len(self.buffer) >= message_length:
                message_id = struct.unpack(">b", self.buffer[4:5])

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

        return None


class Handshake:
    """Handshake is not really part of PeerMessages, it is more like start of connection,message used only once """
    """49 is length of message and 19 is currently the size for pstr which is name of Protocol used"""

    length = 49 + 19

    def __init__(self, info_hash, client_id):
        self.pstr = b'BitTorrent protocol'
        self.pstrlen = len(self.pstr)
        self.info_hash = info_hash.encode('utf-8')
        self.peer_id = client_id.encode('utf-8')

    '''Handshake message represented in bytes (ready to be transmitted)'''

    def encode(self):
        return struct.pack('>B19s8x20s20s', self.pstrlen, self.pstr, self.info_hash, self.peer_id)

    """Decodes handshake from user,
      if length is correct tries to parse it and return handshake object , otherwise None"""

    @classmethod
    def decode(cls, response: bytes):
        if len(response) < Handshake.length:
            return None

        segments = struct.unpack('>B19s8x20s20s', response)
        return cls(info_hash=segments[2], client_id=segments[3])


class PeerMessages(Enum):
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
        bitfield_data = struct.unpack('>Ib' + str(message_length) + 's', data)[2]

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

        index: integer specifying the zero-based piece index
        begin: integer specifying the zero-based byte offset within the piece
        block: block of data, which is a subset of the piece specified by index.

    Message format:
         <len=0009+X><id=7><index><begin><block>
    """

    base_length = 9

    def __init__(self, index, begin, block):
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self):
        message_length = Piece.base_length + len(self.block)
        return struct.pack('>IbII' + str(len(self.block)) + 's',
                           message_length,
                           PeerMessages.Piece,
                           self.index,
                           self.begin,
                           self.block
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
