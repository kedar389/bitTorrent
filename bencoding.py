from collections import OrderedDict

TOKEN_INTEGER = b'i'
TOKEN_STRING_DELIMITER = b':'
TOKEN_LIST = b'l'
TOKEN_DICTIONARY = b'd'
TOKEN_TERMINATOR = b'e'

"""Class for decoding a sequence of bytes ,"""


class Decoder():

    def __init__(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError("Data should be type of bytes")
        self._data = data
        self._index = 0

    def decode(self):
        char = self._peek_token()

        if char is None:
            raise EOFError('Unexpected End of file')

        elif char == TOKEN_INTEGER:
            self._consume_token()
            return self._decode_int()

        elif char == TOKEN_LIST:
            self._consume_token()
            return self._decode_list()

        elif char == TOKEN_DICTIONARY:
            self._consume_token()
            return self._decode_dictionary()

        elif char in b'0123456789':
            return self._decode_string()

        elif char == TOKEN_TERMINATOR:
            self._consume_token()
            return None

        else:
            raise RuntimeError("Token {0} is not valid,Found at Index {1}".format(str(char), self._index))

    def _consume_token(self):
        """Increases index to represent that char was read"""
        self._index += 1

    def _peek_token(self):
        """Returns current char in sequence ,otherwise None """
        next_index = self._index + 1
        if next_index > len(self._data):
            return None

        """We have to slice to return char in represented in bytes ,otherwise we would get number representing ASCII code """
        return self._data[self._index:next_index]

    def _read_until_token(self, token):
        """ read sequence until specified token, otherwise Value error"""
        try:
            index_of_token = self._data.index(token, self._index)
            result_sequence = self._data[self._index:index_of_token]
            self._index = index_of_token + 1
            return result_sequence
        except ValueError:
            raise RuntimeError("Unable to find token {0}".format(str(token)))

    def _read_bytes(self, number_of_bytes):
        """Reads number of bytes from sequence , increases index corresponding to number of bytes """

        if number_of_bytes + self._index > len(self._data):
            raise IndexError("Length of sequence is {0},Cannot read {1} bytes from position {2}".format(len(self._data),
                                                                                                        number_of_bytes,
                                                                                                        self._index))

        result_sequence = self._data[self._index:self._index + number_of_bytes]
        self._index += number_of_bytes
        return result_sequence

    def _decode_int(self):
        return int(self._read_until_token(TOKEN_TERMINATOR))

    def _decode_string(self):
        bytes_to_read = int(self._read_until_token(TOKEN_STRING_DELIMITER))
        return self._read_bytes(bytes_to_read)

    def _decode_list(self):
        decoded_list = []

        while( (data := self.decode()) != None ):
            decoded_list.append(data)

        return decoded_list

    def _decode_dictionary(self):
        dict  = OrderedDict()

        while (self._data[self._index: self._index + 1] != TOKEN_TERMINATOR):
            key = self.decode()
            if not isinstance(key, bytes):
                raise TypeError("Key should be a Binary String")
            object = self.decode()
            dict[key] = object

        return dict


class Encoder():
    def __init__(self, data):
        self._data = data


    def encode(self):

        if isinstance(self._data,int):
            return self._encode_int()

        elif isinstance(self._data,bytes):
            return self._encode_bytes()

        elif isinstance(self._data, str):
            return self._encode_string()

        elif isinstance(self._data, list):
            return self._encode_list()

        elif isinstance(self._data, dict) or isinstance(self._data, OrderedDict) :
            return self._encode_dict()

        else:
            raise RuntimeError("Cannot encode {0}".format(type(self._data)))





    def _encode_bytes(self):
        return (str(len(self._data)) + ':').encode() + self._data

    def _encode_int(self):
       return ('i' + str(self._data) + 'e').encode()

    def _encode_string(self):
        return ( str(len(self._data)) + ':' + self._data ).encode()

    def _encode_list(self):
        encoded_list = bytearray()
        encoded_list += b'l'
        return encoded_list
    def _encode_dict(self):
        pass
