from bencoding import Decoder,Encoder







if __name__ == '__main__':

    print(Encoder(123).encode())
    print(Encoder(b'Middle Earth').encode())
    print(Encoder(['spam', 'eggs', 123]).encode())
    '''
    with open('C:\\Users\\RADEK_RYZEN\\Downloads\\ubuntu-16.04-desktop-amd64.iso.torrent', 'rb') as f:
        meta_info = f.read()
        torrent = Decoder(meta_info).decode()
        print(torrent)
    '''


