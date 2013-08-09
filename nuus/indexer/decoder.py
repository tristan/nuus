import binascii
import re

class CrcError(Exception):
    def __init__(self, needcrc, gotcrc, data):
        Exception.__init__(self)
        self.needcrc = needcrc
        self.gotcrc = gotcrc
        self.data = data

class BadYenc(Exception):
    def __init__(self):
        Exception.__init__(self)

YDEC_TRANS = bytes([((i + 256 - 42) % 256) for i in range(256)])
def decode(data):
    data = strip(data)
    if data:
        ybegin = None
        ypart = None
        yend = None
        filename = None

        for i in range(min(40, len(data))):
            try:
                if data[i].startswith(b'=ybegin'):
                    splits = 3
                    if data[i].find(b' part=') > 0:
                        splits += 1
                    if data[i].find(b' total=') > 0:
                        splits += 1
                    ybegin = ysplit(data[i], splits)
                    if data[i+1].startswith(b'=ypart '):
                        ypart = ysplit(data[i+1])
                        data = data[i+2:]
                        break
                    else:
                        data = data[i+1:]
                        break
            except IndexError:
                break
        for i in range(-1, -11, -1):
            try:
                if data[i].startswith(b'=yend '):
                    yend = ysplit(data[i])
                    data = data[:i]
                    break
            except IndexError:
                break
        
        if (ybegin and yend):
            if 'name' in ybegin:
                filename = ybegin['name']
            data = b''.join(data)
            for i in (0, 9, 10, 13, 27, 32, 46, 61):
                j = bytes([61, (i + 64)])
                data = data.replace(j, bytes([i]))
            decoded_data = data.translate(YDEC_TRANS)
            crc = binascii.crc32(decoded_data)
            partcrc = '%08X' % (crc & 2**32 - 1)
       
            if ypart:
                crcname = 'pcrc32'
            else:
                crcname = 'crc32'
            if crcname in yend:
                _partcrc = '0' * (8 - len(yend[crcname])) + yend[crcname].upper()
            else:
                _partcrc = None
            if not (_partcrc == partcrc):
                raise CRCError(_partcrc, parcrc, decoded_data)
        else:
            raise BadYenc()
        return decoded_data
                
def strip(data):
    while data and not data[0]:
        data.pop(0)
    while data and not data[-1]:
        data.pop()
    for i in range(len(data)):
        if data[i][:2] == '..':
            data[i] = data[i][1:]
    return data

YSPLIT_RE = re.compile(r'([a-zA-Z0-9]+)=')
def ysplit(line, maxsplit=0):
    fields = {}
    parts = YSPLIT_RE.split(line.decode(), maxsplit)[1:]
    if len(parts) % 2: # check for invalid results
        return fields
    for i in range(0, len(parts), 2):
        key, value = parts[i], parts[i+1]
        fields[key] = value.strip()
    return fields

def __test():
    return decode(___test)
