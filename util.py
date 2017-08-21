import sys
import struct
import json
import collections
import logging

# write msg to socket/writer using length-prefixed binary protocol
async def write_msg(writer, rawmsg):
    writer.write(b''.join([struct.pack('>H', len(rawmsg)), bytes(rawmsg, 'ascii')]))

# read msg from socket/writer expecting length-prefixed binary protocol
async def read_msg(reader):
    msg_len = struct.unpack('>H', await reader.readexactly(2))
    return b'' if not msg_len else await reader.readexactly(msg_len[0])

# composite 'service-instance' class (used as composite keys)
class Endpoint(collections.namedtuple('Endpoint', ['service', 'instance'])):
    __slots__ = ()

    def __str__(self):
        return '-'.join([self.service, self.instance])

# general event msg
class TelcoMsg(collections.namedtuple('TelcoMsg', ['from_', 'to', 'body'])):
    __slots__ = ()

    def __str__(self):
        return 'TelcoMsg: {from=' + str(self.from_) + ', to=' + str(self.to) + ', body=' + str(self.body) + '}'

    async def write(self, writer):
        await write_msg(writer, json.dumps({'from': self.from_, 'to': self.to, 'body': self.body}))

    @staticmethod
    async def read(reader):
        raw = await read_msg(reader)
        parsed = json.loads(raw)
        return TelcoMsg(from_=parsed['from'], to=parsed['to'], body=parsed['body'])

# init log dumps to stdout
def log_init():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(filename)s - %(message)s"))
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)