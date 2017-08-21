import asyncio

from util import *

# registration DB: dict.  For larger scale, use at least in-memory sqlite
client_map = {}


async def handle_router(reader, writer):
    addr = writer.get_extra_info('peername')
    logging.info("router accepted connection from: {:s}".format(str(addr)))

    while not reader.at_eof():

        tmsg = await TelcoMsg.read(reader)

        logging.info("recv: '{:s}' from: {:s}".format(str(tmsg), str(addr)))

        from_service, _, from_instance = tmsg.from_.partition('-')
        to_service, _, to_instance =  tmsg.to.partition('-')

        if tmsg.to == 'message_router' and tmsg.body == 'register':
            # registering sender in 'client_map' dictionary
            client_map[Endpoint(service=from_service, instance=from_instance)] = (reader, writer)
            logging.info("registered: {:s}".format(tmsg.from_))
            continue

        if tmsg.to == 'monitoring':
            # do nothing for now, since we have no monitoring instance implemented
            continue

        # try to find direct route
        ep0 = Endpoint(service=to_service, instance=to_instance)
        if ep0 in client_map:
            dst_reader, dst_writer = client_map[ep0]
            await tmsg.write(dst_writer)  # route original TelcoMsg
            logging.info("routed: {:s}".format(str(tmsg)))
            continue

        # try to find route to any (first) instance of requested service
        for ep, v in client_map.items():
            if ep.service == to_service:
                dst_reader, dst_writer = v
                tmsg2 = TelcoMsg(from_=tmsg.from_, to=str(ep), body=tmsg.body)  # patch recipient id
                await tmsg2.write(dst_writer)  # route patched TelcoMsg
                logging.info("smartrouted: {:s}".format(str(tmsg)))
                continue

        # recipient unknown
        logging.info("ignored (recipient unknown): {:s}".format(str(tmsg)))

    await writer.drain()
    logging.info("router closing connection")
    writer.close()

log_init()
loop = asyncio.get_event_loop()
coro = asyncio.start_server(handle_router, '127.0.0.1', 2222, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
logging.info('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()