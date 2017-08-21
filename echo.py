import pprint
import configparser
import asyncio

from util import *

cfg = {
    'message_router_host':      None,
    'message_router_port':      4321,
    'message_router_timeout':   None,
    'echo_service_name':        'echo',
    'instance_number':          None,
    'heartbeat_interval':       3
}

# make next variables global for easy access from heartbeat coro
# in real app we will prefer class instance variables
reader = None
writer = None
alive = True
registered = False


def my_id():
    return str(Endpoint(service=cfg['echo_service_name'], instance=cfg['instance_number']))

async def write_my_req(writer, to, body):
    tmsg = TelcoMsg(from_=my_id(), to=to, body=body)
    tmsg.write(writer)

async def read_and_process(reader, writer):
    tmsg = await TelcoMsg.read(reader)
    logging.info("Received. ",  str(tmsg))

    # filter by 'to' field
    if tmsg.to != my_id():
        logging.info("Ignoring incoming message (wrong recipient)")
        return

    # consume tmsg - echoing
    logging.info("Processed. Echoing to: ", tmsg.from_)
    etmsg = TelcoMsg(from_=my_id(), to=tmsg.from_, body=tmsg.body)
    await etmsg.write(writer)

async def echo_client(loop):
    global alive, registered, reader, writer
    while True:
        try:
            reader, writer = await asyncio.open_connection(cfg['message_router_host'],
                                                           cfg['message_router_port'], loop=loop)
            break
        except ConnectionRefusedError:
            logging.info("Unable to connect to '{:s}:{:s}'".format(cfg['message_router_host'], cfg['message_router_port']))
            await asyncio.sleep(int(cfg['message_router_timeout']))

    addr = writer.get_extra_info('peername')
    logging.info("Connected to router: {:s}".format(str(addr)))

    # router registration attempt
    reg_req = TelcoMsg(from_=my_id(), to='message_router', body='register')
    await reg_req.write(writer)

    registered = True   # start sending heartbeats from now

    while True:
        try:
            await read_and_process(reader, writer)
        except KeyboardInterrupt:
            break

    registered = False
    alive = False

    logging.info('Close the socket')
    writer.close()


async def heartbeat_sender():
    global alive, registered, reader, writer

    while alive:
        await asyncio.sleep(float(cfg['heartbeat_interval']))
        if registered:
            logging.info("heartbeat notification sent")
            beat = TelcoMsg(from_=my_id(), to='monitoring', body='heartbeat')
            await beat.write(writer)

async def amain(loop):
    await asyncio.gather(
        echo_client(loop),
        heartbeat_sender()
    )


def main():
    log_init()

    # parsing and combining config settings
    config = configparser.ConfigParser()
    config.read('/etc/iskratel/echo.conf')
    cfg.update(config._sections['config'])

    for arg in sys.argv[1:]:
        arg_dict = arg.split('=')
        cfg.update({arg_dict[0]:arg_dict[1]})

    # aggregated config is ready to use now as 'cfg' dict
    # check for consistency
    absent_dict = {k:v for (k,v) in cfg.items() if not v}
    if absent_dict:
        print("Missed cfg parameters:\n", pprint.pformat(absent_dict))
        sys.exit(1)

    print("Starting module with config values:\n", pprint.pformat(cfg))
    print()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(amain(loop))
    loop.close()


if __name__ == '__main__':
    main()