#!/Users/hugo/.virtualenvs/dredis/bin/python

import asyncore
import json
import os.path
import socket
import sys
import traceback

from dredis.commands import run_command
from dredis.keyspace import RedisScriptError, DiskKeyspace
from dredis.parser import parse_instructions



def not_found(send_fn, cmd):
    send_fn("-ERR unknown command '{}'\r\n".format(cmd))


def err(send_fn, tb):
    send_fn("-Server exception: {}\r\n".format(json.dumps(tb)))


def execute_cmd(send_fn, cmd, *args):
    print('cmd={}, args={}'.format(repr(cmd), repr(args)))
    try:
        result = run_command(keyspace, cmd, args)
    except (ValueError, RedisScriptError) as exc:
        send_fn('-{}\r\n'.format(str(exc)))
    except KeyError:
        not_found(send_fn, cmd)
    except Exception:
        err(send_fn, traceback.format_exc())
    else:
        transmit(send_fn, result)


def transmit(send_fn, result):
    if result is None:
        send_fn('$-1\r\n')
    elif isinstance(result, int):
        send_fn(':{}\r\n'.format(result))
    elif isinstance(result, basestring):
        send_fn('${}\r\n{}\r\n'.format(len(result), result))
    elif isinstance(result, (set, list, tuple)):
        send_fn('*{}\r\n'.format(len(result)))
        for element in result:
            transmit(send_fn, element)



class CommandHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        data = self.recv(8192)
        print('data = {}'.format(repr(data)))
        if not data:
            return
        cmd = parse_instructions(data)
        execute_cmd(self.debug_send, *cmd)
        print('')

    def debug_send(self, *args):
        print("out={}".format(repr(args)))
        return self.send(*args)


class RedisServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(1024)

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            print 'Incoming connection from %s' % repr(addr)
            CommandHandler(sock)
            sys.stdout.flush()
            sys.stderr.flush()


keyspace = DiskKeyspace()


if __name__ == '__main__':
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 6377

    keyspace.flushall()

    RedisServer('127.0.0.1', port)
    print 'PID: {}'.format(os.getpid())
    print 'Ready to accept connections'
    sys.stdout.flush()
    asyncore.loop()
