from __future__ import annotations
from typing import TYPE_CHECKING, Union, Literal
if TYPE_CHECKING:
    from zmq import SocketType, SocketOption

import time
import logging
from enum import StrEnum
from collections import defaultdict

import zmq
from msgspec import msgpack

from pfund.enums import PublicDataChannel, PrivateDataChannel, PFundDataChannel, PFundDataTopic


class SocketMethod(StrEnum):
    bind = "bind"
    connect = "connect"


class ZeroMQDataChannel(StrEnum):
    signal = 'signal'
DataChannel = Union[ZeroMQDataChannel, PFundDataChannel, PublicDataChannel, PrivateDataChannel]
JSONValue = Union[dict, list, str, int, float, bool, None]


class ZeroMQSignal(StrEnum):
    START = 'START'
    STOP = 'STOP'


# TODO: need to tunnel zeromq connections with SSH when communicating with remote ray actors
# see https://pyzmq.readthedocs.io/en/latest/howto/ssh.html
class ZeroMQ:
    '''
    Thin wrapper of zmq to handle sockets for both sending and receiving messages with exception handling.
    '''
    DEFAULT_URL = 'tcp://localhost'

    def __init__(
        self,
        name: str,
        logger: logging.Logger,
        url: str=DEFAULT_URL,
        io_threads: int=1,
        *,
        sender_method: SocketMethod | Literal['bind', 'connect']='bind',
        receiver_method: SocketMethod | Literal['bind', 'connect']='connect',
        sender_type: SocketType | None=None,
        receiver_type: SocketType | None=None,
        send_latest_only: bool=False,
        recv_latest_only: bool=False,
    ):
        '''
        Args:
            port: If not provided, a random port will be assigned.
            send_latest_only : bool, optional
                If True, sets `ZMQ_CONFLATE=1` on the sender socket.
                Only the newest unsent message is kept; every new send overwrites any previous.
                Use when only the latest update matters—intermediate messages are dropped.

            recv_latest_only : bool, optional
                If True, sets `ZMQ_CONFLATE=1` on the receiver socket.
                Only the newest unprocessed message is kept; arriving messages overwrite older ones if not yet received.
                Use when you only care about the most recent data and not the message history.
        '''
        assert any([sender_type, receiver_type]), 'Either sender_type or receiver_type must be provided'
        self._name = name
        self._logger = logger
        self._url = url
        self._ctx = zmq.Context(io_threads=io_threads)
        self._socket_methods: dict[zmq.Socket, SocketMethod] = {}
        self._socket_ports: defaultdict[zmq.Socket, list[int]] = defaultdict(list)
        self._sender: zmq.Socket | None = None
        self._target_identity: bytes | None = None  # identity of the sender socket to send to, currently only used for ROUTER socket
        self._receiver: zmq.Socket | None = None
        self._poller: zmq.Poller | None = None

        if sender_type:
            self._sender = self._ctx.socket(sender_type)
            self._socket_methods[self._sender] = SocketMethod[sender_method.lower()]
            # only queue outgoing messages once the remote peer is fully connected—otherwise block (or error) instead of buffering endlessly.
            self._sender.setsockopt(zmq.IMMEDIATE, 1)
            if send_latest_only:
                self._sender.setsockopt(zmq.CONFLATE, 1)
        if receiver_type:
            self._receiver = self._ctx.socket(receiver_type)
            self._socket_methods[self._receiver] = SocketMethod[receiver_method.lower()]
            self._poller = zmq.Poller()
            self._poller.register(self._receiver, zmq.POLLIN)
            if recv_latest_only:
                self._receiver.setsockopt(zmq.CONFLATE, 1)
            
    @property
    def name(self) -> str:
        if 'zeromq' not in self._name.lower() or 'zmq' not in self._name.lower():
            return self._name + '_zmq'
        else:
            return self._name
    
    @property
    def sender_name(self) -> str:
        return '.'.join([self.name, self._sender.socket_type.name, 'sender'])
    
    @property
    def receiver_name(self) -> str:
        return '.'.join([self.name, self._receiver.socket_type.name, 'receiver'])
    
    @property
    def sender(self) -> zmq.Socket:
        assert self._sender, 'sender is not initialized'
        return self._sender
    
    @property
    def receiver(self) -> zmq.Socket:
        assert self._receiver, 'receiver is not initialized'
        return self._receiver
    
    def set_target_identity(self, identity: str):
        assert self._sender, 'sender is not initialized'
        self._target_identity = identity.encode()
    
    def get_ports_in_use(self, socket: zmq.Socket) -> list[int]:
        return self._socket_ports[socket]
    
    def bind(self, socket: zmq.Socket, port: int | None=None):
        '''Binds a socket which uses bind method to a port.'''
        assert socket in self._socket_methods, f'{socket=} has not been initialized'
        assert self._socket_methods[socket] == SocketMethod.bind, f'{socket=} is not a socket used for binding'
        if port is None:
            port: int = socket.bind_to_random_port(self._url)
        else:
            socket.bind(f"{self._url}:{port}")
        if port not in self._socket_ports[socket]:
            self._socket_ports[socket].append(port)
        else:
            raise ValueError(f'{port=} is already bound')
    
    def connect(self, socket: zmq.Socket, port: int):
        '''Connects to a port which uses connect method.'''
        assert socket in self._socket_methods, f'{socket=} has not been initialized'
        assert self._socket_methods[socket] == SocketMethod.connect, f'{socket=} is not a socket used for connecting'
        socket.connect(f"{self._url}:{port}")
        if port not in self._socket_ports[socket]:
            self._socket_ports[socket].append(port)
        else:
            raise ValueError(f'{port=} is already subscribed')
    
    def terminate(self):
        if self._poller and self._receiver:
            self._poller.unregister(self._receiver)

        # terminate sockets
        for socket in [self._sender, self._receiver]:
            if socket is None:
                continue
            for port in self._socket_ports[socket]:
                if self._socket_methods[socket] == SocketMethod.bind:
                    socket.unbind(f"{self._url}:{port}")
                else:
                    socket.disconnect(f"{self._url}:{port}")
            socket.setsockopt(zmq.LINGER, 5000)  # wait up to 5 seconds, then close anyway
            socket.close()

        # terminate context
        self._ctx.term()

    def send(self, channel: DataChannel | str, topic: PFundDataTopic | str, data: JSONValue) -> None:
        '''
        Sends message to receivers
        Args:
            data: A JSON serializable object.
            topic: A message key used to group messages within a channel.
        '''
        try:
            msg_ts = time.time()
            msg = [channel.encode(), topic.encode(), msgpack.encode(data), msgpack.encode(msg_ts)]
            if self._sender.socket_type == zmq.ROUTER:
                msg.insert(0, self._target_identity)
            self._sender.send_multipart(msg, zmq.NOBLOCK)
        except zmq.error.Again:  # raised when send queue is full and using zmq.NOBLOCK
            hwm = self._sender.getsockopt(zmq.SNDHWM)
            self._logger.warning(f'{self.sender_name} zmq.error.Again, HWM={hwm}, {msg=}')
        except zmq.error.ContextTerminated:
            self._logger.debug(f'{self.sender_name} context terminated')
        except zmq.ZMQError as err:
            self._logger.exception(f'{self.sender_name} send() exception (errno={err.errno}):')
        except Exception:
            self._logger.exception(f'{self.sender_name} send() unhandled exception:')

    def recv(self) -> tuple[DataChannel | str, PFundDataTopic | str, JSONValue, float] | None:
        try:
            # REVIEW: blocks for 1ms to avoid busy-waiting and 100% CPU usage
            events = self._poller.poll(1)
            if events:
                msg = self._receiver.recv_multipart(zmq.NOBLOCK)
                channel, topic, data, msg_ts = msg
                return channel.decode(), topic.decode(), msgpack.decode(data), msgpack.decode(msg_ts)
        except zmq.error.Again:  # no message available, will be raised when using zmq.NOBLOCK
            self._logger.warning(f'{self.receiver_name} zmq.error.Again, no message available')
        except zmq.error.ContextTerminated:
            self._logger.debug(f'{self.receiver_name} context terminated')
        except zmq.ZMQError as err:
            self._logger.exception(f'{self.receiver_name} recv() exception (errno={err.errno}):')
        except Exception:
            self._logger.exception(f'{self.receiver_name} recv() unhandled exception:')
    