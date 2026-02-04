from __future__ import annotations
from typing import TYPE_CHECKING, Literal, Any, Union
if TYPE_CHECKING:
    from zmq import SocketType, SocketOption
    from pfund.accounts.account_base import BaseAccount
    from pfund.products.product_base import BaseProduct
    from pfund.datas.resolution import Resolution
    from pfund.enums import PrivateDataChannel
    from pfeed.enums import DataSource

import time
import logging
from enum import StrEnum
from collections import defaultdict

import zmq
from msgspec import msgpack, Raw, ValidationError


class SocketMethod(StrEnum):
    bind = "bind"
    connect = "connect"


class ZeroMQDataChannel(StrEnum):
    signal = 'signal'

    @staticmethod
    def create_private_channel(account: BaseAccount, channel: PrivateDataChannel):
        return f'{account.trading_venue}.{account.name}.{channel}'

    @staticmethod
    def create_market_data_channel(data_source: DataSource, product: BaseProduct, resolution: Resolution) -> str:
        return f'{product.trading_venue}.{data_source}.{repr(resolution)}.{product.name}'


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
        from pfeed.streaming import BarMessage
        
        assert any([sender_type, receiver_type]), 'Either sender_type or receiver_type must be provided'
        self._name = name
        self._logger = logger
        self._ctx = zmq.Context(io_threads=io_threads)
        self._socket_methods: dict[zmq.Socket, SocketMethod] = {}
        self._socket_addresses: defaultdict[zmq.Socket, list[str]] = defaultdict(list)
        self._sender: zmq.Socket | None = None
        self._target_identity: bytes | None = None  # identity of the sender socket to send to, currently only used for ROUTER socket
        self._receiver: zmq.Socket | None = None
        self._poller: zmq.Poller | None = None
        self._encoder = msgpack.Encoder()
        self._decoder = msgpack.Decoder()
        self._msg_decoder = msgpack.Decoder(type=Union[BarMessage])
        self._peek_decoder = msgpack.Decoder(type=dict[str, Raw])

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
        if self._sender.socket_type != zmq.ROUTER:
            raise ValueError(
                f'set_target_identity() is only supported for ROUTER socket, '
                f'but sender socket type is {self._sender.socket_type.name}'
            )
        self._target_identity = identity.encode()
    
    def get_addresses_in_use(self, socket: zmq.Socket) -> list[str]:
        return self._socket_addresses[socket]
    
    def get_urls_in_use(self, socket: zmq.Socket) -> list[str]:
        addresses = self.get_addresses_in_use(socket)
        return [addr.split(':')[0] for addr in addresses]
    
    def get_ports_in_use(self, socket: zmq.Socket) -> list[int]:
        addresses = self.get_addresses_in_use(socket)
        return [int(addr.split(':')[-1]) for addr in addresses]
    
    def _assert_socket_initialized(self, socket: zmq.Socket, socket_method: SocketMethod):
        assert isinstance(socket, zmq.Socket), f'{socket=} is not a zmq.Socket'
        assert socket in self._socket_methods, f'{socket=} has not been initialized'
        assert self._socket_methods[socket] == socket_method, f'{socket=} registered with socket_method={self._socket_methods[socket]}'
    
    def bind(self, socket: zmq.Socket, port: int | None=None, url: str=DEFAULT_URL):
        '''Binds a socket which uses bind method to a port.'''
        self._assert_socket_initialized(socket, SocketMethod.bind)
        if port is None:
            port: int = socket.bind_to_random_port(url)
            address = f"{url}:{port}"
        else:
            address = f"{url}:{port}"
            socket.bind(address)
        if address not in self._socket_addresses[socket]:
            self._socket_addresses[socket].append(address)
        else:
            raise ValueError(f'{address=} is already bound')
    
    def unbind(self, socket: zmq.Socket, address: str):
        self._assert_socket_initialized(socket, SocketMethod.bind)
        socket.unbind(address)
        if address in self._socket_addresses[socket]:
            self._socket_addresses[socket].remove(address)
    
    def connect(self, socket: zmq.Socket, port: int, url: str=DEFAULT_URL):
        '''Connects to a port which uses connect method.'''
        self._assert_socket_initialized(socket, SocketMethod.connect)
        address = f"{url}:{port}"
        socket.connect(address)
        if address not in self._socket_addresses[socket]:
            self._socket_addresses[socket].append(address)
        else:
            raise ValueError(f'{address=} is already subscribed')
    
    def disconnect(self, socket: zmq.Socket, address: str):
        self._assert_socket_initialized(socket, SocketMethod.connect)
        socket.disconnect(address)
        if address in self._socket_addresses[socket]:
            self._socket_addresses[socket].remove(address)
    
    def terminate(self):
        # send STOP signal to notice listeners, which probably have a while True loop running
        if self._sender:
            self.send(channel=ZeroMQDataChannel.signal, topic=self.sender_name, data=ZeroMQSignal.STOP)
        
        if self._poller and self._receiver:
            self._poller.unregister(self._receiver)

        # terminate sockets
        for socket in [self._sender, self._receiver]:
            if socket is None:
                continue
            for address in self._socket_addresses[socket][:]:
                if self._socket_methods[socket] == SocketMethod.bind:
                    self.unbind(socket, address)
                else:
                    self.disconnect(socket, address)
            socket.setsockopt(zmq.LINGER, 5000)  # wait up to 5 seconds, then close anyway
            socket.close()

        # terminate context
        self._ctx.term()

    def send(self, channel: str, topic: str, data: Any) -> None:
        '''
        Sends message to receivers
        Args:
            data: A JSON serializable object.
            topic: A message key used to group messages within a channel.
        '''
        try:
            msg_ts = time.time()
            msg = [channel.encode(), topic.encode(), self._encoder.encode(data), self._encoder.encode(msg_ts)]
            # REVIEW: currently only used for ROUTER socket
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

    def recv(self) -> tuple[str, str, Any, float] | None:
        try:
            def _is_streaming_msg(buf: bytes) -> bool:
                '''Peeks at the fields to check if the message is of type StreamingMessage, etc.'''
                try:
                    outer = self._peek_decoder.decode(buf)
                except ValidationError:
                    return False  # Not a dict at the top level
                # if 'specs' and '_created_at' is also present, it is a streaming message
                if outer.get("_created_at") and outer.get('specs'):  
                    return True
                else:
                    return False

            # REVIEW: blocks for 1ms to avoid busy-waiting and 100% CPU usage
            events = self._poller.poll(1)
            if events:
                msg = self._receiver.recv_multipart(zmq.NOBLOCK)
                channel, topic, data, msg_ts = msg
                if _is_streaming_msg(data):
                    decoded_data = self._msg_decoder.decode(data)
                else:
                    decoded_data = msgpack.decode(data)
                return channel.decode(), topic.decode(), decoded_data, self._decoder.decode(msg_ts)
            else:
                return None
        except zmq.error.Again:  # no message available, will be raised when using zmq.NOBLOCK
            self._logger.warning(f'{self.receiver_name} zmq.error.Again, no message available')
        except zmq.error.ContextTerminated:
            self._logger.debug(f'{self.receiver_name} context terminated')
        except zmq.ZMQError as err:
            self._logger.exception(f'{self.receiver_name} recv() exception (errno={err.errno}):')
        except Exception:
            self._logger.exception(f'{self.receiver_name} recv() unhandled exception:')
    