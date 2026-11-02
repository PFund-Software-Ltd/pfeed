from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from pytest_mock import MockerFixture

import logging
import time

import pytest
import zmq

from pfeed.streaming.zeromq import ZeroMQ

TIMEOUT_SECONDS = 5
URL = ZeroMQ.DEFAULT_URL


@pytest.fixture
def logger(mocker: MockerFixture):
    return mocker.Mock(spec=logging.Logger)


@pytest.fixture
def make(logger) -> Iterator[Callable[..., ZeroMQ]]:
    """Creates ZeroMQ instances and cleans up the ones a test didn't terminate."""
    instances: list[tuple[ZeroMQ, list[zmq.Socket]]] = []

    def _make(name: str = "test", **kwargs) -> ZeroMQ:
        instance = ZeroMQ(name, logger, **kwargs)
        # NOTE: stored separately since a test can replace instance._sender with a mock
        sockets = [socket for socket in (instance._sender, instance._receiver) if socket is not None]
        instances.append((instance, sockets))
        return instance

    yield _make
    for instance, sockets in instances:
        for socket in sockets:
            if not socket.closed:
                socket.close(0)
        if not instance._ctx.closed:
            instance._ctx.term()


def _recv(receiver: ZeroMQ) -> tuple | None:
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if (msg := receiver.recv()) is not None:
            return msg
    return None


def _make_router_and_dealer(make: Callable[..., ZeroMQ], identity: str = "worker") -> tuple[ZeroMQ, ZeroMQ]:
    router = make("router", sender_type=zmq.ROUTER)
    router.bind(router.sender)
    dealer = make("dealer", receiver_type=zmq.DEALER)
    dealer.receiver.setsockopt(zmq.IDENTITY, identity.encode())
    # NOTE: the ROUTER can only route to the DEALER after receiving this probe
    dealer.receiver.setsockopt(zmq.PROBE_ROUTER, 1)
    dealer.connect(dealer.receiver, router.get_ports_in_use(router.sender)[0])
    assert router.sender.poll(TIMEOUT_SECONDS * 1000)
    router.sender.recv_multipart()
    return router, dealer


def _make_push_and_pull(make: Callable[..., ZeroMQ]) -> tuple[ZeroMQ, ZeroMQ]:
    pull = make("pull", receiver_type=zmq.PULL, receiver_method="bind")
    pull.bind(pull.receiver)
    push = make("push", sender_type=zmq.PUSH, sender_method="connect")
    push.connect(push.sender, pull.get_ports_in_use(pull.receiver)[0])
    # NOTE: PUSH with IMMEDIATE can't send until connected, so retry until a message gets through
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while pull.recv() is None:
        assert time.monotonic() < deadline, "PUSH failed to connect to PULL"
        push.send("probe", "probe", "probe")
    while pull.recv() is not None:
        pass
    return push, pull


class TestInit:
    def test_requires_a_socket_type(self, logger):
        with pytest.raises(AssertionError, match="must be provided"):
            ZeroMQ("test", logger)

    def test_accepts_pair_whose_socket_type_is_zero(self, make):
        assert zmq.PAIR == 0
        assert make(sender_type=zmq.PAIR).sender.type == zmq.PAIR
        assert make(receiver_type=zmq.PAIR).receiver.type == zmq.PAIR

    def test_sets_immediate_on_sender(self, make):
        assert make(sender_type=zmq.PUSH).sender.getsockopt(zmq.IMMEDIATE) == 1

    def test_only_creates_requested_sockets(self, make):
        sender_only = make(sender_type=zmq.PUSH)
        with pytest.raises(AssertionError, match="receiver is not initialized"):
            _ = sender_only.receiver
        receiver_only = make(receiver_type=zmq.PULL)
        with pytest.raises(AssertionError, match="sender is not initialized"):
            _ = receiver_only.sender


class TestNames:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("faucet", "faucet_zmq"),
            ("faucet_zmq", "faucet_zmq"),
            ("faucet_zeromq", "faucet_zeromq"),
            ("FAUCET_ZMQ", "FAUCET_ZMQ"),
        ],
    )
    def test_name_appends_suffix_only_if_missing(self, make, name, expected):
        assert make(name, sender_type=zmq.PUSH).name == expected

    def test_sender_and_receiver_names(self, make):
        zeromq = make("engine", sender_type=zmq.XPUB, receiver_type=zmq.PULL)
        assert zeromq.sender_name == "engine_zmq.XPUB.sender"
        assert zeromq.receiver_name == "engine_zmq.PULL.receiver"


class TestAddresses:
    def test_bind_to_random_port(self, make):
        zeromq = make(sender_type=zmq.ROUTER)
        zeromq.bind(zeromq.sender)
        port = zeromq.get_ports_in_use(zeromq.sender)[0]
        assert port > 0
        assert zeromq.get_addresses_in_use(zeromq.sender) == [f"{URL}:{port}"]
        assert zeromq.get_urls_in_use(zeromq.sender) == [URL]

    def test_bind_to_given_port(self, make):
        ctx = zmq.Context()
        socket = ctx.socket(zmq.PULL)
        port = socket.bind_to_random_port(URL)
        socket.close(0)
        ctx.term()

        zeromq = make(sender_type=zmq.ROUTER)
        zeromq.bind(zeromq.sender, port=port)
        assert zeromq.get_ports_in_use(zeromq.sender) == [port]

    def test_unbind_removes_address(self, make):
        zeromq = make(sender_type=zmq.ROUTER)
        zeromq.bind(zeromq.sender)
        zeromq.unbind(zeromq.sender, zeromq.get_addresses_in_use(zeromq.sender)[0])
        assert zeromq.get_addresses_in_use(zeromq.sender) == []

    def test_connect_and_disconnect(self, make):
        pull = make(receiver_type=zmq.PULL, receiver_method="bind")
        pull.bind(pull.receiver)
        port = pull.get_ports_in_use(pull.receiver)[0]

        zeromq = make(sender_type=zmq.PUSH, sender_method="connect")
        zeromq.connect(zeromq.sender, port)
        assert zeromq.get_ports_in_use(zeromq.sender) == [port]
        zeromq.disconnect(zeromq.sender, f"{URL}:{port}")
        assert zeromq.get_addresses_in_use(zeromq.sender) == []

    def test_connect_twice_raises(self, make):
        pull = make(receiver_type=zmq.PULL, receiver_method="bind")
        pull.bind(pull.receiver)
        port = pull.get_ports_in_use(pull.receiver)[0]

        zeromq = make(sender_type=zmq.PUSH, sender_method="connect")
        zeromq.connect(zeromq.sender, port)
        with pytest.raises(ValueError, match="already subscribed"):
            zeromq.connect(zeromq.sender, port)

    def test_rejects_the_wrong_socket_method(self, make):
        binder = make(sender_type=zmq.ROUTER)
        with pytest.raises(AssertionError, match="registered with"):
            binder.connect(binder.sender, 1234)
        with pytest.raises(AssertionError, match="registered with"):
            binder.disconnect(binder.sender, f"{URL}:1234")

        connector = make(sender_type=zmq.PUSH, sender_method="connect")
        with pytest.raises(AssertionError, match="registered with"):
            connector.bind(connector.sender)
        with pytest.raises(AssertionError, match="registered with"):
            connector.unbind(connector.sender, f"{URL}:1234")

    def test_rejects_a_socket_it_does_not_own(self, make):
        owner = make(sender_type=zmq.ROUTER)
        other = make(sender_type=zmq.ROUTER)
        with pytest.raises(AssertionError, match="has not been initialized"):
            owner.bind(other.sender)
        with pytest.raises(AssertionError, match=r"is not a zmq\.Socket"):
            owner.bind("not a socket")


class TestSendAndRecv:
    def test_recv_returns_none_without_messages(self, make):
        zeromq = make(receiver_type=zmq.PULL, receiver_method="bind")
        zeromq.bind(zeromq.receiver)
        assert zeromq.recv() is None

    def test_push_to_pull(self, make, logger):
        push, pull = _make_push_and_pull(make)
        logger.reset_mock()

        push.send("channel", "topic", {"price": 1.5, "items": [1, 2]})

        msg = _recv(pull)
        assert msg is not None
        channel, topic, data, msg_ts = msg
        assert (channel, topic, data) == ("channel", "topic", {"price": 1.5, "items": [1, 2]})
        assert isinstance(msg_ts, float)
        logger.exception.assert_not_called()

    def test_router_sends_to_target_identity_only(self, make):
        router = make("router", sender_type=zmq.ROUTER)
        router.bind(router.sender)
        port = router.get_ports_in_use(router.sender)[0]
        dealers = {}
        for identity in ("worker-1", "worker-2"):
            dealer = make(identity, receiver_type=zmq.DEALER)
            dealer.receiver.setsockopt(zmq.IDENTITY, identity.encode())
            dealer.receiver.setsockopt(zmq.PROBE_ROUTER, 1)
            dealer.connect(dealer.receiver, port)
            dealers[identity] = dealer
        for _ in dealers:
            assert router.sender.poll(TIMEOUT_SECONDS * 1000)
            router.sender.recv_multipart()

        router.send("channel", "topic", "for worker-2", target_identity="worker-2")

        msg = _recv(dealers["worker-2"])
        assert msg is not None
        assert msg[:3] == ("channel", "topic", "for worker-2")
        assert dealers["worker-1"].receiver.poll(100) == 0

    def test_target_identity_is_ignored_by_other_socket_types(self, make):
        push, pull = _make_push_and_pull(make)
        push.send("channel", "topic", "data", target_identity="worker")
        msg = _recv(pull)
        assert msg is not None
        assert msg[:3] == ("channel", "topic", "data")

    @pytest.mark.parametrize("target_identity", ["unknown", None])
    def test_router_raises_on_undeliverable_message(self, make, target_identity):
        router = make(sender_type=zmq.ROUTER)
        router.bind(router.sender)

        with pytest.raises(zmq.ZMQError) as exc_info:
            router.send("channel", "topic", "data", target_identity=target_identity)

        assert exc_info.value.errno == zmq.EHOSTUNREACH

    def test_again_logs_channel_and_topic_but_not_the_message(self, make, logger):
        push = make(sender_type=zmq.PUSH, sender_method="connect")  # no peer, so zmq.Again

        push.send("channel", "topic", "secret-payload")

        logger.warning.assert_called_once()
        warning = logger.warning.call_args.args[0]
        assert "zmq.error.Again" in warning
        assert "channel" in warning
        assert "topic" in warning
        assert "secret-payload" not in warning

    def test_send_logs_unserializable_data(self, make, logger):
        push = make(sender_type=zmq.PUSH, sender_method="connect")

        push.send("channel", "topic", object())

        logger.exception.assert_called_once()
        assert "unhandled exception" in logger.exception.call_args.args[0]

    def test_send_logs_context_terminated(self, make, logger, mocker: MockerFixture):
        push = make(sender_type=zmq.PUSH, sender_method="connect")
        sender = mocker.Mock()
        sender.send_multipart.side_effect = zmq.error.ContextTerminated()
        sender.type = zmq.PUSH
        push._sender = sender

        push.send("channel", "topic", "data")

        logger.debug.assert_called_once()
        logger.exception.assert_not_called()

    def test_recv_logs_malformed_message(self, make, logger):
        push, pull = _make_push_and_pull(make)
        logger.reset_mock()

        push.sender.send(b"one frame only")

        deadline = time.monotonic() + TIMEOUT_SECONDS
        while not logger.exception.called and time.monotonic() < deadline:
            assert pull.recv() is None
        logger.exception.assert_called_once()
        assert "unhandled exception" in logger.exception.call_args.args[0]

    @pytest.mark.parametrize(
        ("error", "log_method"),
        [
            (zmq.error.Again(), "warning"),
            (zmq.error.ContextTerminated(), "debug"),
            (zmq.ZMQError(zmq.EFAULT), "exception"),
        ],
    )
    def test_recv_logs_zmq_errors_and_returns_none(self, make, logger, mocker: MockerFixture, error, log_method):
        zeromq = make(receiver_type=zmq.PULL, receiver_method="bind")
        zeromq._poller = mocker.Mock(poll=mocker.Mock(side_effect=error))

        assert zeromq.recv() is None

        getattr(logger, log_method).assert_called_once()


class TestTerminate:
    def test_sends_stop_to_each_target_identity(self, make):
        router = make("router", sender_type=zmq.ROUTER)
        router.bind(router.sender)
        port = router.get_ports_in_use(router.sender)[0]
        dealers = []
        for identity in ("worker-1", "worker-2"):
            dealer = make(identity, receiver_type=zmq.DEALER)
            dealer.receiver.setsockopt(zmq.IDENTITY, identity.encode())
            dealer.receiver.setsockopt(zmq.PROBE_ROUTER, 1)
            dealer.connect(dealer.receiver, port)
            dealers.append(dealer)
        for _ in dealers:
            assert router.sender.poll(TIMEOUT_SECONDS * 1000)
            router.sender.recv_multipart()

        router.terminate(target_identities=["worker-1", "worker-2"])

        for dealer in dealers:
            msg = _recv(dealer)
            assert msg is not None
            assert msg[:3] == ("signal", "router_zmq.ROUTER.sender", "STOP")

    def test_dead_target_does_not_stop_terminate(self, make, logger):
        router = make("router", sender_type=zmq.ROUTER)
        router.bind(router.sender)
        dealer = make("worker", receiver_type=zmq.DEALER)
        dealer.receiver.setsockopt(zmq.IDENTITY, b"worker-1")
        dealer.receiver.setsockopt(zmq.PROBE_ROUTER, 1)
        dealer.connect(dealer.receiver, router.get_ports_in_use(router.sender)[0])
        assert router.sender.poll(TIMEOUT_SECONDS * 1000)
        router.sender.recv_multipart()

        router.terminate(target_identities=["dead-worker", "worker-1"])

        msg = _recv(dealer)
        assert msg is not None
        assert msg[2] == "STOP"
        assert router.sender.closed
        logger.warning.assert_called_once()
        assert "dead-worker" in logger.warning.call_args.args[0]

    def test_sends_stop_without_target_identities(self, make):
        push, pull = _make_push_and_pull(make)

        push.terminate()

        msg = _recv(pull)
        assert msg is not None
        assert msg[:3] == ("signal", "push_zmq.PUSH.sender", "STOP")

    def test_ignores_target_identities_on_other_socket_types(self, make):
        push, pull = _make_push_and_pull(make)

        push.terminate(target_identities=["worker-1", "worker-2"])

        msg = _recv(pull)
        assert msg is not None
        assert msg[2] == "STOP"
        assert pull.recv() is None

    def test_unbinds_and_closes_everything(self, make):
        zeromq = make(sender_type=zmq.XPUB, receiver_type=zmq.PULL, receiver_method="bind")
        zeromq.bind(zeromq.sender)
        zeromq.bind(zeromq.receiver)
        sender, receiver = zeromq.sender, zeromq.receiver

        zeromq.terminate()

        assert zeromq.get_addresses_in_use(sender) == []
        assert zeromq.get_addresses_in_use(receiver) == []
        assert sender.closed
        assert receiver.closed
        assert zeromq._ctx.closed

    def test_disconnects_and_closes_everything(self, make):
        pull = make(receiver_type=zmq.PULL, receiver_method="bind")
        pull.bind(pull.receiver)
        zeromq = make(receiver_type=zmq.DEALER)
        zeromq.connect(zeromq.receiver, pull.get_ports_in_use(pull.receiver)[0])
        receiver = zeromq.receiver

        zeromq.terminate()

        assert zeromq.get_addresses_in_use(receiver) == []
        assert receiver.closed
        assert zeromq._ctx.closed
