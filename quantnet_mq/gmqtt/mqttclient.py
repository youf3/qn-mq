import socket
from gmqtt import Client
from gmqtt.mqtt.constants import PubRecReasonCode
from quantnet_mq import MQTTClientInterface

# used in rpcserver.py
PubRecReasonCode


class MQTTClient(MQTTClientInterface, Client):
    def __init__(self, client_id, clean_session=True, optimistic_acknowledgement=True,
                 will_message=None, **kwargs):
        super(MQTTClient, self).__init__(client_id, clean_session, optimistic_acknowledgement, will_message, **kwargs)

    async def _create_connection(self, host, port, ssl, clean_session, keepalive):
        connection = await super(MQTTClient, self)._create_connection(
            host, port, ssl, clean_session, keepalive
        )
        transport = getattr(connection, "_transport", None)
        if transport is not None:
            sock = transport.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
        return connection

    def topic_match(self, sub, topic):
        """ check if topic start with sub """
        return True if sub in topic else False

    def topic_tokenise(self, topic):
        """ TODO: break the topic into token array """
        return topic.split('/')

    def topic_wildcard(self, topic):
        """ """
        return topic.split('/')[0]+'/+'

    def set_will(self, topic, msg, qos, ratain):
        """ TODO: set the will message """
        pass

    def set_tls_psk(self, id, psk):
        """ TODO: set the psk """
        pass

    def set_ca(self, capath):
        """ TODO setup the ca """
        pass
