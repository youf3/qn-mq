import asyncio
import importlib
import logging
import uuid
import json
import uvloop
import types
from quantnet_mq import Code
from quantnet_mq.gmqtt.mqttclient import MQTTClient, PubRecReasonCode
from quantnet_mq.rpc import RPCHandler
from quantnet_mq.util import Constants
from quantnet_mq.schema.models import (
    rpcResponse,
    Status as responseStatus,
)


logger = logging.getLogger(__name__)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class RPCServer:
    def __init__(self, cid, model="quantnet_mq.schema.models", topic=Constants.DEFAULT_RPC_TOPIC, **kwargs):
        self._cid = cid or uuid.uuid4().hex
        self._topic = topic or Constants.DEFAULT_RPC_TOPIC
        self._rpc_handlers = {}
        self._model = model
        self._mqtt_client_username = kwargs.get("username", "")
        self._mqtt_client_password = kwargs.get("password", "")
        self._mqtt_broker_host = kwargs.get("host", "127.0.0.1")
        self._mqtt_broker_port = kwargs.get("port", 1883)
        self._mqttclient = None

    def _send_response(self, response, properties):
        """ send back response """
        if isinstance(response, dict):
            res = response
        else:
            res = response.serialize()
        self._mqttclient.publish(properties['response_topic'][0],
                                 res,
                                 correlation_data=properties['correlation_data'][0],
                                 qos=1,
                                 retain=False)
        logger.debug(f'Sent RPC response: {res}')

    def on_connect(self, client, flags, rc, properties):
        logger.info('Connected: %s', self._cid)
        self._mqttclient.subscribe(self._topic, 2)

    async def on_message(self, client, topic, payload, qos, properties):
        """ check message properties """
        if 'response_topic' not in properties.keys() or 'correlation_data' not in properties.keys():
            reason = "no response_topic or correlation_data found in the propeties"
            logger.warning(reason)
            return PubRecReasonCode.TOPIC_NAME_INVALID

        """ parse the message """
        try:
            rpcmsg = json.loads(payload)
            logger.debug(f"Received message: {rpcmsg}")
            if not isinstance(rpcmsg, dict):
                raise Exception('unknown format')
        except Exception as e:
            logger.error(f"Invalid Payload: {e}")
            rc = 6
            self._send_response(
                rpcResponse(
                    status=responseStatus(
                        code=rc,
                        value=Code(rc).name,
                        reason="Message decode error"),
                    reason="Message decode error"
                ),
                properties)
            return PubRecReasonCode.PAYLOAD_FORMAT_INVALID

        if 'cmd' not in rpcmsg.keys():
            rc = 6
            self._send_response(
                rpcResponse(
                    status=responseStatus(
                        code=rc,
                        value=Code(rc).name,
                        reason="Invalid RPC message format"),
                    reason='Invalid RPC message format'),
                properties)
            return PubRecReasonCode.PAYLOAD_FORMAT_INVALID

        cmd = rpcmsg['cmd']
        if cmd not in self._rpc_handlers.keys():
            rc = 6
            reason = f"cmd not defined: {cmd}"
            self._send_response(
                rpcResponse(
                    status=responseStatus(
                        code=rc,
                        value=Code(rc).name,
                        reason=reason),
                    reason=reason),
                properties)
            logger.warn(reason)
            return PubRecReasonCode.PAYLOAD_FORMAT_INVALID

        handler = self._rpc_handlers[cmd]
        try:
            module_name, class_name = handler.classpath.rsplit(".", 1)
            submodules = handler.classpath.replace(f"{self._model}.", "").split(".")
            model_module = importlib.import_module(self._model)
            for submodule in submodules[:-1]:
                model_module = getattr(model_module, submodule)
            MyClass = getattr(model_module, class_name)
            try:
                instance = MyClass.from_json(payload)
            except Exception:
                # Explicitly try each type in abc if coercion above fails
                from quantnet_mq.schema.loader import schemaLoader
                instance = schemaLoader.coerceRPC(module_name, MyClass, rpcmsg)
            if (handler):
                res = handler.handle(instance)
                if isinstance(res, types.CoroutineType):
                    res = await res
                if not res:
                    rc = 0
                    res = rpcResponse(status=responseStatus(code=rc, value=Code(rc).name))
                self._send_response(res, properties)
                return PubRecReasonCode.SUCCESS
        except Exception as e:
            rc = 6
            reason = f"Failed cmd {cmd}: {e}"
            self._send_response(
                rpcResponse(
                    status=responseStatus(
                        code=rc,
                        value=Code(rc).name,
                        reason=reason),
                    reason=reason),
                properties)
            logger.warn(reason)
            return PubRecReasonCode.IMPLEMENTATION_SPECIFIC_ERROR

    def on_disconnect(self, client, packet, exc=None):
        logger.info('Disconnected')

    def on_subscribe(self, client, mid, qos, properties):
        subs = next((sub for sub in client.subscriptions if sub.mid == mid), None)
        logger.info('Subscribed: %s', subs.topic)

    async def _start_mqttclient(self):
        """
        start the mqtt client
        """
        self._mqttclient = MQTTClient(f'rpcserver-{self._cid}')
        self._mqttclient.on_connect = self.on_connect
        self._mqttclient.on_message = self.on_message
        self._mqttclient.on_disconnect = self.on_disconnect
        self._mqttclient.on_subscribe = self.on_subscribe
        self._mqttclient.set_auth_credentials(self._mqtt_client_username,
                                              self._mqtt_client_password)
        await self._mqttclient.connect(host=self._mqtt_broker_host,
                                       port=self._mqtt_broker_port)
        #self._mqttclient.subscribe(self._topic, 2)

    def _stop_mqttclient(self):
        pass

    async def start(self):
        await self._start_mqttclient()

    async def stop(self):
        self._stop_mqttclient()

    @property
    def on_rpcmsg(self):
        return self._on_rpcmsg_callback

    @on_rpcmsg.setter
    def on_rpcmsg(self, cb):
        if not callable(cb):
            raise ValueError
        self._on_rpcmsg_callback = cb

    def subscribe(self, topic: str, qos: int = 2):
        """Subscribe to an additional MQTT topic on the existing client.

        This allows the same RPC server to handle messages arriving on
        multiple topic prefixes (e.g. both ``rpc/{cid}`` and ``link/{cid}``).
        Must be called after :meth:`start`.
        """
        if self._mqttclient is not None:
            self._mqttclient.subscribe(topic, qos)

    def set_handler(self, cmd: str, cb, classpath):
        self._rpc_handlers[cmd] = RPCHandler(cmd, cb, classpath)
