import asyncio
import importlib
import logging
import uuid
import json
import uvloop
import queue
import threading
import time
from datetime import datetime
from quantnet_mq.gmqtt.mqttclient import MQTTClient, PubRecReasonCode
from quantnet_mq.rpc import RPCHandler
from quantnet_mq.util import Constants

logger = logging.getLogger(__name__)


class RPCClient:
    """ This is the class that works as the client in the RPC communication.
    It sends to messages to the remote RPC server and received the response.

    Parameters
    ----------
    cid: str
        Client ID
    topic: str
        Topic of RPC

    """

    def __init__(self, cid, topic=Constants.DEFAULT_RPC_TOPIC, **kwargs):
        self._cid = cid or uuid.uuid4().hex
        self._topic = topic
        self._queue = "rpc-res/" + self._cid
        self._mqtt_client_username = kwargs.get("username", "")
        self._mqtt_client_password = kwargs.get("password", "")
        self._mqtt_broker_host = kwargs.get("host", "127.0.0.1")
        self._mqtt_broker_port = kwargs.get("port", 1883)
        self._mqttclient = None
        self._rpc_handlers = dict()
        self._subscriptions = dict()
        self._sent_requests = dict()

    @property
    def cid(self):
        return self._cid

    def on_connect(self, client, flags, rc, properties):
        logger.info("Connected: %s", self._cid)

    async def on_message(self, client, topic, payload, qos, properties):
        """ Handle received messages
        """

        jsonString = json.dumps(json.loads(payload), indent=4, sort_keys=False)
        logger.debug("RECV MSG: %s", jsonString)
        # find the correlation id
        corrid = properties["correlation_data"][0].decode("utf-8")
        # set value of body
        body = payload

        # handle requests...
        try:
            if not self._sent_requests:
                return
            fut = self._sent_requests.pop(corrid)
        except Exception as e:
            return

        if fut is not None:
            if not fut.done():
                fut.set_result(body)
            else:
                logger.debug("Received late response for already-completed future (corrid=%s), discarding", corrid)

        return PubRecReasonCode.SUCCESS

    def on_disconnect(self, client, packet, exc=None):
        logger.info("Disconnected")

    def on_subscribe(self, client, mid, qos, properties):
        logger.info("Subscribed")

    async def _start_mqttclient(self):
        """
        start the mqtt client
        """
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self._mqttclient = MQTTClient(f"rpcclient-{self._cid}")
        self._mqttclient.on_connect = self.on_connect
        self._mqttclient.on_message = self.on_message
        self._mqttclient.on_disconnect = self.on_disconnect
        self._mqttclient.on_subscribe = self.on_subscribe
        self._mqttclient.set_auth_credentials(self._mqtt_client_username, self._mqtt_client_password)
        await self._mqttclient.connect(host=self._mqtt_broker_host, port=self._mqtt_broker_port)
        self._mqttclient.subscribe(self._queue, 2)
        self._add_subscription(self._queue, 2)

    def _add_subscription(self, queue, qos):
        self._subscriptions[queue] = qos

    async def _stop_mqttclient(self):
        if self._mqttclient:
            try:
                await asyncio.wait_for(self._mqttclient.disconnect(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning("MQTT disconnect timed out after 3s, forcing shutdown")
            except Exception as e:
                logger.warning("MQTT disconnect error: %s", e)
            logger.info("Stopped MQTT client")

    async def call(self, target, msg, timeout=5.0, verbose=None, topic=None, model="quantnet_mq.schema.models", sync=True):
        if topic is None:
            topic = self._topic
        if target not in self._rpc_handlers.keys():
            logging.error(f"Unknown RPC target: {target}")
            raise Exception(f"RPC message target not defined: {target}")
        handler = self._rpc_handlers[target]
        module_name, class_name = handler.classpath.rsplit(".", 1)
        submodules = handler.classpath.replace(f"{model}.", "").split(".")
        model_module = importlib.import_module(model)
        for submodule in submodules[:-1]:
            model_module = getattr(model_module, submodule)
        RPCClass = getattr(model_module, class_name)
        try:
            obj = RPCClass(cmd=target, agentId=self._cid, payload=msg)
        except Exception:
            # logger.error(f"{e}")
            # Explicitly try each type in abc if coercion above fails
            from quantnet_mq.schema.loader import schemaLoader
            rmsg = {"cmd": target, "agentId": self._cid, "payload": msg}
            obj = schemaLoader.coerceRPC(module_name, RPCClass, rmsg)
        corrid = uuid.uuid4().hex
        self._mqttclient.publish(
            topic,
            obj.serialize(),
            correlation_data=corrid.encode("utf-8"),
            response_topic=self._queue,
            qos=1,
            retain=False,
        )

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._sent_requests[corrid] = fut

        if sync:
            try:
                await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.exceptions.TimeoutError:
                logger.error("Timeout awaiting RPC response")
                raise TimeoutError("Timeout awaiting RPC response")
            body = fut.result()
            return body
        else:
            task = None
            try:
                task = asyncio.create_task(self.resp_task(handler, fut))
            except Exception as e:
                logger.error(f"resp_task can not create: {e}")
                raise e
            return task

    async def start(self):
        await self._start_mqttclient()

    async def stop(self):
        await self._stop_mqttclient()

    def set_handler(self, cmd: str, cb, classpath):
        self._rpc_handlers[cmd] = RPCHandler(cmd, cb, classpath)

    async def resp_task(self, handler, fut, timeout=5.0):
        """ Task handling response messages
        """

        try:
            await asyncio.wait_for(fut, timeout=timeout)
            cb_func = handler.cb
            body = fut.result()
            await cb_func(body)
        except asyncio.TimeoutError:
            logger.info(f"resp_task was terminated due to a {timeout} second timeout!")
        except Exception as e:
            logger.info(e)

        logger.info("resp_task was terminated successfully.")
