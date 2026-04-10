from abc import ABC, abstractmethod
from enum import IntEnum, StrEnum

__version__ = "1.0.0"


class MQTTClientInterface(ABC):

    # @abstractmethod
    # def publish(self, topic, msg, qos, retain):
    #     pass
    #
    # @abstractmethod
    # def subscribe(self, topic, qos):
    #     pass

    @abstractmethod
    def topic_match(self, sub, topic):
        """ TODO: check if topic start with sub """
        raise NotImplementedError

    @abstractmethod
    def topic_tokenise(self, topic):
        """ TODO: break the topic into token array """
        raise NotImplementedError


class Code(IntEnum):
    """
    Reference: schema/rpc/objects.yaml
    """
    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    NOT_FOUND = 4
    ALREADY_EXISTS = 5
    FAILED = 6
    QUEUED = 7


class EventType(StrEnum):
    """
    Reference: schema/messages/monitor.yaml
    """
    AGENT_STATE = "agentState"
    EXPERIMENT_RESULT = "experimentResult"
    AGENT_HEARTBEAT = "agentHeartbeat"
    AGENT_TASK_SCHEDULER_PHASE = "agentTaskSchedulerPhase"
    AGENT_TASK_SCHEDULER_TASK = "agentTaskSchedulerTask"
    AGENT_TASK_RESULT = "agentTaskResult"
