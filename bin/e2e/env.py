from enum import Enum

MITMPROXY_URL = 'http://localhost:9200'
Action = Enum('Action', ['START', 'STOP', 'SAVE', 'CLEAN'])
PATH_PREFIX = '/___quesma_e2e_recorder'


def get_url(action: Action) -> str:
    return f"{MITMPROXY_URL}{PATH_PREFIX}_{action.name.lower()}"