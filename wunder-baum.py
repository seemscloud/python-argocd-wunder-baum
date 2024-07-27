import requests
import json
import time
from queue import Queue
from threading import Event, Thread
import sys
import logging
import os

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
TLS_VERIFY = False

AROCD_URL = os.getenv('AROCD_URL')
AROCD_TOKEN = os.getenv('AROCD_TOKEN')
GLOBAL_TIMEOUT = int(os.getenv('GLOBAL_TIMEOUT'))
MAX_ACTIVE_THREADS = int(os.getenv('MAX_ACTIVE_THREADS'))

DEFAULT_HEADERS = {
    "Authorization": "Bearer {}".format(AROCD_TOKEN),
    "Content-Type": "application/json"
}

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)


class ArgoApplication:
    def __init__(self, namespace, name):
        self.namespace = namespace
        self.name = name


def get_request(url, headers=None):
    try:
        response = requests.get(url, headers=headers, verify=TLS_VERIFY)
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred: {e}")
        return None


def get_applications():
    url = "{}/{}".format(AROCD_URL, "api/v1/applications")
    headers = DEFAULT_HEADERS

    return get_request(url=url, headers=headers)


def refresh_application(namespace, name):
    url = "{}/{}/{}?refresh=normal&appNamespace={}".format(AROCD_URL, "api/v1/applications", name, namespace)
    headers = DEFAULT_HEADERS
    response = get_request(url=url, headers=headers)

    if response is not None:
        logger.info("{}/{} -> '{}'".format(namespace, name, response.status_code))


queue = Queue()
processing_timeout = Event()
to_process = []


def processing_worker(timeout):
    try:
        while True:
            for i, obj in enumerate(to_process):
                if not obj.is_alive():
                    del to_process[i]

            if timeout.is_set():
                break

            if len(to_process) < MAX_ACTIVE_THREADS and queue.qsize() > 0:
                app = queue.get()
                thread = Thread(target=refresh_application, args=(app.namespace, app.name))
                thread.start()
                to_process.append(thread)

            time.sleep(0.01)
    except Exception as e:
        logger.error(e)


def processing_monitor(timeout):
    for i in range(1, GLOBAL_TIMEOUT):
        time.sleep(2)
        logger.warning("Timeout in: '{}', left in queue: '{}', active threads: '{}'".format(
            GLOBAL_TIMEOUT - (i * 2), queue.qsize(), len(to_process)))

        if queue.qsize() == 0 and len(to_process) == 0:
            logger.info("Processing finished")
            break

    timeout.set()


def main():
    json_data = json.loads(get_applications().text)

    for i in json_data['items']:
        queue.put(ArgoApplication(i['metadata']['namespace'], i['metadata']['name']))

    process = Thread(target=processing_worker, args=(processing_timeout,))
    process.start()

    monitor = Thread(target=processing_monitor, args=(processing_timeout,))
    monitor.start()
    monitor.join()


if __name__ == "__main__":
    main()
