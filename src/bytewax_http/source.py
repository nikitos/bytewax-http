import queue
import time
import threading

from bytewax.inputs import batch, batch_async, DynamicSource, StatelessSourcePartition
from cloudevents.core.bindings.http import from_http_event, HTTPMessage
from flask import Flask, jsonify, request
from waitress import serve

from .utils import BatchFormat


data_queue = queue.SimpleQueue()


def http_listener(host: str, port: int, path: str):
    app = Flask(__name__)
    @app.route(path, methods=['POST'])
    def receive_data():
        for data in request.get_json():
            m = HTTPMessage(request.headers, data)
            msg = from_http_event(m, BatchFormat())
            data_queue.put(msg)

        return jsonify({'status': 'received'}), 202

    serve(app, host=host, port=port)


class HTTPSourcePartition(StatelessSourcePartition):
    def __init__(self, host: str, port: int, path: str):
        self.proc = threading.Thread(target=http_listener, args=(host, port, path), daemon=True)
        self.proc.start()

    def next_batch(self):
        try:
            msg = data_queue.get_nowait()
            return [msg]
        except queue.Empty:
            return []

    def close(self) -> None:
        self.proc.stop()


class HTTPSource(DynamicSource):
    def __init__(self, host: str = '0.0.0.0', port: int = 8080, path: str = '/'):
        self.host = host
        self.port = port
        self.path = path

    def build(self, *args) -> HTTPSourcePartition:
        return HTTPSourcePartition(self.host, self.port, self.path)
