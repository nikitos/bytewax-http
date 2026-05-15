import logging
import queue
import threading

from bytewax.inputs import DynamicSource, StatelessSourcePartition
from cloudevents.core.bindings.http import from_http_event, HTTPMessage
from flask import Flask, jsonify, request
from waitress import serve
from whitenoise import WhiteNoise

from .utils import BatchFormat


data_queue = queue.SimpleQueue()


def http_listener(host: str, port: int, path: str):
    app = Flask(__name__)

    @app.route(path, methods=['POST'])
    def receive_data():
        for data in request.get_json():
            try:
                m = HTTPMessage(request.headers, data)
                msg = from_http_event(m, BatchFormat())
                data_queue.put(msg)
            except Exception as e:
                logging.info(f'Failed to parse CloudEvent: {e}')

        return jsonify({'status': 'received'}), 202

    app = WhiteNoise(app)
    app.add_files('/static', prefix='static/')
    serve(app, host=host, port=port)


class HTTPSourcePartition(StatelessSourcePartition):
    def __init__(self, host: str, port: int, path: str):
        self.proc = threading.Thread(target=http_listener, args=(host, port, path), daemon=True)
        self.proc.start()
        logging.info(f'Http server started {host}:{port}{path}')

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
