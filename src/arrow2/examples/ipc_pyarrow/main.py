import pyarrow as pa
from time import sleep
import socket

# Set up the data exchange socket
sk = socket.socket()
sk.bind(("127.0.0.1", 12989))
sk.listen()

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(["foo", "bar", "baz", None]),
    pa.array([True, None, False, True]),
]

batch = pa.record_batch(data, names=["f0", "f1", "f2"])

# Accept incoming connection and stream the data away
connection, address = sk.accept()
dummy_socket_file = connection.makefile("wb")
with pa.RecordBatchStreamWriter(dummy_socket_file, batch.schema) as writer:
    for i in range(50):
        writer.write_batch(batch)
        sleep(1)
