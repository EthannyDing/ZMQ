"""
Load-balancing broker

Clients and workers are shown here in-process.

Author: Brandon Carpenter (hashstat) <brandon(dot)carpenter(at)pnnl(dot)gov>
"""

from __future__ import print_function

import multiprocessing, subprocess, os, json
import zmq

NBR_CLIENTS = 20
NBR_WORKERS = 3

# def client_task(ident):
#     """Basic request-reply client using REQ socket."""
#     socket = zmq.Context().socket(zmq.REQ)
#     socket.identity = u"Client-{}".format(ident).encode("ascii")
#     socket.connect("tcp://192.168.0.15:4666")
#
#     # Send request, get reply
#     socket.send(b"HELLO")
#     reply = socket.recv()
#     print("{} received result.".format(socket.identity))
#     # print("\n{}: {}".format(socket.identity.decode("ascii"),
#     #                       reply.decode("ascii")))

def worker_task(ident):
    """Worker task, using a REQ socket to do load-balancing."""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Worker-{}".format(ident).encode("ascii")
    socket.connect("tcp://192.168.0.15:4555")

    # Tell broker we're ready for work
    socket.send(b"READY")

    while True:
        address, empty, request = socket.recv_multipart()
        cmd = "python tm_cleaner.py JOB.CONF {}".format(ident)
        proc = subprocess.Popen(cmd, shell=True)
        proc.wait()
        # print("\n{}: {}".format(socket.identity.decode("ascii"),
        #                       request.decode("ascii")))
        socket.send_multipart([address, b"", b"OK"])

def main():
    """Load balancer main loop."""
    # Prepare context and sockets
    context = zmq.Context.instance()
    frontend = context.socket(zmq.ROUTER)
    frontend.bind("tcp://*:4666")
    backend = context.socket(zmq.ROUTER)
    backend.bind("tcp://192.168.0.15:4555")

    # Start background tasks
    def start(task, *args):
        process = multiprocessing.Process(target=task, args=args)
        process.daemon = True
        process.start()
    # for i in range(NBR_CLIENTS):
    #     start(client_task, i)
    for i in range(NBR_WORKERS):
        start(worker_task, i)

    # Initialize main loop state
    count = NBR_CLIENTS
    workers = []
    poller = zmq.Poller()
    # Only poll for requests from backend until workers are available
    poller.register(backend, zmq.POLLIN)

    print("Server Ready!")
    while True:
        sockets = dict(poller.poll())
        # print("socket: {}".format(sockets))
        # print("backend {}".format(backend))
        # print("frontend {}".format(frontend))

        if backend in sockets:
            # Handle worker activity on the backend
            request = backend.recv_multipart()
            print("Backend: ", request)
            worker, empty, client = request[:3]
            #print(worker, empty, client)
            if not workers:
                # Poll for clients now that a worker is available
                # print('Worker is available.')
                poller.register(frontend, zmq.POLLIN)
            workers.append(worker)
            if client != b"READY" and len(request) > 3:
                # If client reply, send rest back to frontend
                empty, reply = request[3:]
                frontend.send_multipart([client, b"", reply])
                # count -= 1
                # if not count:
                #     break

        if frontend in sockets:
            # Get next client request, route to last-used worker
            client, empty, request = frontend.recv_multipart()
            print("frontend: ", client, empty, request)
            worker = workers.pop(0)
            backend.send_multipart([worker, b"", client, b"", request])
            if not workers:
                # Don't poll clients if no workers are available
                poller.unregister(frontend)

    # Clean up
    backend.close()
    frontend.close()
    context.term()

if __name__ == "__main__":
    main()