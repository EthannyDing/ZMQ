import zmq, os, json
import concurrent.futures
import multiprocessing

NBR_CLIENTS = 3

def client_task(ident):
    """Basic request-reply client using REQ socket."""
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("tcp://192.168.0.15:4666")

    # Send request, get reply
    socket.send_json(b"HELLO")
    reply = socket.recv()
    print("{} got response: {}".format(socket.identity.decode("ascii"),
                          reply.decode("ascii")))

    socket.close()
    context.term()

# def start(task, *args):
#     process = multiprocessing.Process(target=task, args=args)
#     #process.daemon = False
#     process.start()
#
# for i in range(NBR_CLIENTS):
#     start(client_task, i)
if __name__ == '__main__':

    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        executor.map(client_task, list(range(20)))


client_task(1)