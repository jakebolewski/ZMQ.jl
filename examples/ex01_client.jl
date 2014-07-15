import ZMQ

# Hello World Client

const NREQ = 10

println("Connecting to the hello world server")

ZMQ.with_ctx() do ctx
    requester = ZMQ.Socket(ctx, ZMQ.REQ)
    connect(requester, "tcp://localhost:5555")
    for i=1:NREQ
        println("Sending Hello $i...")
        ZMQ.send(requester, "Hello")
        buff = ZMQ.recv(requester)
        println("Received World $i")
    end
end
