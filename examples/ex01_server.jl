import ZMQ

# Hello world server

ZMQ.with_ctx() do ctx
    responder = ZMQ.Socket(ctx, ZMQ.REP)
    bind(responder, "tcp://*:5555")
    while true
        buffer = ZMQ.recv(responder)
        println("Received Hello")
        sleep(1)
        ZMQ.send(responder, "World")
    end
end
