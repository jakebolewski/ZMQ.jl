import ZMQ

#= 
Task Sink
Binds PULL socket to tcp://localhost:5558
Collects results from workers via that socket
=#

# Prepare our ctx and socket
ZMQ.with_ctx() do ctx

    receiver = ZMQ.Socket(ctx, ZMQ.PULL)
    ZMQ.bind(receiver, "tcp://*:5558")

    # wait for start of batch
    ZMQ.recv(receiver)

    # start our clock now
    tic()
    # process 100 confirmations
    for i=1:100
        ZMQ.recv(receiver)
        div(i, 10) * 10 == i ? print(":") : print(".")
    end
    println()
    # report duration of the batch
    toc()
end
