import ZMQ

#=
Task worker
Connects PULL socket to tcp://locahost:5557
Collects workloads from ventilator via that socket
Connects PUSH socket to tcp://localhost:5558
sends results to sink via that socket
=#

ZMQ.with_ctx() do ctx
    # socket to recv msg on
    receiver = ZMQ.Socket(ctx, ZMQ.PULL)
    ZMQ.connect(receiver, "tcp://localhost:5557")

    # socket to send messages to
    sender = ZMQ.Socket(ctx, ZMQ.PUSH)
    ZMQ.connect(sender, "tcp://localhost:5558")

    # process tasks forever
    while true
        str = bytestring(ZMQ.recv(receiver))
        println("$str.")        # show progress
        sleep(0.01 * int(str))  # do the work
        ZMQ.send(sender, "")    # send results to sink
    end 
end
