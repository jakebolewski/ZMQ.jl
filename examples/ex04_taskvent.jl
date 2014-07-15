import ZMQ

#=
Task ventilator
Binds PUSH socket to tcp://localhost:5557
Sends batc of tasks to workers via that socket
=#

ZMQ.with_ctx() do ctx
    # socket to send messages on
    sender = ZMQ.Socket(ctx, ZMQ.PUSH)
    ZMQ.bind(sender, "tcp://*:5557")

    # socket to send start of batch message on
    sink = ZMQ.Socket(ctx, ZMQ.PUSH)
    ZMQ.connect(sink, "tcp://localhost:5558")

    println("Press enter when workers are ready...")
    while true
        res = readbytes(STDIN, 1)
        res == 0x0a || break
    end
    println("sending tasks to workers")

    # the first message is 0 and signals start of the batch
    ZMQ.send(sink, "0")

    # total expected cost in msec
    total_msec = 0 
    
    for i=1:100
        workload = rand(1:100)
        total_msec += workload
        ZMQ.send(sender, string(workload))
    end
    
    println("Total expected cost: $total_msec")
end
