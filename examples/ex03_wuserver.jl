import ZMQ

#=
Weather update server
Binds PUB socket to tcp://*:5556
Publishes random weather updates
=#

info("Starting up weather server...")

ZMQ.with_ctx() do ctx
    sock = ZMQ.Socket(ctx, ZMQ.PUB)
    ZMQ.bind(sock, "tcp://*:5556")

    while true
        zipcode = rand(1:100000)
        temp    = rand(-80:135)
        humid   = rand(10:60)
        ZMQ.send(sock, "$zipcode $temp $humid")
    end 
end
