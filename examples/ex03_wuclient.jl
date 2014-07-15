import ZMQ

#=
Weather update client
Connects SUB socket to tcp://localhost:5556
Collects weather updates and finds avg temp in zipcode
=#

info("Collecting updates from weather server...")

const NSAMPLES = 10

ZMQ.with_ctx() do ctx
    sock = ZMQ.Socket(ctx, ZMQ.SUB)
    ZMQ.connect(sock, "tcp://localhost:5556")
    
    # subscribe to zipcode
    zip_filter = length(ARGS) > 1 ? string(ARGS[1]) : "13152"

    ZMQ.subscribe(sock, zip_filter)
    
    total_temp = 0
    for i=1:NSAMPLES
        res = bytestring(ZMQ.recv(sock))
        zipcode, temp, humid = split(res)
        total_temp += int(temp)
        info("Update $i:$NSAMPLES $temp") 
    end

    info("Average temperature for zipcode $zip_filter was $(total_temp / NSAMPLES)")
end
