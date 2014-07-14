using Base.Test
using ZMQ

println("Testing with ZMQ version $(ZMQ.version)")

ctx = Context(1)

@test typeof(ctx) == Context

ZMQ.close(ctx)

#try to create socket with expired context
@test_throws StateError Socket(ctx, PUB)

ctx2 = Context(1)
s = Socket(ctx2, PUB)
@test typeof(s) == Socket{PUB}
ZMQ.close(s)

s1 = Socket(ctx2, REP)
if ZMQ.version.major == 2 
	ZMQ.set_hwm(s1, 1000)
else 
	ZMQ.set_sndhwm(s1, 1000)
end
ZMQ.set_linger(s1, 1)
ZMQ.set_identity(s1, "abcd")

@test ZMQ.get_identity(s1)::String == "abcd"
if ZMQ.version.major == 2
	@test ZMQ.get_hwm(s1)::Integer == 1000
else
	@test ZMQ.get_sndhwm(s1)::Integer == 1000
end
@test ZMQ.get_linger(s1)::Integer == 1
@test ZMQ.ismore(s1) == false 

s2 = Socket(ctx2, REQ)
@test ZMQ.get_type(s1) == REP 
@test ZMQ.get_type(s2) == REQ 

ZMQ.bind(s1, "tcp://*:5555")
ZMQ.connect(s2, "tcp://localhost:5555")

ZMQ.send(s2, Message("test request"))
@test (bytestring(ZMQ.recv(s1)) == "test request")

ZMQ.send(s1, msg"test response")
@test (bytestring(ZMQ.recv(s2)) == "test response")

# Test task-blocking behavior
c = Base.Condition()
msg_sent = false
@async begin
	global msg_sent
	sleep(0.5)
	msg_sent = true
	ZMQ.send(s2, msg"test request")
	@test (bytestring(ZMQ.recv(s2)) == "test response")
	notify(c)
end

# This will hang forver if ZMQ blocks the entire process since 
# we'll never switch to the other task
@test (bytestring(ZMQ.recv(s1)) == "test request")
@test msg_sent == true
ZMQ.send(s1, msg"test response")
wait(c)

ZMQ.send(s2, msg"another test request")
msg = ZMQ.recv(s1)
o = convert(IOStream, msg); seek(o, 0)
@test (takebuf_string(o)=="another test request")

ZMQ.close(s1)
ZMQ.close(s2)
ZMQ.close(ctx2)
