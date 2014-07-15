# Support for ZeroMQ, a network and interprocess communication library
module ZMQ

include("../deps/deps.jl")

import Base: convert, get, bytestring, length, size, stride, show,
             similar, getindex, setindex!, fd, wait, close, connect

# Julia 0.2 does not define these (avoid warning for import)
isdefined(:bind) && import Base.bind
isdefined(:send) && import Base.send
isdefined(:recv) && import Base.recv

export bind, send, recv

export 
    # Types
    StateError, Context, Socket, Message,
    # Macros
    @msg_str, 
    # Functions
    set, subscribe, unsubscribe,
    # Constants
    IO_THREADS, MAX_SOCKETS, PAIR, PUB, SUB, REQ, REP, ROUTER, DEALER, PULL, PUSH,
    XPUB, XREQ, XREP, UPSTREAM, DOWNSTREAM, MORE, SNDMORE, POLLIN, POLLOUT, POLLERR,
    STREAMER, FORWARDER, QUEUE

# A server will report most errors to the client over a Socket, but
# errors in ZMQ state can't be reported because the socket may be
# corrupted. Therefore, we need an exception type for errors that
# should be reported locally.
type StateError <: Exception
    msg::String
end

show(io::IO, thiserr::StateError) = print(io, "ZMQ.StateError(\"$(thiserr.msg)\")")

# Basic functions
function jl_zmq_error_str()
    errno = ccall((:zmq_errno, zmq), Cint, ())
    errstr = ccall ((:zmq_strerror, zmq), Ptr{Uint8}, (Cint,), errno)
    return errstr != C_NULL ? bytestring(errstr) : "Unknown error"
end

macro check(expr::Expr)
    quote
        local ret = zero(Cint)
        ret = $(esc(expr))
        if ret != zero(Cint)
            throw(StateError(jl_zmq_error_str()))
        end
    end
end

const version = let major = major = Cint[0], minor = Cint[0], patch = Cint[0]
    ccall((:zmq_version, zmq), Void, (Ptr{Cint}, Ptr{Cint}, Ptr{Cint}), major, minor, patch)
    VersionNumber(major[1], minor[1], patch[1])
end

# define macro to enable version specific code
macro v2only(ex)
    version.major == 2 ? esc(ex) : :nothing
end

macro v3only(ex)
    version.major >= 3 ? esc(ex) : :nothing
end

## Sockets ##
type Socket{T}
    data::Ptr{Void}
    
    # ctx should be ::Context, but forward type references are not allowed
    Socket(ctx) = begin
        p = ccall((:zmq_socket, zmq), Ptr{Void}, (Ptr{Void}, Cint), ctx.data, T)
        p == C_NULL && throw(StateError(jl_zmq_error_str()))
        socket = new(p)
        finalizer(socket, close)
        push!(ctx.sockets, socket)
        return socket
    end
end
Socket(ctx, typ::Integer) = Socket{int(typ)}(ctx)

function close(socket::Socket)
    if socket.data != C_NULL
        rc = ccall((:zmq_close, zmq), Cint, (Ptr{Void},), socket.data)
        rc != zero(Cint) && throw(StateError(jl_zmq_error_str()))
        socket.data = C_NULL
    end
end

show(io::IO, s::Socket) = begin
    typ = _socket_type_map[get_type(s)]
    ptr_addr = "0x$(hex(unsigned(s.data), WORD_SIZE>>2))"
    print(io, "ZMQ.Socket{$typ}(@$ptr_addr)")
end

## Contexts ##
# Provide the same constructor API for version 2 and version 3, even
# though the underlying functions are changing
type Context
    data::Ptr{Void}

    # need to keep a list of sockets for this Context in order to
    # close them before finalizing (otherwise zmq_term will hang)
    sockets::Vector{Socket}

    Context(n::Integer) = begin
        @v2only p = ccall((:zmq_init, zmq), Ptr{Void}, (Cint,), n)
        @v3only p = ccall((:zmq_ctx_new, zmq), Ptr{Void}, ())
        p == C_NULL && throw(StateError(jl_zmq_error_str()))
        zctx = new(p, Socket[])
        finalizer(zctx, close)
        return zctx
    end
end
Context() = Context(1)

show(io::IO, ctx::Context) = begin
    ptr_addr = "0x$(hex(unsigned(ctx.data), WORD_SIZE>>2))"
    print(io, "ZMQ.Context(@$ptr_addr)")
end

function close(ctx::Context)
    if ctx.data != C_NULL # don't close twice!
        map(close, ctx.sockets)
        @v2only @check ccall((:zmq_term, zmq), Cint, (Ptr{Void},), ctx.data)
        @v3only @check ccall((:zmq_ctx_destroy, zmq), Cint, (Ptr{Void},), ctx.data)
        ctx.data = C_NULL
    end
end

with_ctx(f::Function, n::Integer) = begin
    ctx = ZMQ.Context(n)
    try
        f(ctx)
    finally
        close(ctx)
    end
end
with_ctx(f::Function) = with_ctx(f, 1)

term(ctx::Context) = close(ctx)

@v3only begin
    function get(ctx::Context, option::Integer)
        val = ccall((:zmq_ctx_get, zmq), Cint, (Ptr{Void}, Cint), ctx.data, option)
        val < zero(Cint) && throw(StateError(jl_zmq_error_str()))
        return val
    end

    function set(ctx::Context, option::Integer, value::Integer)
        @check ccall((:zmq_ctx_set, zmq), Cint, (Ptr{Void}, Cint, Cint), ctx.data, option, value)
    end
end 


# Getting and setting socket options
# Socket options of integer type
let u64p = Uint64[0], i64p = Int64[0], ip = Cint[0], u32p = Uint32[0], sz = Uint[0], pp = Ptr{Void}[0]
    opslist = {
        (:set_affinity,                :get_affinity,                 4, u64p)
        (:set_type,                    :get_type,                    16,   ip)
        (:set_linger,                  :get_linger,                  17,   ip)
        (:set_reconnect_ivl,           :get_reconnect_ivl,           18,   ip)
        (:set_backlog,                 :get_backlog,                 19,   ip)
        (:set_reconnect_ivl_max,       :get_reconnect_ivl_max,       21,   ip)
      }

    @unix_only    opslist = vcat(opslist, (nothing,  :get_fd,        14,   ip))
    @windows_only opslist = vcat(opslist, (nothing,  :get_fd,        14,   pp))

    if version.major == 2
        opslist = vcat(opslist, {
            (:set_hwm,                     :get_hwm,                      1, u64p)
            (:set_swap,                    :get_swap,                     3, i64p)
            (:set_rate,                    :get_rate,                     8, i64p)
            (:set_recovery_ivl,            :get_recovery_ivl,             9, i64p)
            (:_zmq_setsockopt_mcast_loop,  :_zmq_getsockopt_mcast_loop,  10, i64p)
            (:set_sndbuf,                  :get_sndbuf,                  11, u64p)
            (:set_rcvbuf,                  :get_rcvbuf,                  12, u64p)
            (nothing,                      :_zmq_getsockopt_rcvmore,     13, i64p)
            (nothing,                      :get_events,                  15, u32p)
            (:set_recovery_ivl_msec,       :get_recovery_ivl_msec,       20, i64p)
        })
    else
        opslist = vcat(opslist, {
            (:set_rate,                    :get_rate,                     8,   ip)
            (:set_recovery_ivl,            :get_recovery_ivl,             9,   ip)
            (:set_sndbuf,                  :get_sndbuf,                  11,   ip)
            (:set_rcvbuf,                  :get_rcvbuf,                  12,   ip)
            (nothing,                      :_zmq_getsockopt_rcvmore,     13,   ip)
            (nothing,                      :get_events,                  15,   ip)
            (:set_maxmsgsize,              :get_maxmsgsize,              22,   ip)
            (:set_sndhwm,                  :get_sndhwm,                  23,   ip)
            (:set_rcvhwm,                  :get_rcvhwm,                  24,   ip)
            (:set_multicast_hops,          :get_multicast_hops,          25,   ip)
            (:set_ipv4only,                :get_ipv4only,                31,   ip)
            (:set_tcp_keepalive,           :get_tcp_keepalive,           34,   ip)
            (:set_tcp_keepalive_idle,      :get_tcp_keepalive_idle,      35,   ip)
            (:set_tcp_keepalive_cnt,       :get_tcp_keepalive_cnt,       36,   ip)
            (:set_tcp_keepalive_intvl,     :get_tcp_keepalive_intvl,     37,   ip)
        })
    end

    if version > v"2.1"
        opslist = vcat(opslist, {
            (:set_rcvtimeo,                :get_rcvtimeo,                27,   ip)
            (:set_sndtimeo,                :get_sndtimeo,                28,   ip)
        })
    end
    
    for (fset, fget, k, p) in opslist
        if fset != nothing
            @eval global $fset
            @eval function ($fset)(socket::Socket, option_val::Integer)
                ($p)[1] = option_val
                @check ccall((:zmq_setsockopt, zmq), Cint,
                             (Ptr{Void}, Cint, Ptr{Void}, Uint),
                             socket.data, $k, $p, sizeof(eltype($p)))
            end
        end

        if fget != nothing
            @eval global $fget
            @eval function ($fget)(socket::Socket)
                ($sz)[1] = sizeof(eltype($p))
                @check ccall((:zmq_getsockopt, zmq), Cint,
                             (Ptr{Void}, Cint, Ptr{Void}, Ptr{Uint}),
                             socket.data, $k, $p, $sz)
                return int(($p)[1])
            end
        end        
    end
    
    # For some functions, the publicly-visible versions should require & return boolean
    if version.major == 2
        global set_mcast_loop
        set_mcast_loop(socket::Socket, val::Bool) = _zmq_setsockopt_mcast_loop(socket, val)
        
        global get_mcast_loop
        get_mcast_loop(socket::Socket) = bool(_zmq_getsockopt_mcast_loop(socket))
    end
end

# More functions with boolean prototypes
get_rcvmore(socket::Socket) = bool(_zmq_getsockopt_rcvmore(socket))

# And a convenience function
ismore(socket::Socket) = get_rcvmore(socket)

# subscribe/unsubscribe options take an arbitrary byte array
for (f,k) in ((:subscribe, 6), (:unsubscribe, 7))
    f_ = symbol(string(f, "_"))
    @eval begin
        function $f_{T}(socket::Socket, filter::Ptr{T}, len::Integer)
            @check ccall((:zmq_setsockopt, zmq), Cint,
                         (Ptr{Void}, Cint, Ptr{T}, Uint),
                         socket.data, $k, filter, len)
        end
        $f(socket::Socket, filter::Array)  = $f_(socket, pointer(filter), sizeof(filter))
        $f(socket::Socket, filter::String) = $f_(socket, pointer(filter), sizeof(filter))
        $f(socket::Socket) = $f_(socket, C_NULL, 0)
    end
end

# Raw FD access
@unix_only begin
    fd(socket::Socket) = RawFD(get_fd(socket))
end
@windows_only begin
    fd(socket::Socket) = WindowsRawSocket(convert(Ptr{Void}, get_fd(socket)))
end

wait(socket::Socket; readable=false, writable=false) = 
    wait(fd(socket); readable=readable, writable=writable)

# Socket options of string type
let u8ap = zeros(Uint8, 255), sz = Uint[0]
    opslist = {
        (:set_identity,     :get_identity, 5)
        (:set_subscribe,    nothing,       6)
        (:set_unsubscribe,  nothing,       7)
    }
    
    if version.major >= 3
        opslist = vcat(opslist, {
            (nothing,                :get_last_endpoint, 32)
            (:set_tcp_accept_filter, nothing,            38)
        })
    end

    for (fset, fget, k) in opslist
        if fset != nothing
            @eval global $fset
            @eval function ($fset)(socket::Socket, option_val::ByteString)
                length(option_val) > 255 && throw(StateError("option value too large"))
                @check ccall((:zmq_setsockopt, zmq), Cint,
                             (Ptr{Void}, Cint, Ptr{Uint8}, Uint),
                             socket.data, $k, option_val, length(option_val))
            end      
        end

        if fget != nothing
            @eval global $fget
            @eval function ($fget)(socket::Socket)
                ($sz)[1] = length($u8ap)
                @check ccall((:zmq_getsockopt, zmq), Cint,
                             (Ptr{Void}, Cint, Ptr{Uint8}, Ptr{Uint}),
                             socket.data, $k, $u8ap, $sz)
                return bytestring(convert(Ptr{Uint8}, $u8ap), int(($sz)[1]))
            end
        end        
    end
end
    
bind(socket::Socket, endpoint::String) = 
    @check ccall((:zmq_bind, zmq), Cint, (Ptr{Void}, Ptr{Uint8}), socket.data, endpoint)

connect(socket::Socket, endpoint::String) = 
    @check ccall((:zmq_connect, zmq), Cint, (Ptr{Void}, Ptr{Uint8}), socket.data, endpoint)

# in order to support zero-copy messages that share data with Julia
# arrays, we need to hold a reference to the Julia object in a dictionary
# until zeromq is done with the data, to prevent it from being garbage
# collected.  The gc_protect dictionary is keyed by a uv_async_t* pointer,
# used in uv_async_send to tell Julia to when zeromq is done with the data.
const gc_protect = Dict{Ptr{Void},Any}()

# 0.2 compatibility
gc_protect_cb(work) = pop!(gc_protect, work.handle, nothing)
gc_protect_cb(work, status) = gc_protect_cb(work)

function gc_protect_handle(obj::Any)
    work = Base.SingleAsyncWork(gc_protect_cb)
    gc_protect[work.handle] = (work,obj)
    work.handle
end

# Thread-safe zeromq callback when data is freed, passed to zmq_msg_init_data.
# The hint parameter will be a uv_async_t* pointer.
gc_free_fn(data::Ptr{Void}, hint::Ptr{Void}) = ccall(:uv_async_send, Cint, (Ptr{Void},), hint)

const gc_free_fn_c = cfunction(gc_free_fn, Cint, (Ptr{Void}, Ptr{Void}))

## Messages ##
type Message <: AbstractArray{Uint8,1}
    # 32 bytes (for v3) + a pointer (for v2)
    w0::Int64
    w1::Int64
    w2::Int64
    w3::Int64
    w4::Int
    handle::Ptr{Void} # index into gc_protect, if any

    # Create an empty message (for receive)
    Message() = begin
        zmsg = new()
        zmsg.handle = C_NULL
        @check ccall((:zmq_msg_init, zmq), Cint, (Ptr{Message},), &zmsg)
        finalizer(zmsg, close)
        return zmsg
    end

    # Create a message with a given buffer size (for send)
    Message(len::Integer) = begin
        zmsg = new()
        zmsg.handle = C_NULL
        rc = ccall((:zmq_msg_init_size, zmq), Cint, (Ptr{Message}, Csize_t), &zmsg, len)
        rc != zero(Cint) && throw(StateError(jl_zmq_error_str()))
        finalizer(zmsg, close)
        return zmsg
    end

    # low-level function to create a message (for send) with an existing
    # data buffer, without making a copy.  The origin parameter should
    # be the Julia object that is the origin of the data, so that
    # we can hold a reference to it until zeromq is done with the buffer.
    Message{T}(origin::Any, m::Ptr{T}, len::Integer) = begin
        zmsg = new()
        zmsg.handle = gc_protect_handle(origin)
        @check ccall((:zmq_msg_init_data, zmq), Cint, 
                     (Ptr{Message}, Ptr{T}, Csize_t, Ptr{Void}, Ptr{Void}), 
                     &zmsg, m, len, gc_free_fn_c, zmsg.handle)
        finalizer(zmsg, close)
        return zmsg
    end

    # Create a message with a given String or Array as a buffer (for send)
    # (note: now "owns" the buffer ... the Array must not be resized,
    #        or even written to after the message is sent!)
    Message(m::ByteString) = 
        Message(m, convert(Ptr{Uint8}, m), sizeof(m))
    
    Message{T<:ByteString}(p::SubString{T}) = 
        Message(p, convert(Ptr{Uint8}, p.string.data) + p.offset, sizeof(p))
    
    Message(a::Array) = 
        Message(a, pointer(a), sizeof(a))
    
    Message(io::IOBuffer) = begin
        if !io.readable || !io.seekable
            error("byte read failed")
        end
        Message(io.data)
    end
end

macro msg_str(str::String)
    Message(str)
end

# check whether zeromq has called our free-function, i.e. whether
# we are save to reclaim ownership of any buffer object
isfreed(m::Message) = haskey(gc_protect, m.handle)

# AbstractArray behaviors:
similar(a::Message, T, dims::Dims) = Array(T, dims) # ?

length(zmsg::Message) = 
    int(ccall((:zmq_msg_size, zmq), Csize_t, (Ptr{Message},), &zmsg))

size(zmsg::Message) = (length(zmsg),)

convert(::Type{Ptr{Uint8}}, zmsg::Message) = 
    ccall((:zmq_msg_data, zmq), Ptr{Uint8}, (Ptr{Message},), &zmsg)

function getindex(a::Message, i::Integer)
    (i < 1 || i > length(a)) && throw(BoundsError())
    return unsafe_load(pointer(a), i)
end

function setindex!(a::Message, v, i::Integer)
    (i < 1 || i > length(a)) && throw(BoundsError())
    unsafe_store(pointer(a), v, i)
end

# Convert message to string (copies data)
bytestring(zmsg::Message) = bytestring(pointer(zmsg), length(zmsg))

# Build an IOStream from a message
# Copies the data
function convert(::Type{IOStream}, zmsg::Message)
    s = IOBuffer()
    write(s, zmsg)
    return s
end

# Close a message. You should not need to call this manually (let the
# finalizer do it).
close(zmsg::Message) = 
    @check ccall((:zmq_msg_close, zmq), Cint, (Ptr{Message},), &zmsg)

@v3only begin
    function get(zmsg::Message, property::Integer)
        val = ccall((:zmq_msg_get, zmq), Cint, (Ptr{Message}, Cint), &zmsg, property)
        val < zero(Cint) && throw(StateError(jl_zmq_error_str()))
        return val
    end

    function set(zmsg::Message, property::Integer, value::Integer)
        @check ccall((:zmq_msg_set, zmq), Cint, (Ptr{Message}, Cint, Cint), &zmsg, property, value)
    end
end 

## Send/receive messages
#
# Julia defines two types of ZMQ messages: "raw" and "serialized". A "raw"
# message is just a plain ZeroMQ message, used for sending a sequence
# of bytes. You send these with the following:
#   send(socket, zmsg)
#   zmsg = recv(socket)

#Send/Recv Options
const NOBLOCK  = 1 # deprecated old name for DONTWAIT in ZMQ v2
const DONTWAIT = 1
const SNDMORE  = 2

@v2only begin
    function send(socket::Socket, zmsg::Message, flag=zero(Int32))
        @check ccall((:zmq_send, zmq), Cint, (Ptr{Void}, Ptr{Message}, Cint),
                     socket.data, &zmsg, flag)
    end
end 
@v3only begin
    function send(socket::Socket, zmsg::Message, flag=int32(0))
        if (get_events(socket) & POLLOUT) == 0
            wait(socket; writable = true)
        end
        rc = ccall((:zmq_msg_send, zmq), Cint, (Ptr{Void}, Ptr{Message}, Cint),
                    &zmsg, socket.data, flag)
        if rc == int32(-1)
            throw(StateError(jl_zmq_error_str()))
        end
    end
end 

# strings are immutable, so we can send them zero-copy by default
send(socket::Socket, msg::String, flag=int32(0)) = send(socket, Message(msg), flag)

# Make a copy of arrays before sending, by default, since it is too
# dangerous to require that the array not change until ZMQ is done with it.
# For zero-copy array messages, construct a Message explicitly.
send(socket::Socket, msg::AbstractArray, flag=int32(0)) = send(socket, Message(copy(msg)), flag)

function send(f::Function, socket::Socket, flag=int32(0))
    io = IOBuffer()
    f(io)
    send(socket, Message(io), flag)
end

@v2only begin
    function recv(socket::Socket)
        zmsg = Message()
        while true
            rc = ccall((:zmq_recv, zmq), Cint, (Ptr{Void}, Ptr{Message},  Cint),
                        socket.data, &zmsg, NOBLOCK)
            if rc != zero(Cint)
                if errno() == Base.EAGAIN
                    while (get_events(socket) & POLLIN) == 0
                        wait(socket; readable = true)
                    end
                    continue
                end 
                throw(StateError(jl_zmq_error_str()))
            end
            break
        end
        return zmsg
    end
end 
@v3only begin
    function recv(socket::Socket)
        zmsg = Message()
        while true
            rc = ccall((:zmq_msg_recv, zmq), Cint, (Ptr{Message}, Ptr{Void}, Cint),
                        &zmsg, socket.data, NOBLOCK)
            if rc == int32(-1)
                if errno() == Base.EAGAIN
                    while (get_events(socket) & POLLIN) == 0
                        wait(socket; readable = true)
                    end
                    continue
                end 
                throw(StateError(jl_zmq_error_str()))
            end
            break
        end
        return zmsg
    end
end 

## Constants

# Context options
const IO_THREADS = 1
const MAX_SOCKETS = 2

#Socket Types
const PAIR = 0
const PUB = 1
const SUB = 2
const REQ = 3
const REP = 4
const DEALER = 5
const ROUTER = 6
const PULL = 7
const PUSH = 8
const XPUB = 9
const XSUB = 10
const XREQ = DEALER        
const XREP = ROUTER        
const UPSTREAM = PULL      
const DOWNSTREAM = PUSH    

const _socket_type_map = [PAIR   => :PAIR,
                          PUB    => :PUB,
                          SUB    => :SUB,
                          REQ    => :REQ,
                          REP    => :REP,
                          DEALER => :DEALER,
                          ROUTER => :ROUTER,
                          PULL   => :PULL,
                          PUSH   => :PUSH, 
                          XPUB   => :XPUB,
                          XSUB   => :XSUB]
#Message options
const MORE = 1

#IO Multiplexing
const POLLIN = 1
const POLLOUT = 2
const POLLERR = 4

#Built in devices
const STREAMER = 1
const FORWARDER = 2
const QUEUE = 3

end
