extern crate bincode;
extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;
extern crate serde;
extern crate tokio_core;
#[macro_use]
extern crate tokio_io;

pub mod codec {
    use bincode::{self, Bounded, deserialize, serialize_into, serialized_size};
    use bytes::{BufMut, BytesMut, LittleEndian, ByteOrder};
    use serde::de::DeserializeOwned;
    use serde::ser::Serialize;
    use std::fmt::Debug;
    use std::io;
    use std::marker::PhantomData;

    pub trait Codec {
        /// The type of items to be encoded into byte buffer
        type In;

        /// The type of items to be returned by decoding from byte buffer
        type Out;

        /// Attempts to decode a frame from the provided buffer of bytes.
        fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Out>>;

        /// A default method available to be called when there are no more bytes
        /// available to be read from the I/O.
        fn decode_eof(&mut self, buf: &mut BytesMut) -> io::Result<Self::Out> {
            match try!(self.decode(buf)) {
                Some(frame) => Ok(frame),
                None => Err(io::Error::new(
                    io::ErrorKind::Other,
                    "bytes remaining on stream",
                )),
            }
        }

        /// Encodes a frame inot the buffer provided.
        fn encode(&mut self, msg: Self::In, buf: &mut BytesMut) -> io::Result<()>;
    }

    /// Codec based upon bincode serialization
    pub struct LengthDelimitedCodec<In, Out> {
        state: State,
        __in: PhantomData<In>,
        __out: PhantomData<Out>,
    }

    enum State {
        Head,
        Data(u16),
    }

    impl<In, Out> LengthDelimitedCodec<In, Out> {
        pub fn new() -> Self {
            LengthDelimitedCodec {
                state: State::Head,
                __in: PhantomData,
                __out: PhantomData,
            }
        }
    }

    impl<In, Out> LengthDelimitedCodec<In, Out> {
        fn decode_head(&mut self, buf: &mut BytesMut) -> io::Result<Option<u16>> {
            if buf.len() < 2 {
                // Not enough data
                return Ok(None);
            }

            let n = LittleEndian::read_u16(buf.as_ref());

            // Consume the length field
            let _ = buf.split_to(2);

            Ok(Some(n))
        }

        fn decode_data(&mut self, buf: &mut BytesMut, n: u16) -> io::Result<Option<Out>>
        where
            Out: DeserializeOwned + Debug,
        {
            // At this point, the buffer has already had the required capacity
            // reserved. All there is to do is read.
            let n = n as usize;
            if buf.len() < n {
                return Ok(None);
            }

            let buf = buf.split_to(n).freeze();

            trace!("Attempting to decode");
            let msg = try!(deserialize::<Out>(buf.as_ref()).map_err(|e| match *e {
                bincode::ErrorKind::IoError(e) => return e,
                _ => return io::Error::new(io::ErrorKind::Other, *e),
            }));

            trace!("... Decoded {:?}", msg);
            Ok(Some(msg))
        }
    }

    impl<In, Out> Codec for LengthDelimitedCodec<In, Out>
    where
        In: Serialize + Debug,
        Out: DeserializeOwned + Debug,
    {
        type In = In;
        type Out = Out;

        fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Out>> {
            let n = match self.state {
                State::Head => {
                    match try!(self.decode_head(buf)) {
                        Some(n) => {
                            self.state = State::Data(n);

                            // Ensure that the buffer has enough space to read the
                            // incoming payload
                            buf.reserve(n as usize);

                            n
                        }
                        None => return Ok(None),
                    }
                }
                State::Data(n) => n,
            };

            match try!(self.decode_data(buf, n)) {
                Some(data) => {
                    // Update the decode state
                    self.state = State::Head;

                    // Make sure the buffer has enough space to read the next head
                    buf.reserve(2);

                    Ok(Some(data))
                }
                None => Ok(None),
            }
        }

        fn encode(&mut self, item: Self::In, buf: &mut BytesMut) -> io::Result<()> {
            trace!("Attempting to encode");
            let encoded_len = serialized_size(&item);
            if encoded_len > 8 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "encoded message too big",
                ));
            }

            buf.reserve((encoded_len + 2) as usize);

            buf.put_u16::<LittleEndian>(encoded_len as u16);

            if let Err(e) = serialize_into::<_, Self::In, _>(
                &mut buf.writer(),
                &item,
                Bounded(encoded_len),
            )
            {
                match *e {
                    bincode::ErrorKind::IoError(e) => return Err(e),
                    _ => return Err(io::Error::new(io::ErrorKind::Other, *e)),

                }
            }

            Ok(())
        }
    }
}

pub mod frame {
    use bytes::{Bytes, Buf, BytesMut, IntoBuf};
    use futures::{AsyncSink, Poll, Stream, Sink, StartSend};
    use tokio_io::{AsyncRead, AsyncWrite};
    use std::io;
    use codec::Codec;

    const INITIAL_CAPACITY: usize = 1024;
    const BACKPRESSURE_THRESHOLD: usize = 4 * INITIAL_CAPACITY;

    /// A unified `Stream` and `Sink` interface over an I/O object, using
    /// the `Codec` trait to encode and decode frames.
    pub struct Framed<A, C> {
        io: A,
        codec: C,
        read_buf: BytesMut,
        write_buf: BytesMut,
        frame: Option<<Bytes as IntoBuf>::Buf>,
        is_readable: bool,
        eof: bool,
    }

    impl<A, C> Framed<A, C>
    where
        A: AsyncWrite,
    {
        // If there is a buffered frame, try to write it to `A`
        fn do_write(&mut self) -> Poll<(), io::Error> {
            loop {
                if self.frame.is_none() {
                    self.set_frame();
                }

                if self.frame.is_none() {
                    return Ok(().into());
                }

                let done = {
                    let frame = self.frame.as_mut().unwrap();
                    try_ready!(self.io.write_buf(frame));
                    !frame.has_remaining()
                };

                if done {
                    self.frame = None;
                }
            }
        }

        fn set_frame(&mut self) {
            if self.write_buf.is_empty() {
                return;
            }

            debug_assert!(self.frame.is_none());

            self.frame = Some(self.write_buf.take().freeze().into_buf());
        }
    }

    impl<A, C> Stream for Framed<A, C>
    where
        A: AsyncRead,
        C: Codec,
    {
        type Item = C::Out;
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
            loop {
                // Repeatedly call `decode` or `decode_eof` as long as it is
                // "readable". Readable is defined as not having returned `None`. If
                // the upstream has returned EOF, and the decoder is no longer
                // readable, it can be assumed that the decoder will never become
                // readable again, at which point the stream is terminated.
                if self.is_readable {
                    if self.eof {
                        let frame = try!(self.codec.decode_eof(&mut self.read_buf));
                        return Ok(Some(frame).into());
                    }

                    trace!("attempting to decode a frame");

                    if let Some(frame) = try!(self.codec.decode(&mut self.read_buf)) {
                        trace!("frame decoded from buffer");
                        return Ok(Some(frame).into());
                    }

                    self.is_readable = false;
                }

                assert!(!self.eof);

                // Otherwise, try to read more data and try again. Make sure we've
                // got room for at least one byte to read to ensure that we don't
                // get a spurious 0 that looks like EOF
                if try_ready!(self.io.read_buf(&mut self.read_buf)) == 0 {
                    self.eof = true;
                }

                self.is_readable = true;
            }
        }
    }

    impl<A, C> Sink for Framed<A, C>
    where
        A: AsyncWrite,
        C: Codec,
    {
        type SinkItem = C::In;
        type SinkError = io::Error;

        fn start_send(
            &mut self,
            item: Self::SinkItem,
        ) -> StartSend<Self::SinkItem, Self::SinkError> {
            // If the buffer is already over BACKPRESSURE_THRESHOLD,
            // then attempt to flush it. If after flush it's *still*
            // over BACKPRESSURE_THRESHOLD, then reject the send.
            if self.write_buf.len() > BACKPRESSURE_THRESHOLD {
                try!(self.poll_complete());
                if self.write_buf.len() > BACKPRESSURE_THRESHOLD {
                    return Ok(AsyncSink::NotReady(item));
                }
            }

            try!(self.codec.encode(item, &mut self.write_buf));
            Ok(AsyncSink::Ready)
        }

        fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
            trace!("flushing framed transport");

            try_ready!(self.do_write());

            try_nb!(self.io.flush());

            trace!("framed transport flushed");
            Ok(().into())
        }

        fn close(&mut self) -> Poll<(), Self::SinkError> {
            try_ready!(self.poll_complete());
            self.io.shutdown()
        }
    }

    fn write_zero() -> io::Error {
        io::Error::new(io::ErrorKind::WriteZero, "failed to write frame to io")
    }

    pub fn framed<A, C>(io: A, codec: C) -> Framed<A, C> {
        Framed {
            io: io,
            codec: codec,
            read_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
            write_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
            frame: None,
            is_readable: false,
            eof: false,
        }
    }
}

pub mod client_proxy {
    use futures::{Async, Poll, Future};
    use futures::sync::{mpsc, oneshot};
    use std::cell::RefCell;
    use std::io;
    use std::fmt;

    /// Message used to dispatch requests to the task managing the
    /// client connection.
    pub type Request<R, Q> = (R, oneshot::Sender<Q>);

    /// Receive requests submitted to the client
    pub type Receiver<R, Q> = mpsc::UnboundedReceiver<Request<R, Q>>;

    /// Response future returned from a client
    pub struct Response<Q> {
        inner: oneshot::Receiver<Q>,
    }

    pub struct ClientProxy<R, Q> {
        tx: RefCell<mpsc::UnboundedSender<Request<R, Q>>>,
    }

    pub fn channel<R, Q>() -> (ClientProxy<R, Q>, Receiver<R, Q>) {
        // Create a channel to send the requests to client-side of rpc.
        let (tx, rx) = mpsc::unbounded();

        // Wrap the `tx` part in ClientProxy so the rpc call interface
        // can be implemented.
        let client = ClientProxy { tx: RefCell::new(tx) };

        (client, rx)
    }

    impl<R, Q> ClientProxy<R, Q> {
        pub fn call(&self, request: R) -> Response<Q> {
            // The response to returned from the rpc client task over a
            // oneshot channel.
            let (tx, rx) = oneshot::channel();

            // If send returns an Err, its because the other side has been dropped.
            // By ignoring it, we are just dropping the `tx`, which will mean the
            // rx will return Canceled when polled. In turn, that is translated
            // into a BrokenPipe, which conveys the proper error.
            let _ = mpsc::UnboundedSender::unbounded_send(&mut self.tx.borrow_mut(), (request, tx));

            Response { inner: rx }
        }
    }

    impl<R, Q> fmt::Debug for ClientProxy<R, Q>
    where
        R: fmt::Debug,
        Q: fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ClientProxy {{ ... }}")
        }
    }

    impl<Q> Future for Response<Q> {
        type Item = Q;
        type Error = io::Error;

        fn poll(&mut self) -> Poll<Q, io::Error> {
            match self.inner.poll() {
                Ok(Async::Ready(res)) => Ok(Async::Ready(res)),
                Ok(Async::NotReady) => Ok(Async::NotReady),
                // Convert oneshot::Canceled into io::Error
                Err(_) => {
                    let e = io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe");
                    Err(e.into())
                }
            }
        }
    }

    impl<Q> fmt::Debug for Response<Q>
    where
        Q: fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "Response {{ ... }}")
        }
    }
}


pub mod rpc {
    use std::io;
    use std::fmt;
    use futures::{Async, AsyncSink, Sink, Stream, Poll, Future, IntoFuture};
    use futures::future::Executor;
    use std::collections::VecDeque;
    use futures::sync::oneshot;
    use client_proxy;

    pub trait Handler {
        /// Message type read from transport
        type In;
        /// Message type written to transport
        type Out;
        type Transport: 'static
            + Stream<Item = Self::In, Error = io::Error>
            + Sink<SinkItem = Self::Out, SinkError = io::Error>;

        /// Mutable reference to the transport
        fn transport(&mut self) -> &mut Self::Transport;

        /// Consume a request
        fn consume(&mut self, message: Self::In) -> io::Result<()>;

        /// Produce a response
        fn produce(&mut self) -> Poll<Option<Self::Out>, io::Error>;

        /// RPC currently in flight
        fn has_in_flight(&self) -> bool;
    }

    pub struct Driver<T>
    where
        T: Handler,
    {
        // Glue
        handler: T,

        // True as long as the connection has more request frames to read.
        run: bool,

        // True when the transport is fully flushed
        is_flushed: bool,
    }

    impl<T> Driver<T>
    where
        T: Handler,
    {
        /// Create a new rpc driver with the given service and transport.
        pub fn new(handler: T) -> Driver<T> {
            Driver {
                handler: handler,
                run: true,
                is_flushed: true,
            }
        }

        /// Returns true if the driver has nothing left to do
        fn is_done(&self) -> bool {
            !self.run && self.is_flushed && !self.has_in_flight()
        }

        /// Process incoming messages off the transport.
        fn receive_incoming(&mut self) -> io::Result<()> {
            while self.run {
                if let Async::Ready(req) = try!(self.handler.transport().poll()) {
                    try!(self.process_incoming(req));
                } else {
                    break;
                }
            }
            Ok(())
        }

        /// Process an incoming message
        fn process_incoming(&mut self, req: Option<T::In>) -> io::Result<()> {
            trace!("process_incoming");
            // At this point, the service & transport are ready to process the
            // request, no matter what it is.
            match req {
                Some(message) => {
                    trace!("received message");

                    if let Err(e) = self.handler.consume(message) {
                        // TODO: Should handler be infalliable?
                        panic!("unimplemented error handling: {:?}", e);
                    }
                }
                None => {
                    trace!("received None");
                    // At this point, we just return. This works
                    // because poll with be called again and go
                    // through the receive-cycle again.
                    self.run = false;
                }
            }

            Ok(())
        }

        /// Send outgoing messages to the transport.
        fn send_outgoing(&mut self) -> io::Result<()> {
            trace!("send_responses");
            loop {
                match try!(self.handler.produce()) {
                    Async::Ready(Some(message)) => {
                        trace!("  --> got message");
                        try!(self.process_outgoing(message));
                    }
                    Async::Ready(None) => {
                        trace!("  --> got None");
                        // The service is done with the connection.
                        break;
                    }
                    // Nothing to dispatch
                    Async::NotReady => break,
                }
            }

            Ok(())
        }

        fn process_outgoing(&mut self, message: T::Out) -> io::Result<()> {
            trace!("process_outgoing");
            try!(assert_send(&mut self.handler.transport(), message));

            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.is_flushed = try!(self.handler.transport().poll_complete()).is_ready();

            // TODO:
            Ok(())
        }

        fn has_in_flight(&self) -> bool {
            self.handler.has_in_flight()
        }
    }

    impl<T> Future for Driver<T>
    where
        T: Handler,
    {
        type Item = ();
        type Error = io::Error;

        fn poll(&mut self) -> Poll<(), Self::Error> {
            trace!("rpc::Driver::tick");

            // First read off data from the socket
            try!(self.receive_incoming());

            // Handle completed responses
            try!(self.send_outgoing());

            // Try flushing buffered writes
            try!(self.flush());

            if self.is_done() {
                return Ok(().into());
            }

            // Tick again later
            Ok(Async::NotReady)
        }
    }

    fn assert_send<S: Sink>(s: &mut S, item: S::SinkItem) -> Result<(), S::SinkError> {
        match try!(s.start_send(item)) {
            AsyncSink::Ready => Ok(()),
            AsyncSink::NotReady(_) => {
                panic!(
                    "sink reported itself as ready after `poll_ready` but was \
                        then unable to accept a message"
                )
            }
        }
    }

    impl<T> fmt::Debug for Driver<T>
    where
        T: Handler + fmt::Debug,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_struct("rpc::Handler")
                .field("handler", &self.handler)
                .field("run", &self.run)
                .field("is_flushed", &self.is_flushed)
                .finish()
        }
    }

    pub trait Client<A: 'static>: 'static {
        /// Request
        type Request: 'static;

        /// Response
        type Response: 'static;

        /// The message transport, which works with async I/O objects of type `A`
        type Transport: 'static
            + Stream<Item = Self::Response, Error = io::Error>
            + Sink<SinkItem = Self::Request, SinkError = io::Error>;

        /// A future for initializing a transport from an I/O object.
        ///
        /// In simple cases, `Result<Self::Transport, Self::Error>` often suffices.
        type BindTransport: IntoFuture<Item = Self::Transport, Error = io::Error>;

        /// Build a transport from the given I/O object, using `self`
        /// for any configuration.
        fn bind_transport(&self, io: A) -> Self::BindTransport;
    }

    pub fn bind_client<A, C, E>(
        io: A,
        client: C,
        executor: &E,
    ) -> client_proxy::ClientProxy<C::Request, C::Response>
    where
        A: 'static,
        C: Client<A>,
        E: Executor<Box<Future<Item = (), Error = ()>>>,
    {
        let (tx, rx) = client_proxy::channel();

        let fut = client.bind_transport(io).into_future().and_then(
            |transport| {
                let handler = ClientHandler::<A, C> {
                    transport: transport,
                    requests: rx,
                    in_flight: VecDeque::with_capacity(32),
                };
                Driver::new(handler)
            },
        );

        // Spawn the RPC driver into task
        executor.execute(Box::new(fut.map_err(|_| ()))).expect(
            "failed to spawn client onto executor",
        );

        tx
    }

    pub trait Server<A: 'static>: 'static {
        /// Request
        type Request: 'static;

        /// Response
        type Response: 'static;

        /// The message transport, which works with async I/O objects of
        /// type `A`.
        type Transport: 'static
            + Stream<Item = Self::Request, Error = io::Error>
            + Sink<SinkItem = Self::Response, SinkError = io::Error>;

        /// A future for initializing a transport from an I/O object.
        ///
        /// In simple cases, `Result<Self::Transport, Self::Error>` often suffices.
        type BindTransport: IntoFuture<Item = Self::Transport, Error = io::Error>;

        /// Build a transport from the given I/O object, using `self`
        /// for any configuration.
        fn bind_transport(&self, io: A) -> Self::BindTransport;

        /// Process the request and return the response asynchronously.
        fn process(&self, req: Self::Request) -> Self::Response;
    }

    /// Bind an async I/O object `io` to the `server`.
    pub fn bind_server<A, S, E>(io: A, server: S, executor: &E)
    where
        A: 'static,
        S: Server<A>,
        E: Executor<Box<Future<Item = (), Error = ()>>>,
    {
        let fut = server.bind_transport(io).into_future().and_then(
            |transport| {
                let handler = ServerHandler {
                    server: server,
                    transport: transport,
                    in_flight: VecDeque::with_capacity(32),
                };
                Driver::new(handler)
            },
        );

        // Spawn the RPC driver into task
        executor.execute(Box::new(fut.map_err(|_| ()))).expect(
            "failed to spawn server onto executor",
        )
    }

    struct ClientHandler<A, C>
    where
        A: 'static,
        C: Client<A>,
    {
        transport: C::Transport,
        requests: client_proxy::Receiver<C::Request, C::Response>,
        in_flight: VecDeque<oneshot::Sender<C::Response>>,
    }

    impl<A, C> Handler for ClientHandler<A, C>
    where
        A: 'static,
        C: Client<A>,
    {
        type In = C::Response;
        type Out = C::Request;
        type Transport = C::Transport;

        fn transport(&mut self) -> &mut Self::Transport {
            &mut self.transport
        }

        fn consume(&mut self, response: Self::In) -> io::Result<()> {
            trace!("ClientHandler::consume");
            if let Some(complete) = self.in_flight.pop_front() {
                drop(complete.send(response));
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "request / response mismatch",
                ));
            }

            Ok(())
        }

        /// Produce a message
        fn produce(&mut self) -> Poll<Option<Self::Out>, io::Error> {
            trace!("ClientHandler::produce");

            // Try to get a new request
            match self.requests.poll() {
                Ok(Async::Ready(Some((request, complete)))) => {
                    trace!("  --> received request");

                    // Track complete handle
                    self.in_flight.push_back(complete);

                    Ok(Some(request).into())
                }
                Ok(Async::Ready(None)) => {
                    trace!("  --> client dropped");
                    Ok(None.into())
                }
                Ok(Async::NotReady) => {
                    trace!("  --> not ready");
                    Ok(Async::NotReady)
                }
                Err(_) => panic!("Unreachable"),
            }
        }

        /// RPC currently in flight
        fn has_in_flight(&self) -> bool {
            !self.in_flight.is_empty()
        }
    }

    struct ServerHandler<A, S>
    where
        A: 'static,
        S: Server<A>,
    {
        // The service handling the connection
        server: S,
        // The transport responsible for sending/receving messages over the wire
        transport: S::Transport,
        // FIFO of "in flight" responses to requests.
        in_flight: VecDeque<S::Response>,
    }

    impl<A, S> Handler for ServerHandler<A, S>
    where
        A: 'static,
        S: Server<A>,
    {
        type In = S::Request;
        type Out = S::Response;
        type Transport = S::Transport;

        /// Mutable reference to the transport
        fn transport(&mut self) -> &mut Self::Transport {
            &mut self.transport
        }

        /// Consume a message
        fn consume(&mut self, request: Self::In) -> io::Result<()> {
            trace!("ServerHandler::consume");
            let response = self.server.process(request);
            self.in_flight.push_back(response);

            // TODO: Should the error be handled differently?
            Ok(())
        }

        /// Produce a message
        fn produce(&mut self) -> Poll<Option<Self::Out>, io::Error> {
            trace!("ServerHandler::produce");
            // Return the ready response
            match self.in_flight.pop_front() {
                Some(res) => {
                    trace!("  --> received response");
                    Ok(Some(res).into())
                }
                None => {
                    trace!("  --> not ready");
                    Ok(Async::NotReady)
                }
            }
        }

        /// RPC currently in flight
        fn has_in_flight(&self) -> bool {
            !self.in_flight.is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
