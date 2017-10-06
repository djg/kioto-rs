extern crate kioto;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate futures;
#[macro_use]
extern crate serde_derive;
extern crate tokio_core;

use futures::sync::oneshot;
use futures::{Future, Stream};
use kioto::codec::LengthDelimitedCodec;
use kioto::frame::{framed, Framed};
use kioto::rpc;
use std::io;
use std::thread;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Core;

macro_rules! t {
        ($e:expr) => (match $e {
            Ok(e) => e,
            Err(e) => panic!("{} failed with {:?}", stringify!($e), e),
        })
    }

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
enum Request {
    Req(u32),
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq)]
enum Response {
    Resp(u32),
}

struct TestServer;

impl rpc::Server<TcpStream> for TestServer {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<TcpStream, LengthDelimitedCodec<Self::Response, Self::Request>>;
    type BindTransport = io::Result<Self::Transport>;

    fn bind_transport(&self, io: TcpStream) -> io::Result<Self::Transport> {
        let codec = LengthDelimitedCodec::<Response, Request>::new();
        Ok(framed(io, codec))
    }

    fn process(&self, req: Self::Request) -> Self::Response {
        match req {
            Request::Req(n) => Response::Resp(n),
        }
    }
}

struct TestClient;

impl rpc::Client<TcpStream> for TestClient {
    type Request = Request;
    type Response = Response;
    type Transport = Framed<TcpStream, LengthDelimitedCodec<Self::Request, Self::Response>>;
    type BindTransport = io::Result<Self::Transport>;

    fn bind_transport(&self, io: TcpStream) -> io::Result<Self::Transport> {
        let codec = LengthDelimitedCodec::<Request, Response>::new();
        Ok(framed(io, codec))
    }
}

#[test]
fn simple() {
    env_logger::init().unwrap();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let ts = thread::spawn(|| {
        let mut core = t!(Core::new());
        let handle = core.handle();
        let listener = t!(TcpListener::bind(&t!("127.0.0.1:12345".parse()), &handle));

        let server = listener.incoming().for_each(move |(socket, _)| {
            trace!("Incoming connection");

            let server = TestServer;
            rpc::bind_server(socket, server, &handle);

            Ok(())
        });

        let handle = core.handle();
        handle.spawn(server.map_err(|_| ()));

        core.run(shutdown_rx).unwrap();
    });

    thread::sleep(std::time::Duration::from_millis(500));

    let mut core = t!(Core::new());
    let handle = core.handle();

    trace!("Initiating connection");

    let client = TcpStream::connect(&t!("127.0.0.1:12345".parse()), &handle)
        .and_then(|socket| {
            trace!("Connected to server");
            let client = TestClient;
            let rpc = rpc::bind_client(socket, client, &handle);

            Ok(rpc)
        })
        .and_then(|rpc| {
            trace!("RPC client created");
            rpc.call(Request::Req(42))
        })
        .and_then(|resp| {
            trace!("Received response: {:?}", resp);
            assert_eq!(resp, Response::Resp(42));
            Ok(())
        });

    core.run(client).unwrap();

    trace!("Finished client future");

    drop(shutdown_tx.send(()));

    ts.join().unwrap();
}
