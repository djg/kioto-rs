mod server {
    use futures::{future, Stream, Sink};
    use futures::future::FutureResult;
    use messages::{Request, Response};
    use std::io;
    use tokio_io::AsyncRead;
    use tokio_core::net::TcpStream;
    use frame;
    use codec;

    pub struct Server;

    impl Server {
        fn call(&self, req: Request) -> Option<future::FutureResult<Response, io::Error>> {
            match req {
                Request::Data => Some(future::ok(Response::Data)),
                Request::State => None, // No response
            }
        }
    }

    fn process_request(
        service: &mut Server,
        req: IoResult<Request>,
    ) -> Option<IoFutureResult<Response>> {
        req.ok().and_then(|req| service.call(req))
    }

    type IoResult<T> = io::Result<T>;
    type IoFutureResult<T> = FutureResult<T, io::Error>;

    /*
    pub fn server(io: TcpStream) -> Box<impl Stream> {
        let send_buffer = BytesMut::with_capacity(4096);
        let (reader, writer) = io.split();
        let requests = frame::deframer(reader, BytesMut::with_capacity(4096)).map(decode);
        let responses = frame::framer(writer).with(move |resp| encode(resp, &mut send_buffer));
        let service = Server;

        let t = requests
            .filter_map(move |req| process_request(&mut service, req))
            .buffered(16)
            .forward(responses);
        Box::new(t)
    }
*/
}
