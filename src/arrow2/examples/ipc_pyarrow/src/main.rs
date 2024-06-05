use std::net::TcpStream;
use std::thread;
use std::time::Duration;

use arrow2::array::{Array, Int64Array};
use arrow2::datatypes::DataType;
use arrow2::error::Result;
use arrow2::io::ipc::read;

fn main() -> Result<()> {
    const ADDRESS: &str = "127.0.0.1:12989";

    let mut reader = TcpStream::connect(ADDRESS)?;
    let metadata = read::read_stream_metadata(&mut reader)?;
    let mut stream = read::StreamReader::new(&mut reader, metadata);

    let mut idx = 0;
    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(b)) => {
                    idx += 1;
                    println!("batch: {:?}", idx)
                }
                Ok(read::StreamState::Waiting) => thread::sleep(Duration::from_millis(2000)),
                Err(l) => println!("{:?} ({})", l, idx),
            },
            None => break,
        };
    }

    Ok(())
}
