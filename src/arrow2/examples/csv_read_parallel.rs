use crossbeam_channel::unbounded;

use std::thread;
use std::time::SystemTime;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::{error::Result, io::csv::read};

fn parallel_read(path: &str) -> Result<Vec<Chunk<Box<dyn Array>>>> {
    let batch_size = 100;
    let has_header = true;
    let projection = None;

    // prepare a channel to send serialized records from threads
    let (tx, rx) = unbounded();

    let mut reader = read::ReaderBuilder::new().from_path(path)?;
    let (fields, _) =
        read::infer_schema(&mut reader, Some(batch_size * 10), has_header, &read::infer)?;
    let fields = Box::new(fields);

    let start = SystemTime::now();
    // spawn a thread to produce `Vec<ByteRecords>` (IO bounded)
    let child = thread::spawn(move || {
        let mut line_number = 0;
        let mut size = 1;
        while size > 0 {
            let mut rows = vec![read::ByteRecord::default(); batch_size];
            let rows_read = read::read_rows(&mut reader, 0, &mut rows).unwrap();
            rows.truncate(rows_read);
            line_number += rows.len();
            size = rows.len();
            tx.send((rows, line_number)).unwrap();
        }
    });

    let mut children = Vec::new();
    // use 3 consumers of to decompress, decode and deserialize.
    for _ in 0..3 {
        let rx_consumer = rx.clone();
        let consumer_fields = fields.clone();
        let child = thread::spawn(move || {
            let (rows, line_number) = rx_consumer.recv().unwrap();
            let start = SystemTime::now();
            println!("consumer start - {line_number}");
            let batch = read::deserialize_batch(
                &rows,
                &consumer_fields,
                projection,
                0,
                read::deserialize_column,
            )
            .unwrap();
            println!(
                "consumer end - {:?}: {}",
                start.elapsed().unwrap(),
                line_number,
            );
            batch
        });
        children.push(child);
    }

    child.join().expect("child thread panicked");

    let batches = children
        .into_iter()
        .map(|x| x.join().unwrap())
        .collect::<Vec<_>>();
    println!("Finished - {:?}", start.elapsed().unwrap());

    Ok(batches)
}

fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();
    let file_path = &args[1];

    let batches = parallel_read(file_path)?;
    for batch in batches {
        println!("{}", batch.len())
    }
    Ok(())
}
