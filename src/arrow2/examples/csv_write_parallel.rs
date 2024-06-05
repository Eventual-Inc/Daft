use std::io::Write;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    error::Result,
    io::csv::write,
};

fn parallel_write(path: &str, batches: [Chunk<Box<dyn Array>>; 2]) -> Result<()> {
    let options = write::SerializeOptions::default();

    // write a header
    let mut writer = std::fs::File::create(path)?;
    write::write_header(&mut writer, &["c1"], &options)?;

    // prepare a channel to send serialized records from threads
    let (tx, rx): (Sender<_>, Receiver<_>) = mpsc::channel();
    let mut children = Vec::new();

    (0..2).for_each(|id| {
        // The sender endpoint can be cloned
        let thread_tx = tx.clone();

        let options = options.clone();
        let batch = batches[id].clone(); // note: this is cheap
        let child = thread::spawn(move || {
            let rows = write::serialize(&batch, &options).unwrap();
            thread_tx.send(rows).unwrap();
        });

        children.push(child);
    });

    for _ in 0..2 {
        // block: assumes that the order of batches matter.
        let records = rx.recv().unwrap();
        records.iter().try_for_each(|row| writer.write_all(row))?
    }

    for child in children {
        child.join().expect("child thread panicked");
    }

    Ok(())
}

fn main() -> Result<()> {
    let array = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ]);
    let columns = Chunk::new(vec![array.boxed()]);

    parallel_write("example.csv", [columns.clone(), columns])
}
