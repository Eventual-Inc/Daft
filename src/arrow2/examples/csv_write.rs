use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    error::Result,
    io::csv::write,
};

fn write_batch<A: AsRef<dyn Array>>(path: &str, columns: &[Chunk<A>]) -> Result<()> {
    let mut writer = std::fs::File::create(path)?;

    let options = write::SerializeOptions::default();
    write::write_header(&mut writer, &["c1"], &options)?;

    columns
        .iter()
        .try_for_each(|batch| write::write_chunk(&mut writer, batch, &options))
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
    let batch = Chunk::try_new(vec![&array as &dyn Array])?;

    write_batch("example.csv", &[batch])
}
