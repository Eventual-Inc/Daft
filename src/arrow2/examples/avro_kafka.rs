use arrow2::{
    datatypes::{DataType, Field},
    error::Error,
    io::avro,
    io::avro::avro_schema::file::Block,
    io::avro::avro_schema::schema::{Field as AvroField, Schema},
};

fn read_schema_id<R: std::io::Read>(reader: &mut R) -> Result<u32, Error> {
    let mut header = [0; 5];
    reader.read_exact(&mut header)?;

    if header[0] != 0 {
        return Err(Error::ExternalFormat(
            "Avro requires the first byte to be a zero".to_string(),
        ));
    }
    Ok(u32::from_be_bytes(header[1..].try_into().unwrap()))
}

fn read_block<R: std::io::Read>(reader: &mut R, block: &mut Block) -> Result<(), Error> {
    // (we could lump multiple records together by extending the block instead of clearing)
    block.data.clear();
    reader.read_to_end(&mut block.data)?;
    block.number_of_rows = 1;
    Ok(())
}

fn main() -> Result<(), Error> {
    // say we received the event from Kafka
    let data = &[0, 0, 0, 0, 1, 6, 115, 99, 105, 6, 109, 97, 115];
    let mut stream = std::io::Cursor::new(data);

    // we can fetch its schema_id from the registry via:
    let _schema_id = read_schema_id(&mut stream)?;

    // say that from the registry we concluded that this schema has fields
    let avro_fields = vec![
        AvroField::new("first_name", Schema::String(None)),
        AvroField::new("last_name", Schema::String(None)),
    ];
    // (which we map to arrow fields as)
    let fields = vec![
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
    ];

    // the below allow us to read it to arrow (a chunk of a single element)
    let mut block = Block::default();
    read_block(&mut stream, &mut block)?;

    let chunk = avro::read::deserialize(&block, &fields, &avro_fields, &[true, true])?;

    println!("{chunk:?}");
    Ok(())
}
