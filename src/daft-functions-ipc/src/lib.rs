use std::{
    io::Cursor,
    process::{Command, Stdio},
};

use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_dsl::{
    functions::{FunctionArgs, ScalarUDF},
    ExprRef,
};
use daft_recordbatch::{GrowableRecordBatch, RecordBatch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IPCUDF;

#[derive(FunctionArgs, Debug)]
pub struct Args<T> {
    // args
    #[arg(variadic)]
    pub args: Vec<T>,

    // kwargs
    pub name: String,
    pub num_expressions: usize,
    pub return_dtype: DataType,
    pub command: String,
    pub command_args: Vec<String>,
}

use arrow2::{
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
    io::ipc::{
        read::{read_stream_metadata, StreamReader},
        write::{StreamWriter, WriteOptions},
    },
};

#[typetag::serde]
impl ScalarUDF for IPCUDF {
    fn name(&self) -> &'static str {
        "ipc_udf"
    }

    fn evaluate(&self, inputs: FunctionArgs<Series>) -> DaftResult<Series> {
        let args: Args<_> = inputs.try_into()?;

        let rb = RecordBatch::from_nonempty_columns(args.args)?;

        let schema = rb.schema.to_arrow()?;
        let chunk = rb.to_chunk();

        let mut child = Command::new(args.command.clone())
            .args(args.command_args.clone())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        {
            let stdin = child.stdin.take().unwrap();
            let options = WriteOptions { compression: None };
            let mut writer = StreamWriter::new(stdin, options);
            writer.start(&schema, None)?;
            writer.write(&chunk, None)?;
            writer.finish()?;
        }
        let output = child.wait_with_output()?;

        if !output.status.success() {
            eprintln!("Process failed with status: {}", output.status);
            eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            panic!("Process failed");
        }

        if !output.stderr.is_empty() {
            eprintln!(
                "Process stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let mut cursor = Cursor::new(output.stdout);
        let stream_metadata = read_stream_metadata(&mut cursor)?;
        let output_schema = &stream_metadata.schema;
        let schema: Schema = output_schema.clone().into();
        let reader = StreamReader::new(&mut cursor, stream_metadata, None);
        let mut batches = Vec::new();

        for batch_result in reader {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.unwrap();
                    let rb = RecordBatch::from_arrow(schema.clone(), batch.into_arrays())?;
                    batches.push(rb);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    break;
                }
            }
        }
        let rb = RecordBatch::concat(&batches)?;
        let s = rb.get_column(0);
        Ok(s.clone())
    }

    fn function_args_to_field(
        &self,
        inputs: FunctionArgs<ExprRef>,
        schema: &daft_core::prelude::Schema,
    ) -> common_error::DaftResult<daft_core::prelude::Field> {
        let args: Args<_> = inputs.try_into()?;
        let fld = args.args[0].to_field(schema)?;

        Ok(Field::new(fld.name, args.return_dtype.clone()))
    }
}
