use crate::error::UnknownTypeError;
use crate::reader::Reader;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::{
    fs::File,
    io::{BufReader, Cursor, Seek, SeekFrom, Write},
};

#[derive(Serialize, Deserialize)]
struct BigQueryColumnDefinition {
    name: String,
    r#type: String,
    mode: String,
}

fn create_field(
    name: &str,
    type_: &str,
    mode: Option<&str>,
) -> Result<Field, Box<dyn std::error::Error>> {
    let t = match type_ {
        "BOOL" => Ok(DataType::Boolean),
        "STRING" => Ok(DataType::Utf8),
        "INTEGER" => Ok(DataType::Int64),
        "FLOAT" => Ok(DataType::Float64),
        "NUMERIC" => Ok(DataType::Decimal(38, 9)),
        "TIMESTAMP" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "DATE" => Ok(DataType::Date64),
        unknown => Err(UnknownTypeError {
            type_name: String::from(unknown),
        }),
    }?;
    let nullable = if let Some(nullable_) = mode {
        nullable_ == "NULLABLE"
    } else {
        false
    };

    Ok(Field::new(name, t, nullable))
}

pub fn get_schema(schema_file_path: String) -> Result<Schema, Box<dyn std::error::Error>> {
    let mut column_definitions = vec![];
    let file = File::open(schema_file_path.as_str())?;
    let reader = BufReader::new(file);
    let schema: Vec<BigQueryColumnDefinition> = serde_json::from_reader(reader)?;
    for column_definition in &schema {
        let field = create_field(
            &column_definition.name,
            &column_definition.r#type,
            Some(&column_definition.mode),
        )?;
        column_definitions.push(field);
    }
    Ok(Schema::new(column_definitions))
}

#[derive(Debug, Default, Clone)]
pub struct WriteableCursor {
    buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl WriteableCursor {
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.buffer)
            .ok()
            .and_then(|mutex| mutex.into_inner().ok())
            .map(|cursor| cursor.into_inner())
    }

    pub fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            buffer: self.buffer.clone(),
        })
    }
}

impl Write for WriteableCursor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.buffer.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.buffer.lock().unwrap();
        inner.flush()
    }
}

impl Seek for WriteableCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.buffer.lock().unwrap();
        inner.seek(pos)
    }
}

pub trait WriteBatch {
    fn current_batch(&self) -> &Option<RecordBatch>;
    fn current_batch_mut(&mut self) -> &mut Option<RecordBatch>;
    fn current_offset(&self) -> usize;
    fn current_offset_mut(&mut self) -> &mut usize;
    fn reader_mut(&mut self) -> &mut Reader;
    fn schema(&self) -> &Option<SchemaRef>;
    fn schema_mut(&mut self) -> &mut Option<SchemaRef>;
    fn file_extension(&self) -> &String;
    fn write_batch(&self, batch: RecordBatch, cursor: &WriteableCursor);

    fn write(&mut self, cursor: &WriteableCursor, size: usize) -> usize {
        let mut rows_need = size;
        let mut batches = Vec::new();
        while rows_need > 0 {
            if let Some(batch) = self.current_batch() {
                let residue = batch.num_rows() - self.current_offset();
                if residue > rows_need {
                    let sliced = batch.slice(self.current_offset(), rows_need);
                    batches.push(sliced);
                    *self.current_offset_mut() += rows_need;
                    rows_need = 0;
                } else {
                    let sliced = batch.slice(self.current_offset(), residue);
                    batches.push(sliced);
                    rows_need -= residue;
                    *self.current_offset_mut() = 0;
                    *self.current_batch_mut() = self.reader_mut().next();
                }
            } else if let Some(batch) = self.reader_mut().next() {
                *self.schema_mut() = Some(batch.schema());
                *self.current_batch_mut() = Some(batch);
                *self.current_offset_mut() = 0;
            } else {
                break;
            }
        }
        let batch = RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap();
        self.write_batch(batch, cursor);
        size - rows_need
    }

    fn write_all(&mut self, cursor: &WriteableCursor) -> usize {
        let mut batches = Vec::new();
        let mut num_rows_wrote = 0;
        while let Some(batch) = self.reader_mut().next() {
            *self.schema_mut() = Some(batch.schema());
            num_rows_wrote += batch.num_rows();
            batches.push(batch);
        } 
        let batch = RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap();
        self.write_batch(batch, cursor);
        num_rows_wrote
    }
}
