use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use crate::error::UnknownTypeError;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::channel::mpsc;
use futures::StreamExt;
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

pub struct BatchReceiver {
    current_batch: Option<RecordBatch>,
    current_offset: usize,
    rx: mpsc::UnboundedReceiver<RecordBatch>,
    schema: Option<SchemaRef>,
}

impl BatchReceiver {
    pub fn new(rx: mpsc::UnboundedReceiver<RecordBatch>) -> Self {
        Self {
            current_batch: None,
            current_offset: 0,
            schema: None,
            rx,
        }
    }
    fn current_batch(&self) -> &Option<RecordBatch> {
        &self.current_batch
    }

    fn current_batch_mut(&mut self) -> &mut Option<RecordBatch> {
        &mut self.current_batch
    }

    fn current_offset(&self) -> usize {
        self.current_offset
    }

    fn current_offset_mut(&mut self) -> &mut usize {
        &mut self.current_offset
    }

    fn rx(&mut self) -> &mut mpsc::UnboundedReceiver<RecordBatch> {
        &mut self.rx
    }

    fn schema(&self) -> &Option<SchemaRef> {
        &self.schema
    }

    fn schema_mut(&mut self) -> &mut Option<SchemaRef> {
        &mut self.schema
    }

    pub async fn receive(&mut self, size: usize) -> Option<RecordBatch> {
        let mut rows_need = size;
        let mut batches = Vec::new();
        while rows_need > 0 {
            if let Some(batch) = self.current_batch() {
                if batch.num_rows() == 0 { break; }
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
                    *self.current_batch_mut() = self.rx().next().await;
                }
            } else if let Some(batch) = self.rx().next().await {
                *self.schema_mut() = Some(batch.schema());
                *self.current_batch_mut() = Some(batch);
                *self.current_offset_mut() = 0;
            } else {
                break;
            }
        }

        if batches.len() > 0 {
            Some(RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap())
        } else {
            None
        }
    }

    pub async fn receive_all(&mut self) -> Option<RecordBatch> {
        let mut batches = Vec::new();
        while let Some(batch) = self.rx().next().await {
            *self.schema_mut() = Some(batch.schema());
            batches.push(batch);
        }
        if batches.len() > 0 {
            Some(RecordBatch::concat(self.schema().as_ref().unwrap(), &batches).unwrap())
        } else {
            None
        }
    }
}