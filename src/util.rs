use crate::error::UnknownTypeError;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
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
