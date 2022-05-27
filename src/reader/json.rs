use crate::cli::{
    ArgRequired::{False, True},
    ArgType, CmdArg, CmdArgEntry,
};
use crate::extracter::Extracter;
use arrow::{
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::ArrowError,
    json,
    json::reader::DecoderOptions,
    record_batch::RecordBatch,
};
use clap::ArgMatches;

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, fs::File, io::BufReader, sync::Arc};

#[derive(Serialize, Deserialize)]
struct BigQueryColumnDefinition {
    name: String,
    r#type: String,
    mode: String,
}

#[derive(Debug, Clone)]
struct UnknownTypeError {
    type_name: String,
}

impl fmt::Display for UnknownTypeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "unknown BigQuery type: {}", self.type_name)
    }
}

impl Error for UnknownTypeError {
    fn description(&self) -> &str {
        "unknown BigQuery types"
    }
}

fn create_field(name: &str, type_: &str, mode: Option<&str>) -> Result<Field, Box<dyn Error>> {
    let t = match type_ {
        "STRING" => Ok(DataType::Utf8),
        "INTEGER" => Ok(DataType::Int64),
        "FLOAT64" => Ok(DataType::Float64),
        "NUMERIC" => Ok(DataType::Decimal(38, 9)),
        "TIMESTAMP" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
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

pub struct Reader {
    batch_reader: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>>>,
}

impl Reader {
    pub fn new(matches: &ArgMatches, extracter: Extracter) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        let batch_extracter = extracter.batch_extracter();
        let schema_file_path = String::from(matches.value_of("schema-file").unwrap());
        let schema = Self::get_schema(schema_file_path).unwrap();
        let decoder_options = DecoderOptions::new().with_batch_size(batch_size);
        let batch_reader =
            json::reader::Reader::new(batch_extracter, Arc::new(schema), decoder_options);
        Self {
            batch_reader: Box::new(batch_reader),
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "schema-file",
                "Schema file (BigQuery schema)",
                "schema",
                true,
                True,
                ArgType::String,
            ),
            CmdArgEntry::new(
                "batch-size",
                "number of records in each files",
                "batch-size",
                true,
                False(String::from("10000")),
                ArgType::Number,
            ),
        ])
    }

    fn get_schema(schema_file_path: String) -> Result<Schema, Box<dyn Error>> {
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

    /*
    #[cfg(feature = "async-extracter")]
    pub async fn batch_reader(&self) -> json::reader::Reader<Box<dyn Read>> {
        let batch_extracter = self.extracter.batch_extracter().await;
        let schema = self.get_schema().unwrap();
        json::reader::Reader::new(
            batch_extracter,
            Arc::new(schema),
            self.decoder_options.clone(),
        )
    }

    #[cfg(not(feature = "async-extracter"))]
    pub fn batch_reader(&self) -> json::reader::Reader<Box<dyn Read>> {
        let batch_extracter = self.extracter.batch_extracter();
        let schema = self.get_schema().unwrap();
        json::reader::Reader::new(
            batch_extracter,
            Arc::new(schema),
            self.decoder_options.clone(),
        )
    }
    */
}

impl Iterator for Reader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch_reader.next()
    }
}
