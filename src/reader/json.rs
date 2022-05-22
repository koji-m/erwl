use crate::cli::{
    ArgRequired::{False, True},
    ArgType, CmdArg, CmdArgEntry,
};
use arrow::{
    datatypes::{DataType, Field, Schema},
    json,
    json::reader::DecoderOptions,
};
use clap::ArgMatches;

use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt,
    fs::File,
    io::{BufReader, Read},
    sync::Arc,
};

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
    schema_file_path: String,
    decoder_options: DecoderOptions,
}

impl Reader {
    pub fn new(matches: &ArgMatches) -> Self {
        let batch_size: usize = matches.value_of_t("batch-size").unwrap();
        Self {
            schema_file_path: String::from(matches.value_of("schema-file").unwrap()),
            decoder_options: DecoderOptions::new().with_batch_size(batch_size),
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

    fn get_schema(&self) -> Result<Schema, Box<dyn Error>> {
        let mut column_definitions = vec![];
        let file = File::open(self.schema_file_path.as_str())?;
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

    pub fn batch_reader<R: Read>(&self, input_file: R) -> json::reader::Reader<R> {
        let schema = self.get_schema().unwrap();
        json::reader::Reader::new(input_file, Arc::new(schema), self.decoder_options.clone())
    }
}
