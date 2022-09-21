use crate::cli::ArgRequired::True;
use crate::cli::ArgRequired::False;
use crate::cli::CmdArg;
use crate::cli::CmdArgEntry;
use crate::cli::DefaultValue;
use crate::error::GenericError;
use arrow::record_batch::RecordBatch;
use arrow::array;
use arrow::datatypes::Schema;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::TimeUnit;
use clap::ArgMatches;
use futures::channel::mpsc;
use futures::SinkExt;
use sqlx::Column;
use sqlx::postgres;
use sqlx::Row;
use sqlx::TypeInfo;
use sqlx::types::chrono;
use std::future::Future;
use std::sync::Arc;

fn to_arrow_field(name: &str, pgtype: &str) -> Field {
    match pgtype {
        "BOOL" => Field::new(name, DataType::Boolean, false),
        "INT2" => Field::new(name, DataType::Int16, false),
        "INT4" => Field::new(name, DataType::Int32, false),
        "INT8" => Field::new(name, DataType::Int64, false),
        "FLOAT4" => Field::new(name, DataType::Float32, false),
        "FLOAT8" => Field::new(name, DataType::Float64, false),
        "VARCHAR" => Field::new(name, DataType::Utf8, false),
        "TEXT" => Field::new(name, DataType::Utf8, false),
        "TIMESTAMP" => Field::new(name, DataType::Timestamp(TimeUnit::Second, None), false),
        ty => panic!("type not supported: {}", ty),
    }
}

fn to_arrow_schema(cols: &[postgres::PgColumn]) -> Schema {
    let mut fields = vec![];
    for col in cols {
        fields.push(to_arrow_field(col.name().clone(), col.type_info().name()));
    }
    Schema::new(fields)
}

fn to_record_batch(rowv: &Vec<postgres::PgRow>, cols: &[postgres::PgColumn], schema: Schema) -> RecordBatch {
    let mut array_refv = Vec::<array::ArrayRef>::new();

    for col in cols {
        let col_name = col.name();
        let col_type = col.type_info().name();
        let mut rows = rowv.iter();
        match col_type {
            "BOOL" => {
                let mut v: Vec<bool> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::BooleanArray::from(v)));
            },
            "INT2" => {
                let mut v: Vec<i16> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::Int16Array::from(v)));
            },
            "INT4" => {
                let mut v: Vec<i32> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::Int32Array::from(v)));
            },
            "INT8" => {
                let mut v: Vec<i64> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::Int64Array::from(v)));
            },
            "FLOAT4" => {
                let mut v: Vec<f32> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::Float32Array::from(v)));
            },
            "FLOAT8" => {
                let mut v: Vec<f64> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::Float64Array::from(v)));
            },
            "VARCHAR" => {
                let mut v: Vec<String> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::StringArray::from(v)));
            },
            "TEXT" => {
                let mut v: Vec<String> = vec![];
                while let Some(row) = rows.next() {
                    let next_val = row.try_get(col_name).unwrap();
                    v.push(next_val);
                }
                array_refv.push(Arc::new(array::StringArray::from(v)));
            },
            "TIMESTAMP" => {
                let mut v: Vec<i64> = vec![];
                while let Some(row) = rows.next() {
                    let next_val: chrono::NaiveDateTime = row.try_get(col_name).unwrap();
                    v.push(next_val.timestamp());
                }
                array_refv.push(Arc::new(array::TimestampSecondArray::from(v)));
            },
            ty => { panic!("type not supported: {}", ty) },
        }
    }

    RecordBatch::try_new(Arc::new(schema), array_refv).unwrap()
}

async fn fetch_forward(fetch_rows: usize, transaction: &mut sqlx::Transaction<'static, sqlx::Postgres>) -> Vec<postgres::PgRow> {
    let fetch_query = format!("FETCH FORWARD {} FROM cur", fetch_rows);
    let sql_fetch = sqlx::query(&fetch_query);
    sql_fetch.fetch_all(&mut *transaction).await.unwrap()
}

fn column_definitions(rowv: &Vec<postgres::PgRow>) -> &[postgres::PgColumn] {
    let mut rows = rowv.iter();
    let first_row = rows.next().unwrap();
    first_row.columns()
}

pub struct Extractor {
    url: String,
    table: String,
    fetch_rows: usize,
}

impl Extractor {
    pub fn new(matches: &ArgMatches) -> Extractor {
        let username = String::from(matches.value_of("username").unwrap());
        let password = String::from(matches.value_of("password").unwrap());
        let host = String::from(matches.value_of("host").unwrap());
        let port = String::from(matches.value_of("port").unwrap());
        let database = String::from(matches.value_of("database").unwrap());
        let url = format!("postgresql://{}:{}@{}:{}/{}", &username, &password, &host, &port, &database);

        let table = String::from(matches.value_of("table").unwrap());

        let fetch_rows: usize = matches.value_of_t("fetch-rows").unwrap();

        Self {
            url,
            table,
            fetch_rows,
        }
    }

    pub fn cmd_args() -> CmdArg {
        CmdArg::new(vec![
            CmdArgEntry::new(
                "table",
                "PostgreSQL table for record extraction",
                "table",
                true,
                True,
            ),
            CmdArgEntry::new(
                "host",
                "PostgreSQL host name",
                "host",
                true,
                True,
            ),
            CmdArgEntry::new(
                "port",
                "PostgreSQL server port",
                "port",
                true,
                False(DefaultValue::String(String::from("5432"))),
            ),
            CmdArgEntry::new(
                "username",
                "PostgreSQL user name",
                "username",
                true,
                True,
            ),
            CmdArgEntry::new(
                "password",
                "PostgreSQL password",
                "password",
                true,
                True,
            ),
            CmdArgEntry::new(
                "database",
                "PostgreSQL database",
                "database",
                true,
                True,
            ),
            CmdArgEntry::new(
                "fetch-rows",
                "Number of rows fetch at one time",
                "fetch-rows",
                true,
                False(DefaultValue::String(String::from("10000"))),
            ),
        ])
    }

    pub fn extract(&self, mut tx: mpsc::UnboundedSender<RecordBatch>) -> impl Future<Output = Result<(), GenericError>> {
        let url = self.url.clone();
        let fetch_rows = self.fetch_rows;
        let from_clause = self.table.clone();
        let select_clause = "*";

        async move {
            let pool = postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect(&url).await.unwrap();

            let mut transaction = pool.begin().await.unwrap();

            let declare_query = format!("DECLARE cur CURSOR FOR SELECT {} FROM {}", select_clause, from_clause);
            let sql_decl = sqlx::query(&declare_query);
            sql_decl.execute(&mut *transaction).await.unwrap();

            // first batch processing
            let rowv = fetch_forward(fetch_rows, &mut transaction).await;
            if rowv.len() == 0 {
                return Ok(());
            }

            let cols = column_definitions(&rowv);

            let schema = to_arrow_schema(&cols);

            // first record batch
            let rec = to_record_batch(&rowv, cols, schema.clone());
            if let Err(_) = tx.send(rec).await {
                return Err(GenericError { message: String::from("mpsc send error") })
            }

            // next batches processing
            loop {
                let rowv = fetch_forward(fetch_rows, &mut transaction).await;
                if rowv.len() == 0 {
                    break;
                }

                // next iterations: collect values
                let rec = to_record_batch(&rowv, cols, schema.clone());
                if let Err(_) = tx.send(rec).await {
                    return Err(GenericError { message: String::from("mpsc send error") })
                }
            }

            transaction.commit().await.unwrap();

            Ok(())
        }
    }
}
