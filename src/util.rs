use crate::loader::Loader;
use crate::reader::Reader;
use crate::writer::Writer;
use std::io::Read;

#[cfg(any(
    feature = "async-extracter",
    feature = "async-reader",
    feature = "async-writer",
    feature = "async-loader"
))]
pub async fn process_batch<R: Read>(f: R, reader: Reader, writer: Writer, loader: Loader) {
    let batch_reader = reader.batch_reader(f);
    for (i, batch_) in batch_reader.enumerate() {
        let cursor = writer.write(batch_.unwrap());
        loader.load(cursor.into_inner().unwrap(), i).await;
    }
}

#[cfg(not(any(
    feature = "async-extracter",
    feature = "async-reader",
    feature = "async-writer",
    feature = "async-loader"
)))]
pub fn process_batch<R: Read>(f: R, reader: Reader, writer: Writer, loader: Loader) {
    let batch_reader = reader.batch_reader(f);
    for (i, batch_) in batch_reader.enumerate() {
        let cursor = writer.write(batch_.unwrap());
        loader.load(cursor.into_inner().unwrap(), i);
    }
}
