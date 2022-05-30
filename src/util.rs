use std::io::{Cursor, Write, Seek, SeekFrom};
use std::sync::{Arc, Mutex};

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
