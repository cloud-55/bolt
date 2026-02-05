use byteordered::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use storage::DatabaseId;
use std::io::{Read, Write, Result, Cursor};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt as TokioAsyncReadExt, AsyncWriteExt as TokioAsyncWriteExt};

/// Wire protocol format:
/// 1. code (u16)
/// 2. database_id_length (u32)
/// 3. key_length (u32)
/// 4. value_length (u32)
/// 5. database_id (bytes) - if length > 0
/// 6. key (bytes)
/// 7. value (bytes)

const HEADER_SIZE: usize = 2 + 4 + 4 + 4; // code + db_id_len + key_len + value_len
const MAX_MESSAGE_SIZE: u64 = 64 * 1024 * 1024; // 64 MB

#[derive(Debug, Clone)]
pub struct Message {
    pub code: u16,
    pub key: String,
    pub value: String,
    pub not_found: bool,
    pub database_id: DatabaseId,
}

impl Message {
    /// Synchronous send for std::io::Write
    pub fn send<W: Write>(&self, stream: &mut W) -> Result<()> {
        let database_id_bytes = match &self.database_id {
            DatabaseId::Default => Vec::new(),
            DatabaseId::Custom(name) => name.as_bytes().to_vec(),
        };

        let database_id_length = database_id_bytes.len() as u32;
        let key_length = self.key.len() as u32;
        let value_length = self.value.len() as u32;

        // Write header
        stream.write_u16::<BigEndian>(self.code)?;
        stream.write_u32::<BigEndian>(database_id_length)?;
        stream.write_u32::<BigEndian>(key_length)?;
        stream.write_u32::<BigEndian>(value_length)?;

        // Write body
        if !database_id_bytes.is_empty() {
            stream.write_all(&database_id_bytes)?;
        }
        stream.write_all(self.key.as_bytes())?;
        stream.write_all(self.value.as_bytes())?;

        stream.flush()?;
        Ok(())
    }

    /// Synchronous receive for std::io::Read
    pub fn receive<R: Read>(stream: &mut R) -> Result<Message> {
        // Read header
        let code = stream.read_u16::<BigEndian>()?;
        let database_id_length = stream.read_u32::<BigEndian>()?;
        let key_length = stream.read_u32::<BigEndian>()?;
        let value_length = stream.read_u32::<BigEndian>()?;

        // Validate total message size to prevent OOM attacks
        let total_size = database_id_length as u64 + key_length as u64 + value_length as u64;
        if total_size > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message too large: {} bytes (max: {} bytes)", total_size, MAX_MESSAGE_SIZE),
            ));
        }

        // Read body
        let database_id = if database_id_length > 0 {
            let mut buf = vec![0u8; database_id_length as usize];
            stream.read_exact(&mut buf)?;
            let name = String::from_utf8_lossy(&buf).to_string();
            DatabaseId::Custom(name)
        } else {
            DatabaseId::Default
        };

        let mut key = vec![0u8; key_length as usize];
        stream.read_exact(&mut key)?;
        let key = String::from_utf8_lossy(&key).to_string();

        let mut value = vec![0u8; value_length as usize];
        stream.read_exact(&mut value)?;
        let value = String::from_utf8_lossy(&value).to_string();

        Ok(Message {
            code,
            key,
            value,
            not_found: false,
            database_id,
        })
    }

    /// Async send for tokio::io::AsyncWrite
    pub async fn send_async<W: AsyncWrite + Unpin + Send>(&self, stream: &mut W) -> Result<()> {
        let database_id_bytes = match &self.database_id {
            DatabaseId::Default => Vec::new(),
            DatabaseId::Custom(name) => name.as_bytes().to_vec(),
        };

        let database_id_length = database_id_bytes.len() as u32;
        let key_length = self.key.len() as u32;
        let value_length = self.value.len() as u32;

        // Build header in buffer using byteorder (synchronous)
        let mut header = [0u8; HEADER_SIZE];
        {
            let mut cursor = Cursor::new(&mut header[..]);
            WriteBytesExt::write_u16::<BigEndian>(&mut cursor, self.code)?;
            WriteBytesExt::write_u32::<BigEndian>(&mut cursor, database_id_length)?;
            WriteBytesExt::write_u32::<BigEndian>(&mut cursor, key_length)?;
            WriteBytesExt::write_u32::<BigEndian>(&mut cursor, value_length)?;
        }

        // Write header
        TokioAsyncWriteExt::write_all(stream, &header).await?;

        // Write body
        if !database_id_bytes.is_empty() {
            TokioAsyncWriteExt::write_all(stream, &database_id_bytes).await?;
        }
        TokioAsyncWriteExt::write_all(stream, self.key.as_bytes()).await?;
        TokioAsyncWriteExt::write_all(stream, self.value.as_bytes()).await?;

        TokioAsyncWriteExt::flush(stream).await?;
        Ok(())
    }

    /// Async receive for tokio::io::AsyncRead
    pub async fn receive_async<R: AsyncRead + Unpin + Send>(stream: &mut R) -> Result<Message> {
        // Read header
        let mut header = [0u8; HEADER_SIZE];
        TokioAsyncReadExt::read_exact(stream, &mut header).await?;

        // Parse header using byteorder (synchronous)
        let (code, database_id_length, key_length, value_length) = {
            let mut cursor = Cursor::new(&header[..]);
            let code = ReadBytesExt::read_u16::<BigEndian>(&mut cursor)?;
            let database_id_length = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            let key_length = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            let value_length = ReadBytesExt::read_u32::<BigEndian>(&mut cursor)?;
            (code, database_id_length, key_length, value_length)
        };

        // Validate total message size to prevent OOM attacks
        let total_size = database_id_length as u64 + key_length as u64 + value_length as u64;
        if total_size > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message too large: {} bytes (max: {} bytes)", total_size, MAX_MESSAGE_SIZE),
            ));
        }

        // Read body
        let database_id = if database_id_length > 0 {
            let mut buf = vec![0u8; database_id_length as usize];
            TokioAsyncReadExt::read_exact(stream, &mut buf).await?;
            let name = String::from_utf8_lossy(&buf).to_string();
            DatabaseId::Custom(name)
        } else {
            DatabaseId::Default
        };

        let mut key = vec![0u8; key_length as usize];
        TokioAsyncReadExt::read_exact(stream, &mut key).await?;
        let key = String::from_utf8_lossy(&key).to_string();

        let mut value = vec![0u8; value_length as usize];
        TokioAsyncReadExt::read_exact(stream, &mut value).await?;
        let value = String::from_utf8_lossy(&value).to_string();

        Ok(Message {
            code,
            key,
            value,
            not_found: false,
            database_id,
        })
    }

    pub fn not_found_response() -> Self {
        Message {
            code: 0,
            key: String::new(),
            value: String::new(),
            not_found: true,
            database_id: DatabaseId::Default,
        }
    }
}
