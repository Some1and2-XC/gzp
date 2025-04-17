//! Parallel compression.
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "deflate")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{par::compress::{ParCompress, ParCompressBuilder}, deflate::Gzip, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParCompress<Gzip> = ParCompressBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```
use std::{
    io::{self, Write}, thread::JoinHandle
};

use bytes::{Bytes, BytesMut};
pub use flate2::Compression;
use flume::{bounded, Receiver, Sender};
use log::warn;

use crate::check::Check;
use crate::{CompressResult, FormatSpec, GzpError, Message, ZWriter, DICT_SIZE};

/// The [`ParCompress`] builder.
#[derive(Debug)]
pub struct ParCompressBuilder<F>
where
    F: FormatSpec,
{
    /// The buffersize accumulate before trying to compress it. Defaults to `F::DEFAULT_BUFSIZE`.
    buffer_size: usize,
    /// The number of threads to use for compression. Defaults to all available threads.
    num_threads: usize,
    /// The compression level of the output, see [`Compression`].
    compression_level: Compression,
    /// The out file format to use.
    format: F,
    /// Whether or not to pin threads to specific cpus and what core to start pins at
    pin_threads: Option<usize>,
    /// The dictionary used to initialize the [`ParCompress`] object with.
    dictionary: Option<Bytes>,
    /// A flag specifying if the encoder should write a header chunk when starting the compressor.
    write_header_on_start: bool,
    /// A flag specifying if the encoder should write a footer chunk automatically when finished.
    write_footer_on_exit: bool,
    /// A checksum supplied to start encoding from.
    checksum: Option<F::C>,
    /// A cell where the check sum gets set once compression is complete.
    checksum_dest: Option<Sender<F::C>>,
}

impl<F> ParCompressBuilder<F>
where
    F: FormatSpec,
{
    /// Create a new [`ParCompressBuilder`] object.
    pub fn new() -> Self {
        Self {
            buffer_size: F::DEFAULT_BUFSIZE,
            num_threads: num_cpus::get(),
            compression_level: Compression::new(3),
            format: F::new(),
            pin_threads: None,
            dictionary: None,
            write_header_on_start: true,
            write_footer_on_exit: true,
            checksum: None,
            checksum_dest: None,
        }
    }

    /// Set the [`buffer_size`](ParCompressBuilder.buffer_size). Must be >= [`DICT_SIZE`].
    ///
    /// # Errors
    /// - [`GzpError::BufferSize`] error if selected buffer size is less than [`DICT_SIZE`].
    pub fn buffer_size(mut self, buffer_size: usize) -> Result<Self, GzpError> {
        if buffer_size < DICT_SIZE {
            return Err(GzpError::BufferSize(buffer_size, DICT_SIZE));
        }
        self.buffer_size = buffer_size;
        Ok(self)
    }

    /// Set the [`num_threads`](ParCompressBuilder.num_threads) that will be used for compression.
    ///
    /// Note that one additional thread will be used for writing. Threads equal to `num_threads`
    /// will be spun up in the background and will remain blocking and waiting for blocks to compress
    /// until ['finish`](ParCompress.finish) is called.
    ///
    /// # Errors
    /// - [`GzpError::NumThreads`] error if 0 threads selected.
    pub fn num_threads(mut self, num_threads: usize) -> Result<Self, GzpError> {
        if num_threads == 0 {
            return Err(GzpError::NumThreads(num_threads));
        }
        self.num_threads = num_threads;
        Ok(self)
    }

    /// Set the [`compression_level`](ParCompressBuilder.compression_level).
    pub fn compression_level(mut self, compression_level: Compression) -> Self {
        self.compression_level = compression_level;
        self
    }

    /// Set the [`pin_threads`](ParCompressBuilder.pin_threads).
    pub fn pin_threads(mut self, pin_threads: Option<usize>) -> Self {
        if core_affinity::get_core_ids().is_none() {
            warn!("Pinning threads is not supported on your platform. Please see core_affinity_rs. No threads will be pinned, but everything will work.");
            self.pin_threads = None;
        } else {
            self.pin_threads = pin_threads;
        }
        self
    }

    /// Set the [`dictionary`](ParCompressBuilder.dictionary).
    pub fn dictionary(mut self, dictionary: Option<Bytes>) -> Self {
        self.dictionary = dictionary;
        self
    }

    /// Set the [`write_header_on_start`](ParCompressBuilder.write_header_on_start).
    pub fn write_header_on_start(mut self, flag: bool) -> Self {
        self.write_header_on_start = flag;
        self
    }


    /// Set the [`write_footer_on_exit`](ParCompressBuilder.write_footer_on_exit).
    pub fn write_footer_on_exit(mut self, flag: bool) -> Self {
        self.write_footer_on_exit = flag;
        self
    }

    /// Set the [`checksum`](ParCompressBuilder.checksum).
    pub fn checksum(mut self, checksum: Option<F::C>) -> Self {
        self.checksum = checksum;
        self
    }

    /// Passes where the check sum should be passed once the compressor is done compressing.
    pub fn checksum_dest(mut self, dest: Option<Sender<F::C>>) -> Self {
        self.checksum_dest = dest;
        self
    }

    /// Creates a channel to be used the [`ParCompressBuilder::checksum_dest`] function.
    pub fn checksum_channel() -> (Sender<F::C>, Receiver<F::C>) {
        return flume::unbounded();
    }

    /// Create a configured [`ParCompress`] object.
    pub fn from_writer<W: Write + Send + 'static>(self, writer: W) -> ParCompress<F> {
        let (tx_compressor, rx_compressor) = bounded(self.num_threads * 2);
        let (tx_writer, rx_writer) = bounded(self.num_threads * 2);
        let buffer_size = self.buffer_size;
        let num_threads = self.num_threads;
        let comp_level = self.compression_level;
        let format = self.format;
        let pin_threads = self.pin_threads;
        let write_header_on_start = self.write_header_on_start;
        let write_footer_on_exit = self.write_footer_on_exit;
        let checksum = self.checksum;
        let checksum_dest = self.checksum_dest;

        if !write_footer_on_exit && checksum_dest.is_none() {
            warn!("If you're not writing a footer it might be a good idea to keep track of the checksum at the end.
You might want to try using the [`ParCompressBuilder::checksum_dest()`] function!")
        }

        let handle = std::thread::spawn(move || {
            ParCompress::run(
                &rx_compressor,
                &rx_writer,
                writer,
                num_threads,
                comp_level,
                format,
                pin_threads,
                write_header_on_start,
                write_footer_on_exit,
                checksum,
                checksum_dest,
            )
        });
        ParCompress {
            handle: Some(handle),
            tx_compressor: Some(tx_compressor),
            tx_writer: Some(tx_writer),
            dictionary: self.dictionary,
            buffer: BytesMut::with_capacity(buffer_size),
            buffer_size,
            format,
            write_header_on_start: self.write_header_on_start,
            write_footer_on_exit: self.write_footer_on_exit,
        }
    }
}

impl<F> Default for ParCompressBuilder<F>
where
    F: FormatSpec,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
pub struct ParCompress<F>
where
    F: FormatSpec,
{
    handle: Option<std::thread::JoinHandle<Result<(), GzpError>>>,
    tx_compressor: Option<Sender<Message<F::C>>>,
    tx_writer: Option<Sender<Receiver<CompressResult<F::C>>>>,
    buffer: BytesMut,
    dictionary: Option<Bytes>,
    buffer_size: usize,
    format: F,
    write_header_on_start: bool,
    write_footer_on_exit: bool,
}

impl<F> ParCompress<F>
where
    F: FormatSpec,
{
    /// Create a builder to configure the [`ParCompress`] runtime.
    pub fn builder() -> ParCompressBuilder<F> {
        ParCompressBuilder::new()
    }

    /// Launch threads to compress chunks and coordinate sending compressed results
    /// to the writer.
    #[allow(clippy::needless_collect)]
    fn run<W>(
        rx: &Receiver<Message<F::C>>,
        rx_writer: &Receiver<Receiver<CompressResult<F::C>>>,
        mut writer: W,
        num_threads: usize,
        compression_level: Compression,
        format: F,
        pin_threads: Option<usize>,
        write_header_on_start: bool,
        write_footer_on_exit: bool,
        checksum: Option<F::C>,
        checksum_dest: Option<Sender<F::C>>,
    ) -> Result<(), GzpError>
    where
        W: Write + Send + 'static,
    {
        let (core_ids, pin_threads) = if let Some(core_ids) = core_affinity::get_core_ids() {
            (core_ids, pin_threads)
        } else {
            // Handle the case where core affinity doesn't work for a platform.
            // We test and warn in the constructors for this case, so no warning should be needed here.
            (vec![], None)
        };

        let handles: Vec<JoinHandle<Result<(), GzpError>>> = (0..num_threads)
            .map(|i| {
                let rx = rx.clone();
                let core_ids = core_ids.clone();
                std::thread::spawn(move || -> Result<(), GzpError> {
                    if let Some(pin_at) = pin_threads {
                        if let Some(id) = core_ids.get(pin_at + i) {
                            core_affinity::set_for_current(*id);
                        }
                    }

                    let mut compressor = format.create_compressor(compression_level)?;
                    while let Ok(m) = rx.recv() {
                        let chunk = &m.buffer;
                        let buffer = format.encode(
                            chunk,
                            &mut compressor,
                            compression_level,
                            m.dictionary.as_ref(),
                            m.is_last,
                        )?;
                        let mut check = F::create_check();
                        check.update(chunk);

                        m.oneshot
                            .send(Ok::<(F::C, Vec<u8>), GzpError>((check, buffer)))
                            .map_err(|_e| GzpError::ChannelSend)?;
                    }
                    Ok(())
                })
            })
            // This collect is needed to force the evaluation, otherwise this thread will block on writes waiting
            // for data to show up that will never come since the iterator is lazy.
            .collect();

        // Writer
        if write_header_on_start {
            writer.write_all(&format.header(compression_level))?;
        }

        let mut running_check = match checksum {
            Some(v) => v,
            None => F::create_check(),
        };

        while let Ok(chunk_chan) = rx_writer.recv() {
            let chunk_chan: Receiver<CompressResult<F::C>> = chunk_chan;
            let (check, chunk) = chunk_chan.recv()??;
            running_check.combine(&check);
            writer.write_all(&chunk)?;
        }

        if write_footer_on_exit {
            let footer = format.footer(&running_check);
            writer.write_all(&footer)?;
        }
        writer.flush()?;

        // Tries to send the checksum to its destination but prints the result instead if
        // the destination is already set.
        if let Some(dest) = checksum_dest.as_ref() {
            match dest.send(running_check) {
                Ok(v) => v,
                Err(e) => {
                    let checksum = e.into_inner();
                    warn!("GZP Error: Failed to send checksum to its destination! Check Sum: `{}` & Amount: `{}`.", checksum.sum(), checksum.amount());
                },
            };
        }

        // Gracefully shutdown the compression threads
        handles
            .into_iter()
            .try_for_each(|handle| match handle.join() {
                Ok(result) => result,
                Err(e) => std::panic::resume_unwind(e),
            })

    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    ///
    /// If this is the last buffer to be sent, set `is_last` to false to trigger compression
    /// stream completion.
    ///
    /// # Panics
    /// - If called after `finish`
    fn flush_last(&mut self, is_last: bool) -> std::io::Result<()> {
        loop {
            let b = self
                .buffer
                .split_to(std::cmp::min(self.buffer.len(), self.buffer_size))
                .freeze();
            let (mut m, r) = Message::new_parts(b, self.dictionary.take());
            if is_last && self.buffer.is_empty() {
                m.is_last = true;
            }

            if m.buffer.len() >= DICT_SIZE && !m.is_last && self.format.needs_dict() {
                self.dictionary = Some(m.buffer.slice(m.buffer.len() - DICT_SIZE..));
            }

            self.tx_writer
                .as_ref()
                .unwrap()
                .send(r)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            self.tx_compressor
                .as_ref()
                .unwrap()
                .send(m)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            if self.buffer.is_empty() {
                break;
            }
        }
        Ok(())
    }

    /// Returns the dictionary as bytes from the encoder.
    pub fn get_dict(&self) -> Option<&Bytes> {
        self.dictionary.as_ref()
    }

}

impl<F> ZWriter for ParCompress<F>
where
    F: FormatSpec,
{
    /// Flush the buffers and wait on all threads to finish working.
    ///
    /// This *MUST* be called before the [`ParCompress`] object goes out of scope.
    ///
    /// # Errors
    /// - [`GzpError`] if there is an issue flushing the last blocks or an issue joining on the writer thread
    ///
    /// # Panics
    /// - If called twice
    fn finish(&mut self) -> Result<(), GzpError> {

        self.flush_last(self.write_footer_on_exit)?;

        // while !self.tx_compressor.as_ref().unwrap().is_empty() {}
        // while !self.tx_writer.as_ref().unwrap().is_empty() {}
        drop(self.tx_compressor.take());
        drop(self.tx_writer.take());
        match self.handle.take().unwrap().join() {
            Ok(result) => result,
            Err(e) => std::panic::resume_unwind(e),
        }
    }
}

impl<F> Drop for ParCompress<F>
where
    F: FormatSpec,
{
    fn drop(&mut self) {
        if self.tx_compressor.is_some() && self.tx_writer.is_some() && self.handle.is_some() {
            self.finish().unwrap();
        }
        // Resources already cleaned up if channels and handle are None
    }
}

impl<F> Write for ParCompress<F>
where
    F: FormatSpec,
{
    /// Write a buffer into this writer, returning how many bytes were written.
    ///
    /// # Panics
    /// - If called after calling `finish`
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while self.buffer.len() > self.buffer_size {
            let b = self.buffer.split_to(self.buffer_size).freeze();
            let (m, r) = Message::new_parts(b, self.dictionary.take());
            // Bytes uses and ARC, this is O(1) to get the last 32k bytes from teh previous chunk
            self.dictionary = if self.format.needs_dict() {
                Some(m.buffer.slice(m.buffer.len() - DICT_SIZE..))
            } else {
                None
            };
            self.tx_writer
                .as_ref()
                .unwrap()
                .send(r)
                .map_err(|_send_error| {
                    // If an error occured sending, that means the recievers have dropped an the compressor thread hit an error
                    // Collect that error here, and if it was an Io error, preserve it
                    let error = match self.handle.take().unwrap().join() {
                        Ok(result) => result,
                        Err(e) => std::panic::resume_unwind(e),
                    };
                    match error {
                        Ok(()) => std::panic::resume_unwind(Box::new(error)), // something weird happened
                        Err(GzpError::Io(ioerr)) => ioerr,
                        Err(err) => io::Error::new(io::ErrorKind::Other, err),
                    }
                })?;
            self.tx_compressor
                .as_ref()
                .unwrap()
                .send(m)
                .map_err(|_send_error| {
                    // If an error occured sending, that means the recievers have dropped an the compressor thread hit an error
                    // Collect that error here, and if it was an Io error, preserve it
                    let error = match self.handle.take().unwrap().join() {
                        Ok(result) => result,
                        Err(e) => std::panic::resume_unwind(e),
                    };
                    match error {
                        Ok(()) => std::panic::resume_unwind(Box::new(error)), // something weird happened
                        Err(GzpError::Io(ioerr)) => ioerr,
                        Err(err) => io::Error::new(io::ErrorKind::Other, err),
                    }
                })?;
            self.buffer
                .reserve(self.buffer_size.saturating_sub(self.buffer.len()));
        }

        Ok(buf.len())
    }

    /// Flush this output stream, ensuring all intermediately buffered contents are sent.
    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_last(false)
    }
}
