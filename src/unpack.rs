use anyhow::{anyhow, Context, Result};
use chrono::prelude::*;
use clap::ArgMatches;
use io::{Read, Seek, Write};
use serde_json::json;
use serde_json::to_string_pretty;
use size_display::Size;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::archive;
use crate::archive::SLAB_SIZE_TARGET;
use crate::chunkers::*;
use crate::config;
use crate::output::Output;
use crate::paths::*;
use crate::run_iter::*;
use crate::slab::builder::*;
use crate::slab::*;
use crate::stream;
use crate::stream::*;
use crate::thin_metadata::*;
use crate::utils::unmapped_digest_add;

//-----------------------------------------

// Unpack and verify do different things with the data.
trait UnpackDest {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()>;
    fn handle_unmapped(&mut self, len: u64) -> Result<()>;
    fn complete(&mut self) -> Result<String>;
}

struct Unpacker<D: UnpackDest, S: SlabStorage = SlabFile<'static>> {
    stream_file: SlabFile<'static>,
    archive: archive::Data<'static, S>,
    dest: D,
}

impl<D: UnpackDest> Unpacker<D, MultiFile> {
    // Assumes current directory is the root of the archive.
    fn new(archive_dir: &Path, stream: &str, cache_nr_entries: usize, dest: D) -> Result<Self> {
        let data_file = MultiFile::open_for_read(archive_dir, cache_nr_entries)?;
        let hashes_file = Arc::new(Mutex::new(
            SlabFileBuilder::open(hashes_path(archive_dir)).build()?,
        ));
        let stream_file = SlabFileBuilder::open(stream_path(archive_dir, stream)).build()?;

        Ok(Self {
            stream_file,
            archive: archive::Data::new(
                &PathBuf::from(archive_dir),
                data_file,
                hashes_file,
                cache_nr_entries,
            )?,
            dest,
        })
    }
}

impl<D: UnpackDest, S: SlabStorage> Unpacker<D, S> {
    fn unpack_entry(&mut self, e: &MapEntry) -> Result<()> {
        use MapEntry::*;
        match e {
            Fill { byte, len } => {
                // len may be very big, so we have to be prepared to write in chunks.
                // FIXME: if we're writing to a file would this be zeroes anyway?  fallocate?
                const MAX_BUFFER: u64 = 16 * 1024 * 1024;
                let mut written = 0;
                while written < *len {
                    let write_len = std::cmp::min(*len - written, MAX_BUFFER);
                    let bytes: Vec<u8> = vec![*byte; write_len as usize];
                    self.dest.handle_mapped(&bytes[..])?;
                    written += write_len;
                }
            }
            Unmapped { len } => {
                self.dest.handle_unmapped(*len)?;
            }
            Data {
                slab,
                offset,
                nr_entries,
            } => {
                let (data, start, end) =
                    self.archive.data_get(*slab, *offset, *nr_entries, None)?;
                self.dest.handle_mapped(&data[start..end])?;
            }
            Partial {
                begin,
                end,
                slab,
                offset,
                nr_entries,
            } => {
                let partial = Some((*begin, *end));
                let (data, start, end) =
                    self.archive
                        .data_get(*slab, *offset, *nr_entries, partial)?;
                self.dest.handle_mapped(&data[start..end])?;
            }
            Ref { .. } => {
                // Can't get here.
                return Err(anyhow!("unexpected MapEntry::Ref (shouldn't be possible)"));
            }
        }

        Ok(())
    }

    fn unpack(&mut self, output: Arc<Output>, total: u64) -> Result<String> {
        output.report.progress(0);

        let nr_slabs = self.stream_file.get_nr_slabs();
        let mut unpacker = stream::MappingUnpacker::default();

        let start_time: DateTime<Utc> = Utc::now();

        for s in 0..nr_slabs {
            let stream_data = self.stream_file.read(s as u32)?;
            let (entries, _positions) = unpacker.unpack(&stream_data[..])?;
            let nr_entries = entries.len();

            for (i, e) in entries.iter().enumerate() {
                self.unpack_entry(e)?;

                if i % 1024 == 0 {
                    // update progress bar
                    let entry_fraction = i as f64 / nr_entries as f64;
                    let slab_fraction = s as f64 / nr_slabs as f64;
                    let percent =
                        ((slab_fraction + (entry_fraction / nr_slabs as f64)) * 100.0) as u8;
                    output.report.progress(percent);
                }
            }
        }

        let result = self.dest.complete()?;
        output.report.progress(100);
        let end_time: DateTime<Utc> = Utc::now();
        let elapsed = end_time - start_time;
        let elapsed = elapsed.num_milliseconds() as f64 / 1000.0;

        if output.json {
            let result = json!({ "bytes_per_second": (total as f64 / elapsed) as u64 });
            println!("{}", to_string_pretty(&result).unwrap());
        } else {
            output.report.info(&format!(
                "speed            : {:.2}/s",
                Size((total as f64 / elapsed) as u64)
            ));
        }

        Ok(result)
    }
}

//-----------------------------------------

struct ThickDest<W: Write> {
    output: W,
    digest: blake3::Hasher,
}

fn write_bytes<W: Write>(w: &mut W, byte: u8, len: u64, digest: &mut blake3::Hasher) -> Result<()> {
    let buf_size = std::cmp::min(len, 64 * 1024 * 1024);
    let buf = vec![byte; buf_size as usize];

    let mut remaining = len;
    while remaining > 0 {
        let w_len = std::cmp::min(buf_size, remaining);

        digest.update(&buf[0..(w_len as usize)]);
        w.write_all(&buf[0..(w_len as usize)])?;
        remaining -= w_len;
    }

    Ok(())
}

impl<W: Write> UnpackDest for ThickDest<W> {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        self.digest.update(data);
        self.output.write_all(data)?;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        write_bytes(&mut self.output, 0, len, &mut self.digest)
    }

    fn complete(&mut self) -> Result<String> {
        Ok(self.digest.finalize().to_hex().to_string())
    }
}

#[derive(Default)]
struct ValidateStream {
    digest: blake3::Hasher,
}

impl ValidateStream {
    fn digest_bytes(&mut self, byte: u8, len: u64) {
        let buf_size = std::cmp::min(len, 64 * 1024 * 1024);
        let buf = vec![byte; buf_size as usize];
        let mut remaining = len;
        while remaining > 0 {
            let w_len = std::cmp::min(buf_size, remaining);
            self.digest.update(&buf[0..(w_len as usize)]);
            remaining -= w_len;
        }
    }
}

impl UnpackDest for ValidateStream {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        self.digest.update(data);
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        self.digest_bytes(0, len);
        Ok(())
    }

    fn complete(&mut self) -> Result<String> {
        Ok(self.digest.finalize().to_hex().to_string())
    }
}

//-----------------------------------------

// defined in include/uapi/linux/fs.h
const BLK_IOC_CODE: u8 = 0x12;
const BLKDISCARD_SEQ: u8 = 119;
nix::ioctl_write_ptr_bad!(
    ioctl_blkdiscard,
    nix::request_code_none!(BLK_IOC_CODE, BLKDISCARD_SEQ),
    [u64; 2]
);

struct ThinDest {
    block_size: u64,
    output: File,
    pos: u64,
    provisioned: RunIter,

    // (provisioned, len bytes)
    run: Option<(bool, u64)>,
    writes_avoided: u64,
    digest: blake3::Hasher,
}

impl ThinDest {
    fn issue_discard(&mut self, len: u64) -> Result<()> {
        let begin = self.pos;
        let end = begin + len;

        // Discards should always be block aligned
        assert_eq!(begin % self.block_size, 0);
        assert_eq!(end % self.block_size, 0);

        unsafe {
            ioctl_blkdiscard(self.output.as_raw_fd(), &[begin, len])?;
        }

        Ok(())
    }

    //------------------

    // These low level io functions update the position.
    fn forward(&mut self, len: u64) -> Result<()> {
        self.output.seek(std::io::SeekFrom::Current(len as i64))?;
        self.pos += len;
        Ok(())
    }

    fn rewind(&mut self, len: u64) -> Result<()> {
        self.output
            .seek(std::io::SeekFrom::Current(-(len as i64)))?;
        self.pos -= len;
        Ok(())
    }

    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.output.write_all(data)?;
        self.pos += data.len() as u64;
        Ok(())
    }

    fn discard(&mut self, len: u64) -> Result<()> {
        self.issue_discard(len)?;
        self.forward(len)?;
        Ok(())
    }

    fn read(&mut self, len: u64) -> Result<Vec<u8>> {
        let mut buf = vec![0; len as usize];
        self.output.read_exact(&mut buf[..]).with_context(|| {
            format!(
                "ThinDest:failed to read {} bytes from output at position {}",
                len, self.pos
            )
        })?;
        self.pos += len;
        Ok(buf)
    }

    //------------------
    fn handle_mapped_unprovisioned(&mut self, data: &[u8]) -> Result<()> {
        self.write(data)
    }

    fn handle_mapped_provisioned(&mut self, data: &[u8]) -> Result<()> {
        let actual = self.read(data.len() as u64)?;
        if actual == data {
            self.writes_avoided += data.len() as u64;
        } else {
            self.rewind(data.len() as u64)?;
            self.write(data)?;
        }

        Ok(())
    }

    fn handle_unmapped_unprovisioned(&mut self, len: u64) -> Result<()> {
        self.forward(len)
    }

    fn handle_unmapped_provisioned(&mut self, len: u64) -> Result<()> {
        self.discard(len)
    }

    fn ensure_run(&mut self) -> Result<()> {
        if self.run.is_none() {
            match self.provisioned.next() {
                Some((provisioned, run)) => {
                    self.run = Some((provisioned, (run.end - run.start) as u64 * self.block_size));
                }
                None => {
                    return Err(anyhow!("internal error: out of runs"));
                }
            }
        }

        Ok(())
    }

    fn next_run(&mut self, max_len: u64) -> Result<(bool, u64)> {
        self.ensure_run()?;
        let (provisioned, run_len) = self.run.take().unwrap();
        if run_len <= max_len {
            Ok((provisioned, run_len))
        } else {
            self.run = Some((provisioned, run_len - max_len));
            Ok((provisioned, max_len))
        }
    }
}

impl UnpackDest for ThinDest {
    fn handle_mapped(&mut self, data: &[u8]) -> Result<()> {
        let mut remaining = data.len() as u64;
        let mut offset = 0;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            self.digest
                .update(&data[offset as usize..(offset + c_len) as usize]);

            if provisioned {
                self.handle_mapped_provisioned(&data[offset as usize..(offset + c_len) as usize])?;
            } else {
                self.handle_mapped_unprovisioned(
                    &data[offset as usize..(offset + c_len) as usize],
                )?;
            }

            remaining -= c_len;
            offset += c_len;
        }

        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let (provisioned, c_len) = self.next_run(remaining)?;

            unmapped_digest_add(&mut self.digest, c_len);

            if provisioned {
                self.handle_unmapped_provisioned(c_len)?;
            } else {
                self.handle_unmapped_unprovisioned(c_len)?;
            }

            remaining -= c_len;
        }

        Ok(())
    }

    fn complete(&mut self) -> Result<String> {
        assert!(self.run.is_none());
        assert!(self.provisioned.next().is_none());
        Ok(self.digest.finalize().to_hex().to_string())
    }
}

//-----------------------------------------

pub fn run_unpack(matches: &ArgMatches, report_output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap())
        .canonicalize()
        .context("Bad archive dir")?;
    let output_file = Path::new(matches.get_one::<String>("OUTPUT").unwrap());
    let stream = matches.get_one::<String>("STREAM").unwrap();
    let create = matches.get_flag("CREATE");

    let output = if create {
        fs::OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(output_file)
            .context("Couldn't open output")?
    } else {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(output_file)
            .context("Couldn't open output")?
    };

    let stream_cfg = config::read_stream_config(&archive_dir, stream)?;

    report_output
        .report
        .set_title(&format!("Unpacking {} ...", output_file.display()));
    let result = if create {
        let config = config::read_config(&archive_dir, matches)?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        let dest = ThickDest {
            output,
            digest: blake3::Hasher::new(),
        };
        let mut u = Unpacker::new(&archive_dir, stream, cache_nr_entries, dest)?;
        u.unpack(report_output, stream_cfg.size)
    } else {
        // Check the size matches the stream size.
        let stream_size = stream_cfg.size;
        let output_size = thinp::file_utils::file_size(output_file)?;
        if output_size != stream_size {
            return Err(anyhow!("Destination size doesn't not match stream size"));
        }

        let config = config::read_config(&archive_dir, matches)?;
        let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

        if is_thin_device(output_file)? {
            let mappings = read_thin_mappings(output_file)?;
            let block_size = mappings.data_block_size as u64 * 512;
            let provisioned = RunIter::new(
                mappings.provisioned_blocks,
                (output_size / block_size) as u32,
            );

            let dest = ThinDest {
                block_size,
                output,
                pos: 0,
                provisioned,
                run: None,
                writes_avoided: 0,
                digest: blake3::Hasher::new(),
            };
            let mut u = Unpacker::new(&archive_dir, stream, cache_nr_entries, dest)?;
            u.unpack(report_output, stream_size)
        } else {
            let dest = ThickDest {
                output,
                digest: blake3::Hasher::new(),
            };
            let mut u = Unpacker::new(&archive_dir, stream, cache_nr_entries, dest)?;
            u.unpack(report_output, stream_size)
        }
    };
    compare_hashes(stream_cfg.source_sig, result?)?;
    Ok(())
}

fn run_verify_stream(
    archive_dir: &Path,
    stream_id: &str,
    report_output: Arc<Output>,
    ouput_size: u64,
    cache_nr_entries: usize,
) -> Result<String> {
    report_output.report.set_title(&format!(
        "Validating {stream_id} with stored blake3 hash ..."
    ));

    let dest = ValidateStream {
        digest: blake3::Hasher::new(),
    };
    let mut u = Unpacker::new(archive_dir, stream_id, cache_nr_entries, dest)?;
    u.unpack(report_output, ouput_size)
}

//-----------------------------------------

struct VerifyDest {
    input_it: Box<dyn Iterator<Item = Result<Chunk>>>,
    chunk: Option<Chunk>,
    chunk_offset: u64,
    total_verified: u64,
    digest: blake3::Hasher,
}

impl VerifyDest {
    fn new(input_it: Box<dyn Iterator<Item = Result<Chunk>>>) -> Self {
        Self {
            input_it,
            chunk: None,
            chunk_offset: 0,
            total_verified: 0,
            digest: blake3::Hasher::new(),
        }
    }
}

impl VerifyDest {
    fn fail(&self, msg: &str) -> anyhow::Error {
        anyhow!(format!(
            "verify failed at offset ~{}: {}",
            self.total_verified, msg
        ))
    }

    fn ensure_chunk(&mut self) -> Result<()> {
        if self.chunk.is_none() {
            match self.input_it.next() {
                Some(rc) => {
                    self.chunk = Some(rc?);
                    self.chunk_offset = 0;
                    Ok(())
                }
                None => Err(self.fail("archived stream is longer than expected")),
            }
        } else {
            Ok(())
        }
    }

    fn peek_data(&mut self, max_len: u64) -> Result<&[u8]> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let len = std::cmp::min(bytes.len() - self.chunk_offset as usize, max_len as usize);
                Ok(&bytes[self.chunk_offset as usize..(self.chunk_offset + len as u64) as usize])
            }
            Some(Chunk::Unmapped(_)) => Err(self.fail("expected data, got unmapped")),
            Some(Chunk::Ref(_)) => Err(self.fail("expected data, got ref")),
            None => Err(self.fail("ensure_chunk() failed")),
        }
    }

    fn consume_data(&mut self, len: u64) -> Result<()> {
        use std::cmp::Ordering::*;
        match &self.chunk {
            Some(Chunk::Mapped(bytes)) => {
                let c_len = bytes.len() as u64 - self.chunk_offset;
                match c_len.cmp(&len) {
                    Less => {
                        return Err(self.fail("bad consume, chunk too short"));
                    }
                    Greater => {
                        self.chunk_offset += len;
                    }
                    Equal => {
                        self.chunk = None;
                    }
                }
            }
            Some(Chunk::Unmapped(_)) => {
                return Err(self.fail("bad consume, unexpected unmapped chunk"));
            }
            Some(Chunk::Ref(_)) => {
                return Err(self.fail("bad consume, unexpected ref chunk"));
            }
            None => {
                return Err(self.fail("archived stream longer than input"));
            }
        }
        Ok(())
    }

    fn get_unmapped(&mut self, max_len: u64) -> Result<u64> {
        self.ensure_chunk()?;
        match &self.chunk {
            Some(Chunk::Mapped(_)) => Err(self.fail("expected unmapped, got data")),
            Some(Chunk::Unmapped(len)) => {
                let len = *len;
                if len <= max_len {
                    self.chunk = None;
                    Ok(len)
                } else {
                    self.chunk = Some(Chunk::Unmapped(len - max_len));
                    Ok(max_len)
                }
            }
            Some(Chunk::Ref(_)) => Err(self.fail("unexpected Ref")),
            None => Err(self.fail("archived stream longer than input")),
        }
    }
}

impl UnpackDest for VerifyDest {
    fn handle_mapped(&mut self, expected: &[u8]) -> Result<()> {
        let mut remaining = expected.len() as u64;
        let mut offset: u64 = 0;

        while remaining > 0 {
            // Borrow from `self` in limited scope
            let (actual_len, equal) = {
                let actual = self.peek_data(remaining)?;
                let actual_len = actual.len() as u64;

                let start = offset as usize;
                let end = start + actual_len as usize;
                let expected_slice = &expected[start..end];

                (actual_len, actual == expected_slice)
            }; // borrow ends here

            if !equal {
                return Err(self.fail("data mismatch"));
            }

            // Safe to mutably borrow `self` now to update the digest
            let start = offset as usize;
            let end = start + actual_len as usize;
            self.digest.update(&expected[start..end]);

            self.consume_data(actual_len)?;
            remaining -= actual_len;
            offset += actual_len;
        }

        self.total_verified += expected.len() as u64;
        Ok(())
    }

    fn handle_unmapped(&mut self, len: u64) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
            let len = self.get_unmapped(remaining)?;
            remaining -= len;
        }

        unmapped_digest_add(&mut self.digest, len);

        self.total_verified += len;
        Ok(())
    }

    fn complete(&mut self) -> Result<String> {
        if self.chunk.is_some() || self.input_it.next().is_some() {
            return Err(anyhow!("archived stream is too short"));
        }

        Ok(self.digest.finalize().to_hex().to_string())
    }
}

fn thick_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input_it = Box::new(ThickChunker::new(input_file, 16 * 1024 * 1024)?);
    Ok(VerifyDest::new(input_it))
}

fn thin_verifier(input_file: &Path) -> Result<VerifyDest> {
    let input = OpenOptions::new()
        .read(true)
        .write(false)
        .open(input_file)
        .context("couldn't open input file/dev")?;
    let input_size = thinp::file_utils::file_size(input_file)?;
    let mappings = read_thin_mappings(input_file)?;

    // FIXME: what if input_size is not a multiple of the block size?
    let run_iter = RunIter::new(
        mappings.provisioned_blocks,
        (input_size / (mappings.data_block_size as u64 * 512)) as u32,
    );
    let input_it = Box::new(ThinChunker::new(
        input,
        run_iter,
        mappings.data_block_size as u64 * 512,
    ));

    Ok(VerifyDest::new(input_it))
}

fn run_verify_device_or_file(
    archive_dir: &Path,
    input_file: PathBuf,
    output: Arc<Output>,
    stream_id: &str,
    cache_nr_entries: usize,
    size: u64,
) -> Result<String> {
    output.report.set_title(&format!(
        "Verifying {} and {} match ...",
        input_file.display(),
        &stream_id
    ));

    let dest = if is_thin_device(&input_file)? {
        thin_verifier(&input_file)?
    } else {
        thick_verifier(&input_file)?
    };

    let mut u = Unpacker::new(archive_dir, stream_id, cache_nr_entries, dest)?;
    u.unpack(output, size)
}

fn compare_hashes(stored: Option<String>, calculated_digest: String) -> Result<()> {
    if let Some(stored) = stored {
        if stored != calculated_digest {
            return Err(anyhow!(
                    "Hash signatures do not match \n{stored} stored \n{calculated_digest} calculated, unpacked data not correct!"
                ));
        }
    } else {
        panic!("We should always have a stored hash signature");
    }
    Ok(())
}

pub fn run_verify(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let input_file = if matches.contains_id("INPUT") {
        let p = Path::new(matches.get_one::<String>("INPUT").unwrap());
        p.canonicalize()
            .with_context(|| format!("The canonicalize is failing for path {p:?}"))?
    } else {
        PathBuf::new()
    };

    let config = config::read_config(&archive_dir, matches)?;
    let cache_nr_entries = (1024 * 1024 * config.data_cache_size_meg) / SLAB_SIZE_TARGET;

    let stream = matches.get_one::<String>("STREAM").unwrap();
    let stream_cfg = config::read_stream_config(&archive_dir, stream)?;
    let stored_hash = stream_cfg.source_sig;

    let calculated_hash = if matches.get_flag("internal") {
        run_verify_stream(
            &archive_dir,
            stream,
            output,
            stream_cfg.size,
            cache_nr_entries,
        )?
    } else {
        run_verify_device_or_file(
            &archive_dir,
            input_file,
            output,
            stream,
            cache_nr_entries,
            stream_cfg.size,
        )?
    };

    compare_hashes(stored_hash, calculated_hash)?;
    Ok(())
}

//-----------------------------------------
