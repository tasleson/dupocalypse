use anyhow::{anyhow, ensure, Context, Result};
use io::prelude::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Take;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::run_iter::*;

//-----------------------------------------

#[derive(Debug)]
pub enum Chunk {
    Mapped(Vec<u8>),
    Unmapped(u64),

    // Reference a prior stream
    Ref(u64),
}

pub struct ThickChunker {
    input: Take<File>,
    input_size: u64,
    total_read: u64,
    block_size: u64,
}

impl ThickChunker {
    pub fn new(input_path: &Path, block_size: u64) -> Result<Self> {
        let input_size = thinp::file_utils::file_size(input_path)?;
        let input = OpenOptions::new()
            .read(true)
            .write(false)
            .open(input_path)
            .context("couldn't open input file/dev")?
            .take(0);

        Ok(Self {
            input,
            input_size,
            total_read: 0,
            block_size,
        })
    }

    fn do_read(&mut self, size: u64) -> Result<Option<Chunk>> {
        let mut buffer = Vec::with_capacity(size as usize);
        self.input.set_limit(size);
        let read_size = self.input.read_to_end(&mut buffer)?;
        ensure!(read_size == size as usize, "short read");
        self.total_read += buffer.len() as u64;
        Ok(Some(Chunk::Mapped(buffer)))
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let remaining = self.input_size - self.total_read;

        if remaining == 0 {
            Ok(None)
        } else if remaining >= self.block_size {
            self.do_read(self.block_size)
        } else {
            self.do_read(remaining)
        }
    }
}

impl Iterator for ThickChunker {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

//-----------------------------------------

pub struct ThinChunker {
    input: File,
    run_iter: RunIter,
    data_block_size: u64,

    max_read_size: usize,
    current_run: Option<(bool, Range<u64>)>,
}

impl ThinChunker {
    pub fn new(input: File, run_iter: RunIter, data_block_size: u64) -> Self {
        Self {
            input,
            run_iter,
            data_block_size,

            max_read_size: 16 * 1024 * 1024,
            current_run: None,
        }
    }

    fn next_run_bytes(&mut self) -> Option<(bool, Range<u64>)> {
        self.run_iter.next().map(|(b, Range { start, end })| {
            (
                b,
                Range {
                    start: start as u64 * self.data_block_size,
                    end: end as u64 * self.data_block_size,
                },
            )
        })
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut run = None;
        std::mem::swap(&mut run, &mut self.current_run);

        match run.or_else(|| self.next_run_bytes()) {
            Some((false, run)) => Ok(Some(Chunk::Unmapped(run.end - run.start))),
            Some((true, run)) => {
                let run_len = run.end - run.start;
                if run_len <= self.max_read_size as u64 {
                    let mut buf = vec![0; run_len as usize];
                    self.input
                        .read_exact_at(&mut buf, run.start)
                        .with_context(|| {
                            format!(
                                "ThinChuncker:failed to read {} bytes at offset {}",
                                run_len, run.start
                            )
                        })?;
                    Ok(Some(Chunk::Mapped(buf)))
                } else {
                    let mut buf = vec![0; self.max_read_size];
                    self.input
                        .read_exact_at(&mut buf, run.start)
                        .with_context(|| {
                            format!(
                                "ThinChuncker:failed to read {} bytes at offset {}",
                                self.max_read_size, run.start
                            )
                        })?;
                    self.current_run = Some((true, (run.start + buf.len() as u64)..run.end));
                    Ok(Some(Chunk::Mapped(buf)))
                }
            }
            None => Ok(None),
        }
    }
}

impl Iterator for ThinChunker {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

//-----------------------------------------

pub struct DeltaChunker {
    input: File,
    deltas: DualIter,
    data_block_size: u64,

    max_read_size: usize,
    current_run: Option<(DualType, Range<u64>)>,
}

impl DeltaChunker {
    pub fn new(input: File, deltas: DualIter, data_block_size: u64) -> Self {
        Self {
            input,
            deltas,
            data_block_size,

            max_read_size: 16 * 1024 * 1024,
            current_run: None,
        }
    }

    // FIXME: removals are being ignored
    fn next_run_bytes(&mut self) -> Option<(DualType, Range<u64>)> {
        self.deltas.next().map(|(t, Range { start, end })| {
            (
                t,
                Range {
                    start: start as u64 * self.data_block_size,
                    end: end as u64 * self.data_block_size,
                },
            )
        })
    }

    fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        let mut run = None;
        std::mem::swap(&mut run, &mut self.current_run);

        match run.or_else(|| self.next_run_bytes()) {
            Some((DualType::Left, run)) => {
                // Addition
                let run_len = run.end - run.start;
                if run_len <= self.max_read_size as u64 {
                    let mut buf = vec![0; run_len as usize];
                    self.input.read_exact_at(&mut buf, run.start)
                        .with_context(|| format!("DeltaChunker:failed to read {} bytes at offset {} (delta addition)", run_len, run.start))?;
                    Ok(Some(Chunk::Mapped(buf)))
                } else {
                    let mut buf = vec![0; self.max_read_size];
                    self.input.read_exact_at(&mut buf, run.start)
                        .with_context(|| format!("DeltaChunker:failed to read {} bytes at offset {} (delta addition)", self.max_read_size, run.start))?;
                    self.current_run =
                        Some((DualType::Left, (run.start + buf.len() as u64)..run.end));
                    Ok(Some(Chunk::Mapped(buf)))
                }
            }
            Some((DualType::Right, run)) => {
                // Removal
                Ok(Some(Chunk::Unmapped(run.end - run.start)))
            }
            Some((DualType::Both, ..)) => Err(anyhow!(
                "internal error: region can't be both an addition and removal"
            )),
            Some((DualType::Neither, run)) => Ok(Some(Chunk::Ref(run.end - run.start))),
            None => Ok(None),
        }
    }
}

impl Iterator for DeltaChunker {
    type Item = Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

//-----------------------------------------
