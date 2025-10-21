// suppress all the false alarms by cargo test
// https://github.com/rust-lang/rust/issues/46379
#![allow(dead_code)]

pub mod block_visitor;
pub mod dupocalypse;
pub mod fixture;
pub mod process;
pub mod random;
pub mod targets;
pub mod test_dir;
