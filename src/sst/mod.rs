mod catalog;
mod index;
mod table;

use std::path;

pub use catalog::*;

use index::*;
use table::*;

fn table_sequence(path: &path::Path) -> usize {
    path.file_stem().unwrap().to_string_lossy().parse().unwrap()
}
