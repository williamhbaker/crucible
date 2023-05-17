use std::{error::Error, fmt, io};

pub mod compactor;
pub mod memtable;
pub mod protocol;
pub mod sst;
pub mod store;
pub mod wal;

#[derive(Debug)]
pub enum StoreError {
    WalRecovery(io::Error),
    WalConversion(io::Error),
    WalInitialization(io::Error),
    CatalogInitialization(io::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WalRecovery(_) => write!(f, "Failed to recover existing WAL file from disk."),
            Self::WalConversion(_) => {
                write!(f, "Failed to compact WAL file to a sorted string table.")
            }
            Self::WalInitialization(_) => write!(f, "Failed to create new WAL file."),
            Self::CatalogInitialization(_) => write!(f, "Failed to initialized SST catalog."),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WalRecovery(err) => Some(err),
            Self::WalConversion(err) => Some(err),
            Self::WalInitialization(err) => Some(err),
            Self::CatalogInitialization(err) => Some(err),
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        self.source()
    }
}
