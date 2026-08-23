//! Single-instance lock rooted in the canonical Erabi data directory.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Diagnostics persisted with the active data-directory lock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessLockMetadata {
    /// Owning process identifier.
    pub process_id: u32,
    /// Process start timestamp supplied by the runtime.
    pub started_at: String,
    /// Erabi application version.
    pub erabi_version: String,
    /// Verified bind address.
    pub bind_address: String,
}

impl ProcessLockMetadata {
    /// Returns current-process diagnostics without including configuration secrets.
    #[must_use]
    pub fn current(started_at: impl Into<String>, bind_address: impl Into<String>) -> Self {
        Self {
            process_id: std::process::id(),
            started_at: started_at.into(),
            erabi_version: env!("CARGO_PKG_VERSION").to_owned(),
            bind_address: bind_address.into(),
        }
    }

    fn serialize(&self) -> String {
        format!(
            "pid={}\nstarted_at={}\nversion={}\nbind={}\n",
            self.process_id, self.started_at, self.erabi_version, self.bind_address
        )
    }
}

/// Owned exclusive lock file. Dropping it releases the operating-system lock.
#[derive(Debug)]
pub struct ProcessLock {
    file: File,
    path: PathBuf,
}

impl ProcessLock {
    /// Acquires the exclusive lock in an already canonical data directory.
    ///
    /// A stale file is never deleted: an exclusive OS lock can only be
    /// acquired once the prior owner is no longer alive/holding the file lock,
    /// at which point its diagnostics are safely replaced.
    ///
    /// # Errors
    /// Returns contention diagnostics when another process owns the lock, or a
    /// typed I/O failure when the lock cannot be created or updated.
    pub fn acquire(
        canonical_data_dir: &Path,
        metadata: &ProcessLockMetadata,
    ) -> Result<Self, ProcessLockError> {
        let path = canonical_data_dir.join(".erabi-process.lock");
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(ProcessLockError::Io)?;
        if let Err(error) = file.try_lock() {
            let diagnostic =
                read_diagnostic(&mut file).unwrap_or_else(|_| "unavailable".to_owned());
            return Err(ProcessLockError::Contended {
                diagnostic,
                source: error,
            });
        }
        file.set_len(0).map_err(ProcessLockError::Io)?;
        file.seek(SeekFrom::Start(0))
            .map_err(ProcessLockError::Io)?;
        file.write_all(metadata.serialize().as_bytes())
            .map_err(ProcessLockError::Io)?;
        file.sync_all().map_err(ProcessLockError::Io)?;
        Ok(Self { file, path })
    }

    /// Returns the lock path for safe diagnostics.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Process-lock failures that preserve contention evidence.
#[derive(Debug, thiserror::Error)]
pub enum ProcessLockError {
    /// The data directory is already owned by an active process.
    #[error("Erabi data directory is already locked ({diagnostic})")]
    Contended {
        diagnostic: String,
        source: std::fs::TryLockError,
    },
    /// The lock file could not be safely accessed.
    #[error("could not access the Erabi process lock")]
    Io(#[source] std::io::Error),
}

fn read_diagnostic(file: &mut File) -> Result<String, std::io::Error> {
    file.seek(SeekFrom::Start(0))?;
    let mut value = String::new();
    file.read_to_string(&mut value)?;
    Ok(value.replace('\n', "; "))
}
