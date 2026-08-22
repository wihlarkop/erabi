//! Atomic, controlled-root filesystem storage for large artifacts.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

use erabi_domain::ArtifactId;
use sha2::{Digest, Sha256};

/// Metadata returned after an artifact has been atomically published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredArtifact {
    pub id: ArtifactId,
    pub content_hash: String,
    pub byte_size: u64,
    pub safe_relative_path: PathBuf,
}

/// Errors from the controlled `ArtifactStore` boundary.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactStoreError {
    #[error("artifact storage I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("unsafe artifact path {path:?}: {reason}")]
    UnsafePath { path: PathBuf, reason: &'static str },
    #[error("artifact byte count overflowed u64")]
    SizeOverflow,
}

/// An artifact store rooted at one canonical, non-symlinked directory.
#[derive(Clone, Debug)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    /// Opens a controlled artifact root, creating it when necessary.
    ///
    /// # Errors
    /// Returns an error when the root cannot be created, canonicalized, or is a
    /// symbolic link.
    pub fn new(root: impl AsRef<Path>) -> Result<Self, ArtifactStoreError> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        if fs::symlink_metadata(root)?.file_type().is_symlink() {
            return Err(unsafe_path(
                root,
                "artifact root must not be a symbolic link",
            ));
        }
        Ok(Self {
            root: root.canonicalize()?,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Writes bytes through the same atomic publication path as streaming writes.
    ///
    /// # Errors
    /// Returns an error for unsafe paths, interrupted I/O, or an unsuccessful
    /// atomic publish. Failed temporary files are removed before return.
    pub fn write_bytes(
        &self,
        relative_directory: impl AsRef<Path>,
        file_name: &str,
        bytes: &[u8],
    ) -> Result<StoredArtifact, ArtifactStoreError> {
        self.write(relative_directory, file_name, io::Cursor::new(bytes))
    }

    /// Streams an artifact to a temporary file, syncs it, and atomically publishes it.
    ///
    /// # Errors
    /// Returns an error for unsafe paths, interrupted reads/writes, or an
    /// unsuccessful atomic publish. Failed temporary files are removed before
    /// return.
    pub fn write<R>(
        &self,
        relative_directory: impl AsRef<Path>,
        file_name: &str,
        mut reader: R,
    ) -> Result<StoredArtifact, ArtifactStoreError>
    where
        R: Read,
    {
        let relative_directory = validate_relative_directory(relative_directory.as_ref())?;
        let file_name = sanitize_file_name(file_name)?;
        let parent = self.prepare_parent(&relative_directory)?;
        let (id, relative_path, final_path, temporary_path) =
            Self::choose_paths(&relative_directory, &parent, file_name.as_str())?;

        let write_result = write_temporary_file(&temporary_path, &mut reader);
        let (content_hash, byte_size) = match write_result {
            Ok(metadata) => metadata,
            Err(error) => {
                let _ = fs::remove_file(&temporary_path);
                return Err(error);
            }
        };

        if let Err(error) = fs::rename(&temporary_path, &final_path) {
            let _ = fs::remove_file(&temporary_path);
            return Err(ArtifactStoreError::Io(error));
        }

        Ok(StoredArtifact {
            id,
            content_hash,
            byte_size,
            safe_relative_path: relative_path,
        })
    }

    fn prepare_parent(&self, relative_directory: &Path) -> Result<PathBuf, ArtifactStoreError> {
        let mut parent = self.root.clone();
        for component in relative_directory.components() {
            let Component::Normal(component) = component else {
                return Err(unsafe_path(
                    relative_directory,
                    "artifact directory contains a non-normal component",
                ));
            };
            parent.push(component);
            match fs::symlink_metadata(&parent) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(unsafe_path(&parent, "symbolic link escapes are rejected"));
                }
                Ok(metadata) if !metadata.is_dir() => {
                    return Err(unsafe_path(
                        &parent,
                        "directory component is not a directory",
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(&parent)?,
                Err(error) => return Err(ArtifactStoreError::Io(error)),
            }
        }
        let canonical_parent = parent.canonicalize()?;
        if !canonical_parent.starts_with(&self.root) {
            return Err(unsafe_path(
                relative_directory,
                "canonical artifact directory escapes the controlled root",
            ));
        }
        Ok(canonical_parent)
    }

    fn choose_paths(
        relative_directory: &Path,
        parent: &Path,
        file_name: &str,
    ) -> Result<(ArtifactId, PathBuf, PathBuf, PathBuf), ArtifactStoreError> {
        for _ in 0..16 {
            let id = ArtifactId::new();
            let output_name = format!("{id}-{file_name}");
            let relative_path = relative_directory.join(&output_name);
            let final_path = parent.join(&output_name);
            let temporary_path = parent.join(format!(".{id}.partial"));
            if fs::symlink_metadata(&final_path).is_err()
                && fs::symlink_metadata(&temporary_path).is_err()
            {
                return Ok((id, relative_path, final_path, temporary_path));
            }
        }
        Err(unsafe_path(
            relative_directory,
            "could not reserve a collision-safe artifact name",
        ))
    }
}

fn validate_relative_directory(path: &Path) -> Result<PathBuf, ArtifactStoreError> {
    if path.is_absolute() {
        return Err(unsafe_path(path, "absolute paths are rejected"));
    }
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(component) => safe.push(component),
            Component::CurDir => {}
            Component::ParentDir => return Err(unsafe_path(path, "path traversal is rejected")),
            Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_path(path, "absolute paths are rejected"));
            }
        }
    }
    Ok(safe)
}

fn sanitize_file_name(file_name: &str) -> Result<String, ArtifactStoreError> {
    let path = Path::new(file_name);
    if path.components().count() != 1 || file_name.is_empty() {
        return Err(unsafe_path(
            path,
            "file name must be one relative path component",
        ));
    }
    let Component::Normal(component) = path
        .components()
        .next()
        .ok_or_else(|| unsafe_path(path, "file name must be one normal path component"))?
    else {
        return Err(unsafe_path(
            path,
            "file name must be one normal path component",
        ));
    };
    let raw = component.to_string_lossy();
    let sanitized = raw
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => character,
            _ => '_',
        })
        .collect::<String>();
    if sanitized.is_empty()
        || sanitized == "."
        || sanitized == ".."
        || is_windows_reserved(&sanitized)
    {
        return Err(unsafe_path(
            path,
            "file name is not safe for controlled storage",
        ));
    }
    Ok(sanitized)
}

fn is_windows_reserved(file_name: &str) -> bool {
    let base = file_name
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (base.len() == 4
            && (base.starts_with("COM") || base.starts_with("LPT"))
            && matches!(base.as_bytes()[3], b'1'..=b'9'))
}

fn write_temporary_file<R>(
    temporary_path: &Path,
    reader: &mut R,
) -> Result<(String, u64), ArtifactStoreError>
where
    R: Read,
{
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary_path)?;
    let mut digest = Sha256::new();
    let mut byte_size = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        byte_size = byte_size
            .checked_add(u64::try_from(read).map_err(|_| ArtifactStoreError::SizeOverflow)?)
            .ok_or(ArtifactStoreError::SizeOverflow)?;
    }
    output.flush()?;
    output.sync_all()?;
    drop(output);
    Ok((hex_encode(digest.finalize().as_slice()), byte_size))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn unsafe_path(path: impl AsRef<Path>, reason: &'static str) -> ArtifactStoreError {
    ArtifactStoreError::UnsafePath {
        path: path.as_ref().to_path_buf(),
        reason,
    }
}
