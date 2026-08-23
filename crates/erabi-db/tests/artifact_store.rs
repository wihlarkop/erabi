use std::io::{self, Read};
use std::path::{Path, PathBuf};

use erabi_db::{
    ArtifactStore, ArtifactStoreError, ErabiDatabase, MigrationRunner,
    repositories::ArtifactRepository,
};
use erabi_domain::ArtifactId;

struct TemporaryRoot {
    path: PathBuf,
}

impl TemporaryRoot {
    fn new() -> Result<Self, io::Error> {
        let path = std::env::temp_dir().join(format!("erabi-artifact-test-{}", ArtifactId::new()));
        std::fs::create_dir(&path)?;
        Ok(Self { path })
    }
}

impl Drop for TemporaryRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct InterruptedReader {
    first_read: bool,
}

impl Read for InterruptedReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.first_read {
            return Err(io::Error::other("simulated interrupted artifact stream"));
        }
        self.first_read = true;
        buffer[..4].copy_from_slice(b"part");
        Ok(4)
    }
}

#[test]
fn artifact_store_publishes_bytes_atomically_with_hash_and_size()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryRoot::new()?;
    let store = ArtifactStore::new(&root.path)?;
    let artifact = store.write_bytes("pages", "report.html", b"hello")?;

    assert_eq!(artifact.byte_size, 5);
    assert_eq!(
        artifact.content_hash,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );
    assert_eq!(
        std::fs::read(store.root().join(&artifact.safe_relative_path))?,
        b"hello"
    );
    assert!(std::fs::read_dir(store.root().join("pages"))?.all(|entry| {
        entry.is_ok_and(|entry| !entry.file_name().to_string_lossy().ends_with(".partial"))
    }));
    Ok(())
}

#[test]
fn artifact_store_cleans_interrupted_temporary_writes() -> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryRoot::new()?;
    let store = ArtifactStore::new(&root.path)?;

    assert!(matches!(
        store.write(
            "pages",
            "report.html",
            InterruptedReader { first_read: false }
        ),
        Err(ArtifactStoreError::Io(_))
    ));
    let page_directory = store.root().join("pages");
    assert!(std::fs::read_dir(page_directory)?.next().is_none());
    Ok(())
}

#[test]
fn artifact_store_rejects_traversal_and_absolute_paths() -> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryRoot::new()?;
    let store = ArtifactStore::new(&root.path)?;

    assert!(matches!(
        store.write_bytes("../outside", "report.html", b"x"),
        Err(ArtifactStoreError::UnsafePath { .. })
    ));
    assert!(matches!(
        store.write_bytes(Path::new("/outside"), "report.html", b"x"),
        Err(ArtifactStoreError::UnsafePath { .. })
    ));
    Ok(())
}

#[cfg(windows)]
#[test]
fn artifact_store_rejects_windows_absolute_paths() -> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryRoot::new()?;
    let store = ArtifactStore::new(&root.path)?;
    assert!(matches!(
        store.write_bytes(Path::new("C:\\outside"), "report.html", b"x"),
        Err(ArtifactStoreError::UnsafePath { .. })
    ));
    Ok(())
}

#[cfg(windows)]
#[test]
fn artifact_store_rejects_a_symlink_escape() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::symlink_dir;
    use std::process::Command;

    let root = TemporaryRoot::new()?;
    let outside = TemporaryRoot::new()?;
    let escape = root.path.join("escape");
    if symlink_dir(&outside.path, &escape).is_err() {
        let status = Command::new("cmd")
            .arg("/C")
            .arg("mklink")
            .arg("/J")
            .arg(&escape)
            .arg(&outside.path)
            .status()?;
        if !status.success() {
            return Err("could not create a symbolic-link or junction test fixture".into());
        }
    }
    let store = ArtifactStore::new(&root.path)?;

    assert!(matches!(
        store.write_bytes("escape", "report.html", b"x"),
        Err(ArtifactStoreError::UnsafePath { .. })
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn artifact_store_rejects_a_unix_symlink_escape() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let root = TemporaryRoot::new()?;
    let outside = TemporaryRoot::new()?;
    let escape = root.path.join("escape");
    symlink(&outside.path, &escape)?;
    let store = ArtifactStore::new(&root.path)?;

    assert!(matches!(
        store.write_bytes("escape", "report.html", b"x"),
        Err(ArtifactStoreError::UnsafePath { .. })
    ));
    Ok(())
}

#[test]
fn artifact_store_uses_collision_safe_names_and_persists_only_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TemporaryRoot::new()?;
    let store = ArtifactStore::new(&root.path)?;
    let first = store.write_bytes("pages", "report.html", b"first")?;
    let second = store.write_bytes("pages", "report.html", b"second")?;
    assert_ne!(first.safe_relative_path, second.safe_relative_path);

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let database = ErabiDatabase::in_memory().await?;
        MigrationRunner::default().apply(&database).await?;
        let repository = ArtifactRepository::new(&database);
        repository
            .record(
                &first,
                None,
                None,
                Some("text/html"),
                "2026-08-23T00:00:00Z",
                &serde_json::json!({"kind": "raw_html"}),
            )
            .await?;
        assert_eq!(
            repository.safe_relative_path(first.id).await?,
            first.safe_relative_path.to_string_lossy()
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
