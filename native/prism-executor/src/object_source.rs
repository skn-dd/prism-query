//! URI-addressable object storage abstraction.
//!
//! Bridges Prism's Parquet reader to `object_store::ObjectStore` so the same
//! code path can read from `file://`, `s3://`, `gs://`, `abfs[s]://`, and
//! bare local paths. Credentials come from the SDK's default provider chain
//! (env vars, IRSA / instance profile, ADC, etc.); explicit credential
//! plumbing is intentionally out of scope for this slice.

use std::sync::Arc;

use futures::stream::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{parse_url, ObjectStore};
use url::Url;

use crate::error::{PrismError, Result};

/// Parse a URI string and return the matching [`ObjectStore`] together with
/// the path inside that store.
///
/// Supported schemes:
/// - `file://...` — local file system
/// - `s3://bucket/key` (also `s3a://`) — Amazon S3 (and S3-compatible such as OCI / MinIO)
/// - `gs://bucket/key` — Google Cloud Storage
/// - `abfs[s]://`, `az://`, `azure://`, `adl://` — Azure ADLS / Blob
/// - bare paths (no scheme) — treated as local file system
///
/// Cloud credentials are resolved from the corresponding SDK's default
/// provider chain. No explicit credentials are accepted here.
pub fn parse_uri(uri: &str) -> Result<(Arc<dyn ObjectStore>, Path)> {
    // Bare path → local file system. We treat anything missing a `://`
    // (and not already a `file:` URL) as a local path so existing callers
    // that pass `/data/prism/...` keep working.
    if !uri.contains("://") {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let absolute = std::path::Path::new(uri)
            .canonicalize()
            .unwrap_or_else(|_| std::path::PathBuf::from(uri));
        let path = Path::from_filesystem_path(&absolute).map_err(|e| {
            PrismError::Internal(format!("invalid local path {:?}: {}", absolute, e))
        })?;
        return Ok((store, path));
    }

    let url = Url::parse(uri)
        .map_err(|e| PrismError::Internal(format!("invalid URI {:?}: {}", uri, e)))?;

    // Special-case `file://` so we can canonicalise relative paths and
    // produce a correctly rooted `Path` on every platform without the
    // bucket-style normalisation that `parse_url` applies.
    if url.scheme() == "file" {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let raw = url
            .to_file_path()
            .map_err(|_| PrismError::Internal(format!("invalid file URI {:?}", uri)))?;
        let absolute = raw.canonicalize().unwrap_or(raw);
        let path = Path::from_filesystem_path(&absolute).map_err(|e| {
            PrismError::Internal(format!("invalid local path {:?}: {}", absolute, e))
        })?;
        return Ok((store, path));
    }

    let (store, path) = parse_url(&url)
        .map_err(|e| PrismError::Internal(format!("object_store parse_url({}): {}", uri, e)))?;
    Ok((Arc::from(store), path))
}

/// List every object under `prefix` whose key ends with `.{extension}`.
///
/// Results are sorted by full path so callers see a stable order across
/// invocations (Parquet readers expect deterministic file ordering for
/// reproducible row counts).
pub async fn list_files(
    store: &dyn ObjectStore,
    prefix: &Path,
    extension: &str,
) -> Result<Vec<Path>> {
    let suffix = format!(".{}", extension);
    let mut stream = store.list(Some(prefix));
    let mut out = Vec::new();
    while let Some(meta) = stream
        .try_next()
        .await
        .map_err(|e| PrismError::Internal(format!("object_store list: {}", e)))?
    {
        if meta.location.as_ref().ends_with(&suffix) {
            out.push(meta.location);
        }
    }
    // If the prefix itself names a single matching object, `list` returns
    // it — that's the desired behaviour. If `list` returned nothing and the
    // prefix points at a single object, fall back to `head`.
    if out.is_empty() {
        if let Ok(meta) = store.head(prefix).await {
            if meta.location.as_ref().ends_with(&suffix) {
                out.push(meta.location);
            }
        }
    }
    out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    #[tokio::test]
    async fn parse_local_bare_path_roundtrips() {
        let dir = std::env::temp_dir().join("prism_object_source_bare");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (store, path) = parse_uri(dir.to_str().unwrap()).unwrap();
        // The directory should be listable (will be empty).
        let files = list_files(&*store, &path, "parquet").await.unwrap();
        assert!(files.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parse_file_uri_works() {
        let dir = std::env::temp_dir().join("prism_object_source_file_uri");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let uri = format!("file://{}", dir.display());
        let (_store, _path) = parse_uri(&uri).unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn parse_s3_uri_returns_path() {
        // We can't actually contact S3 in tests, but we can assert that
        // URI parsing succeeds and the path component is correct.
        let (_store, path) = parse_uri("s3://my-bucket/some/prefix").unwrap();
        assert_eq!(path.as_ref(), "some/prefix");
    }

    #[tokio::test]
    async fn list_filters_by_extension() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from("data/a.parquet"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        store
            .put(&Path::from("data/b.parquet"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        store
            .put(&Path::from("data/c.json"), PutPayload::from_static(b"x"))
            .await
            .unwrap();

        let prefix = Path::from("data");
        let files = list_files(&*store, &prefix, "parquet").await.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].as_ref(), "data/a.parquet");
        assert_eq!(files[1].as_ref(), "data/b.parquet");
    }
}
