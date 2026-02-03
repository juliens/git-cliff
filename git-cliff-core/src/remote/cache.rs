//! Cache module for storing and retrieving remote data.
//!
//! This module provides caching functionality for GitHub commits and pull requests
//! to avoid fetching all data on every changelog generation.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::Result;

/// Name of the cache file for GitHub data.
const GITHUB_CACHE_FILE: &str = "github_cache.json";

/// Cache data structure for GitHub.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GitHubCache {
    /// Timestamp of when the cache was last updated (RFC3339 format).
    pub last_updated: Option<String>,
    /// Cached commits indexed by SHA.
    pub commits: HashMap<String, CachedCommit>,
    /// Cached pull requests indexed by PR number.
    pub pull_requests: HashMap<i64, CachedPullRequest>,
}

/// Cached commit data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCommit {
    /// Commit SHA.
    pub sha: String,
    /// Author username (login).
    pub author_login: Option<String>,
    /// Commit date (RFC3339 format).
    pub date: Option<String>,
}

/// Cached pull request data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedPullRequest {
    /// PR number.
    pub number: i64,
    /// PR title.
    pub title: Option<String>,
    /// Merge commit SHA.
    pub merge_commit_sha: Option<String>,
    /// Labels.
    pub labels: Vec<String>,
    /// Last updated timestamp (RFC3339 format).
    pub updated_at: Option<String>,
}

impl GitHubCache {
    /// Returns the cache file path for a given git directory.
    pub fn cache_path(git_dir: &Path) -> PathBuf {
        git_dir
            .join(env!("CARGO_PKG_NAME"))
            .join(GITHUB_CACHE_FILE)
    }

    /// Loads the cache from the given git directory.
    ///
    /// Returns an empty cache if the file doesn't exist or can't be parsed.
    pub fn load(git_dir: &Path) -> Self {
        let cache_path = Self::cache_path(git_dir);
        if !cache_path.exists() {
            log::debug!("GitHub cache not found at {:?}, starting fresh", cache_path);
            return Self::default();
        }

        match fs::File::open(&cache_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                if file.read_to_string(&mut contents).is_ok() {
                    match serde_json::from_str(&contents) {
                        Ok(cache) => {
                            log::info!("Loaded GitHub cache from {:?}", cache_path);
                            return cache;
                        }
                        Err(e) => {
                            log::warn!(
                                "Failed to parse GitHub cache at {:?}: {}, starting fresh",
                                cache_path,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!(
                    "Failed to open GitHub cache at {:?}: {}, starting fresh",
                    cache_path,
                    e
                );
            }
        }

        Self::default()
    }

    /// Saves the cache to the given git directory.
    pub fn save(&self, git_dir: &Path) -> Result<()> {
        let cache_path = Self::cache_path(git_dir);

        // Create parent directories if they don't exist
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let contents = serde_json::to_string_pretty(self)?;
        let mut file = fs::File::create(&cache_path)?;
        file.write_all(contents.as_bytes())?;

        log::info!("Saved GitHub cache to {:?}", cache_path);
        Ok(())
    }

    /// Updates the last_updated timestamp to now.
    pub fn update_timestamp(&mut self) {
        self.last_updated = Some(
            OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
        );
    }

    /// Returns the last_updated timestamp as a string suitable for API queries.
    /// Returns None if no previous update exists.
    pub fn get_since_timestamp(&self) -> Option<&str> {
        self.last_updated.as_deref()
    }

    /// Checks if the cache is empty (first run).
    pub fn is_empty(&self) -> bool {
        self.commits.is_empty() && self.pull_requests.is_empty()
    }

    /// Returns statistics about the cache.
    pub fn stats(&self) -> (usize, usize) {
        (self.commits.len(), self.pull_requests.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temp_dir::TempDir;

    #[test]
    fn test_cache_round_trip() {
        let temp_dir = TempDir::new().unwrap();
        let git_dir = temp_dir.path();

        let mut cache = GitHubCache::default();
        cache.commits.insert(
            "abc123".to_string(),
            CachedCommit {
                sha: "abc123".to_string(),
                author_login: Some("user1".to_string()),
                date: Some("2024-01-01T00:00:00Z".to_string()),
            },
        );
        cache.pull_requests.insert(
            1,
            CachedPullRequest {
                number: 1,
                title: Some("Test PR".to_string()),
                merge_commit_sha: Some("def456".to_string()),
                labels: vec!["bug".to_string()],
                updated_at: Some("2024-01-01T00:00:00Z".to_string()),
            },
        );
        cache.update_timestamp();

        cache.save(git_dir).unwrap();

        let loaded = GitHubCache::load(git_dir);
        assert_eq!(loaded.commits.len(), 1);
        assert_eq!(loaded.pull_requests.len(), 1);
        assert!(loaded.last_updated.is_some());
    }

    #[test]
    fn test_empty_cache() {
        let temp_dir = TempDir::new().unwrap();
        let git_dir = temp_dir.path();

        let cache = GitHubCache::load(git_dir);
        assert!(cache.is_empty());
        assert!(cache.last_updated.is_none());
    }
}
