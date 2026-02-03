use std::path::Path;

use async_stream::stream as async_stream;
use futures::{Stream, StreamExt, stream};
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};

use super::cache::{CachedCommit, CachedPullRequest, GitHubCache};
use super::*;
use crate::config::Remote;
use crate::error::*;

/// Log message to show while fetching data from GitHub.
pub const START_FETCHING_MSG: &str = "Retrieving data from GitHub...";

/// Log message to show when done fetching from GitHub.
pub const FINISHED_FETCHING_MSG: &str = "Done fetching GitHub data.";

/// Template variables related to this remote.
pub(crate) const TEMPLATE_VARIABLES: &[&str] = &["github", "commit.github", "commit.remote"];

/// Representation of a single commit.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitHubCommit {
    /// SHA.
    pub sha: String,
    /// Author of the commit.
    pub author: Option<GitHubCommitAuthor>,
    /// Details of the commit
    pub commit: Option<GitHubCommitDetails>,
}

/// Representation of subset of commit details
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitHubCommitDetails {
    /// Author of the commit
    pub author: GitHubCommitDetailsAuthor,
}

/// Representation of subset of commit author details
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitHubCommitDetailsAuthor {
    /// Date of the commit
    pub date: String,
}

impl RemoteCommit for GitHubCommit {
    fn id(&self) -> String {
        self.sha.clone()
    }

    fn username(&self) -> Option<String> {
        self.author.clone().and_then(|v| v.login)
    }

    fn timestamp(&self) -> Option<i64> {
        self.commit
            .clone()
            .map(|f| self.convert_to_unix_timestamp(f.author.date.clone().as_str()))
    }
}

/// Author of the commit.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitHubCommitAuthor {
    /// Username.
    pub login: Option<String>,
}

/// Label of the pull request.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullRequestLabel {
    /// Name of the label.
    pub name: String,
}

/// Representation of a single pull request.
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitHubPullRequest {
    /// Pull request number.
    pub number: i64,
    /// Pull request title.
    pub title: Option<String>,
    /// SHA of the merge commit.
    pub merge_commit_sha: Option<String>,
    /// Labels of the pull request.
    pub labels: Vec<PullRequestLabel>,
    /// Last updated timestamp (RFC3339 format).
    pub updated_at: Option<String>,
}

impl RemotePullRequest for GitHubPullRequest {
    fn number(&self) -> i64 {
        self.number
    }

    fn title(&self) -> Option<String> {
        self.title.clone()
    }

    fn labels(&self) -> Vec<String> {
        self.labels.iter().map(|v| v.name.clone()).collect()
    }

    fn merge_commit(&self) -> Option<String> {
        self.merge_commit_sha.clone()
    }
}

/// HTTP client for handling GitHub REST API requests.
#[derive(Debug, Clone)]
pub struct GitHubClient {
    /// Remote.
    remote: Remote,
    /// HTTP client.
    client: ClientWithMiddleware,
}

/// Constructs a GitHub client from the remote configuration.
impl TryFrom<Remote> for GitHubClient {
    type Error = Error;
    fn try_from(remote: Remote) -> Result<Self> {
        Ok(Self {
            client: remote.create_client("application/vnd.github+json")?,
            remote,
        })
    }
}

impl RemoteClient for GitHubClient {
    const API_URL: &'static str = "https://api.github.com";
    const API_URL_ENV: &'static str = "GITHUB_API_URL";

    fn remote(&self) -> Remote {
        self.remote.clone()
    }

    fn client(&self) -> ClientWithMiddleware {
        self.client.clone()
    }
}

impl GitHubClient {
    /// Constructs the URL for GitHub commits API.
    fn commits_url(api_url: &str, remote: &Remote, ref_name: Option<&str>, page: i32) -> String {
        let mut url = format!(
            "{}/repos/{}/{}/commits?per_page={MAX_PAGE_SIZE}&page={page}",
            api_url, remote.owner, remote.repo
        );

        if let Some(ref_name) = ref_name {
            url.push_str(&format!("&sha={ref_name}"));
        }

        url
    }

    /// Constructs the URL for GitHub pull requests API.
    fn pull_requests_url(api_url: &str, remote: &Remote, page: i32) -> String {
        format!(
            "{}/repos/{}/{}/pulls?per_page={MAX_PAGE_SIZE}&page={page}&state=closed",
            api_url, remote.owner, remote.repo
        )
    }

    /// Fetches the complete list of commits.
    /// This is inefficient for large repositories; consider using
    /// `get_commit_stream` instead.
    pub async fn get_commits(&self, ref_name: Option<&str>) -> Result<Vec<Box<dyn RemoteCommit>>> {
        use futures::TryStreamExt;
        self.get_commit_stream(ref_name).try_collect().await
    }

    /// Fetches the complete list of pull requests.
    /// This is inefficient for large repositories; consider using
    /// `get_pull_request_stream` instead.
    pub async fn get_pull_requests(&self) -> Result<Vec<Box<dyn RemotePullRequest>>> {
        use futures::TryStreamExt;
        self.get_pull_request_stream().try_collect().await
    }

    fn get_commit_stream<'a>(
        &'a self,
        ref_name: Option<&str>,
    ) -> impl Stream<Item = Result<Box<dyn RemoteCommit>>> + 'a {
        let ref_name = ref_name.map(ToString::to_string);
        async_stream! {
            let page_stream = stream::iter(0..)
                .map(|page|
                    {
                    let ref_name = ref_name.clone();
                    async move {
                        let url = Self::commits_url(&self.api_url(), &self.remote(), ref_name.as_deref(), page);
                        self.get_json::<Vec<GitHubCommit>>(&url).await
                    }})
                .buffered(10);

            let mut page_stream = Box::pin(page_stream);

            while let Some(page_result) = page_stream.next().await {
                match page_result {
                    Ok(commits) => {
                        if commits.is_empty() {
                            break;
                        }

                        for commit in commits {
                            yield Ok(Box::new(commit) as Box<dyn RemoteCommit>);
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        }
    }

    fn get_pull_request_stream<'a>(
        &'a self,
    ) -> impl Stream<Item = Result<Box<dyn RemotePullRequest>>> + 'a {
        async_stream! {
            let page_stream = stream::iter(0..)
                .map(|page| async move {
                    let url = Self::pull_requests_url(&self.api_url(), &self.remote(), page);
                    self.get_json::<Vec<GitHubPullRequest>>(&url).await
                })
                .buffered(5);

            let mut page_stream = Box::pin(page_stream);

            while let Some(page_result) = page_stream.next().await {
                match page_result {
                    Ok(prs) => {
                        if prs.is_empty() {
                            break;
                        }

                        for pr in prs {
                            yield Ok(Box::new(pr) as Box<dyn RemotePullRequest>);
                        }
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        }
    }

    // ========== Cache-aware methods ==========

    /// Constructs the URL for GitHub pull requests API with sorting by updated_at.
    /// This is used for incremental fetching.
    fn pull_requests_url_sorted(
        api_url: &str,
        remote: &Remote,
        page: i32,
        since: Option<&str>,
    ) -> String {
        let mut url = format!(
            "{}/repos/{}/{}/pulls?per_page={MAX_PAGE_SIZE}&page={page}&state=closed&sort=updated&direction=desc",
            api_url, remote.owner, remote.repo
        );

        if let Some(since) = since {
            url.push_str(&format!("&since={}", urlencoding::encode(since)));
        }

        url
    }

    /// Constructs the URL for GitHub commits API with since parameter.
    fn commits_url_since(
        api_url: &str,
        remote: &Remote,
        ref_name: Option<&str>,
        page: i32,
        since: Option<&str>,
    ) -> String {
        let mut url = format!(
            "{}/repos/{}/{}/commits?per_page={MAX_PAGE_SIZE}&page={page}",
            api_url, remote.owner, remote.repo
        );

        if let Some(ref_name) = ref_name {
            url.push_str(&format!("&sha={ref_name}"));
        }

        if let Some(since) = since {
            url.push_str(&format!("&since={}", urlencoding::encode(since)));
        }

        url
    }

    /// Fetches commits and pull requests using cache for incremental updates.
    ///
    /// This method:
    /// 1. Loads existing cache from the git directory
    /// 2. Fetches only new/updated data since the last cache update
    /// 3. Merges the new data with the cache
    /// 4. Saves the updated cache
    /// 5. Returns all data as trait objects
    pub async fn get_commits_and_prs_cached(
        &self,
        git_dir: &Path,
        ref_name: Option<&str>,
    ) -> Result<(Vec<Box<dyn RemoteCommit>>, Vec<Box<dyn RemotePullRequest>>)> {
        let mut cache = GitHubCache::load(git_dir);
        let since = cache.get_since_timestamp().map(|s| s.to_string());

        let (cached_commits, cached_prs) = cache.stats();
        if since.is_some() {
            log::info!(
                "Using GitHub cache with {} commits and {} PRs (last updated: {})",
                cached_commits,
                cached_prs,
                since.as_deref().unwrap_or("unknown")
            );
        }

        // Fetch new commits
        let new_commits = self
            .fetch_commits_since(ref_name, since.as_deref())
            .await?;
        log::debug!("Fetched {} new commits from GitHub API", new_commits.len());

        // Update cache with new commits
        for commit in &new_commits {
            cache.commits.insert(
                commit.sha.clone(),
                CachedCommit {
                    sha: commit.sha.clone(),
                    author_login: commit.author.as_ref().and_then(|a| a.login.clone()),
                    date: commit.commit.as_ref().map(|c| c.author.date.clone()),
                },
            );
        }

        // Fetch new/updated PRs
        let new_prs = self.fetch_prs_since(since.as_deref()).await?;
        log::debug!(
            "Fetched {} new/updated PRs from GitHub API",
            new_prs.len()
        );

        // Update cache with new/updated PRs
        for pr in &new_prs {
            cache.pull_requests.insert(
                pr.number,
                CachedPullRequest {
                    number: pr.number,
                    title: pr.title.clone(),
                    merge_commit_sha: pr.merge_commit_sha.clone(),
                    labels: pr.labels.iter().map(|l| l.name.clone()).collect(),
                    updated_at: pr.updated_at.clone(),
                },
            );
        }

        // Update timestamp and save cache
        cache.update_timestamp();
        if let Err(e) = cache.save(git_dir) {
            log::warn!("Failed to save GitHub cache: {}", e);
        }

        // Convert cache to trait objects
        let commits: Vec<Box<dyn RemoteCommit>> = cache
            .commits
            .values()
            .map(|c| {
                Box::new(GitHubCommit {
                    sha: c.sha.clone(),
                    author: c.author_login.as_ref().map(|login| GitHubCommitAuthor {
                        login: Some(login.clone()),
                    }),
                    commit: c.date.as_ref().map(|date| GitHubCommitDetails {
                        author: GitHubCommitDetailsAuthor { date: date.clone() },
                    }),
                }) as Box<dyn RemoteCommit>
            })
            .collect();

        let prs: Vec<Box<dyn RemotePullRequest>> = cache
            .pull_requests
            .values()
            .map(|p| {
                Box::new(GitHubPullRequest {
                    number: p.number,
                    title: p.title.clone(),
                    merge_commit_sha: p.merge_commit_sha.clone(),
                    labels: p
                        .labels
                        .iter()
                        .map(|name| PullRequestLabel { name: name.clone() })
                        .collect(),
                    updated_at: p.updated_at.clone(),
                }) as Box<dyn RemotePullRequest>
            })
            .collect();

        let (final_commits, final_prs) = (commits.len(), prs.len());
        log::info!(
            "GitHub cache now has {} commits and {} PRs",
            final_commits,
            final_prs
        );

        Ok((commits, prs))
    }

    /// Fetches commits since the given timestamp.
    async fn fetch_commits_since(
        &self,
        ref_name: Option<&str>,
        since: Option<&str>,
    ) -> Result<Vec<GitHubCommit>> {
        let mut all_commits = Vec::new();
        let mut page = 0;

        loop {
            let url = Self::commits_url_since(
                &self.api_url(),
                &self.remote(),
                ref_name,
                page,
                since,
            );
            let commits: Vec<GitHubCommit> = self.get_json(&url).await?;

            if commits.is_empty() {
                break;
            }

            all_commits.extend(commits);
            page += 1;
        }

        Ok(all_commits)
    }

    /// Fetches PRs updated since the given timestamp.
    /// Uses sort=updated&direction=desc to stop early when we reach PRs
    /// that haven't been updated since our last fetch.
    async fn fetch_prs_since(&self, since: Option<&str>) -> Result<Vec<GitHubPullRequest>> {
        let mut all_prs = Vec::new();
        let mut page = 0;

        loop {
            let url = Self::pull_requests_url_sorted(
                &self.api_url(),
                &self.remote(),
                page,
                None, // GitHub PRs API doesn't support 'since' parameter directly
            );
            let prs: Vec<GitHubPullRequest> = self.get_json(&url).await?;

            if prs.is_empty() {
                break;
            }

            // If we have a since timestamp, check if we've reached PRs
            // that haven't been updated since then
            if let Some(since_ts) = since {
                let mut found_old_pr = false;
                for pr in prs {
                    if let Some(ref updated_at) = pr.updated_at {
                        if updated_at.as_str() < since_ts {
                            // This PR and all following are older than our cache
                            found_old_pr = true;
                            break;
                        }
                    }
                    all_prs.push(pr);
                }
                if found_old_pr {
                    break;
                }
            } else {
                // No since timestamp, fetch all PRs
                all_prs.extend(prs);
            }

            page += 1;
        }

        Ok(all_prs)
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::remote::RemoteCommit;

    #[test]
    fn timestamp() {
        let remote_commit = GitHubCommit {
            sha: String::from("1d244937ee6ceb8e0314a4a201ba93a7a61f2071"),
            author: Some(GitHubCommitAuthor {
                login: Some(String::from("orhun")),
            }),
            commit: Some(GitHubCommitDetails {
                author: GitHubCommitDetailsAuthor {
                    date: String::from("2021-07-18T15:14:39+03:00"),
                },
            }),
        };

        assert_eq!(Some(1_626_610_479), remote_commit.timestamp());
    }
}
