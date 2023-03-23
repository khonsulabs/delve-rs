use std::collections::{HashMap, HashSet};
use std::path::Path;

use bonsaidb::core::connection::Connection;
use bonsaidb::core::schema::SerializedCollection;
use bonsaidb::core::transaction::{Operation, Transaction};
use bonsaidb::local::Database;
use reqwest::header::LAST_MODIFIED;
use serde::Deserialize;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::cache::Cache;
use crate::schema::{self, CalendarDate, ImportState, OwnerId, VersionDownloadKey};

// TODO this reference to cache means it won't ever drop because this task never exits.
pub async fn import_continuously(database: Database, cache: Cache) -> anyhow::Result<()> {
    loop {
        if let Some(latest_dump) = download_new_dump(&database).await? {
            let (sender, receiver) = std::sync::mpsc::sync_channel(100_000);

            let importer = tokio::task::spawn_blocking({
                let database = database.clone();

                move || import_dump(latest_dump, &database, sender)
            });

            let mut tx = Transaction::new();
            let mut op_count = 0;
            let mut uncompacted_operations = 0;
            while let Ok(operation) = receiver.recv() {
                tx.operations.push(operation);
                if tx.operations.len() >= 100_000 {
                    let new_count = op_count + tx.operations.len();
                    println!("Committing {op_count}:{new_count} changes");
                    tx.apply(&database)?;
                    tx = Transaction::new();
                    op_count = new_count;
                    uncompacted_operations += op_count;
                }

                if uncompacted_operations > 2_000_000 {
                    // Keep disk space down by compacting frequently.
                    database.compact()?;
                    uncompacted_operations = 0;
                }
            }

            if !tx.operations.is_empty() {
                let new_count = op_count + tx.operations.len();
                println!("Committing {op_count}:{new_count} changes");
                tx.apply(&database)?;
                op_count = new_count;
                uncompacted_operations += op_count;
            }

            importer.await??;

            // This cleans up the database once per day-ish.
            if op_count > 0 && uncompacted_operations > 0 {
                println!("Compacting.");
                database.compact()?;
            }

            println!("Done importing.");

            cache.refresh()?;
        } else {
            println!("No new data dumps are available.");
        }
        // Check for new dumps every hour.
        tokio::time::sleep(std::time::Duration::from_secs(60 * 60)).await;
    }
}

async fn download(client: reqwest::Client) -> anyhow::Result<(String, String)> {
    println!("Downloading new dump.");
    let mut response = client
        .get("https://static.crates.io/db-dump.tar.gz")
        .send()
        .await?;
    let last_modified = response
        .headers()
        .get(LAST_MODIFIED)
        .ok_or_else(|| anyhow::anyhow!("db-dump is missing its last-modified header."))?
        .to_str()?
        .to_string();

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open("db-dump.tar.gz")
        .await?;
    while let Some(chunk) = response.chunk().await? {
        file.write_all(&chunk).await?;
    }
    drop(file);

    if !Command::new("/usr/bin/tar")
        .arg("-xzf")
        .arg("db-dump.tar.gz")
        .status()
        .await?
        .success()
    {
        anyhow::bail!("error extracting database dump");
    }

    let latest_dump = find_latest_dump(true)
        .await?
        .ok_or_else(|| anyhow::anyhow!("archive contained stale export"))?;

    Ok((latest_dump, last_modified))
}

async fn find_latest_dump(allow_stale: bool) -> anyhow::Result<Option<String>> {
    let mut entries = tokio::fs::read_dir(".").await?;
    let now = OffsetDateTime::now_utc();
    let mut latest_date = None;
    while let Some(entry) = entries.next_entry().await? {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else { continue };
        let Some(folder_date) = parse_folder_date(file_name)
            else { continue };

        let build_expires_at = folder_date + Duration::days(1);
        if build_expires_at < now || allow_stale {
            latest_date = latest_date.max(Some(file_name.to_string()));
        } else {
            // Delete this folder, because it's stale.
            tokio::fs::remove_dir_all(&file_name).await?;
        }
    }

    Ok(latest_date)
}

async fn download_new_dump(db: &Database) -> anyhow::Result<Option<String>> {
    let mut state = ImportState::get(&(), db)?
        .map(|d| d.contents)
        .unwrap_or_default();

    let http = reqwest::Client::new();
    let response = http
        .head("https://static.crates.io/db-dump.tar.gz")
        .send()
        .await?;
    let new_dump_last_modified = response
        .headers()
        .get(LAST_MODIFIED)
        .ok_or_else(|| anyhow::anyhow!("db-dump is missing its last-modified header."))?
        .to_str()?
        .to_string();
    let new_dump_available = state
        .downloaded_last_modified
        .as_deref()
        .map_or(true, |last_imported| {
            last_imported < new_dump_last_modified.as_str()
        });
    drop(response);

    let latest_date = find_latest_dump(!new_dump_available).await?;

    if let Some(latest_date) = latest_date {
        if state
            .last_dump_imported
            .as_ref()
            .map_or(true, |last_dump_imported| last_dump_imported < &latest_date)
        {
            state.downloaded_last_modified = Some(new_dump_last_modified);
            state.overwrite_into(&(), db)?;
            Ok(Some(latest_date))
        } else {
            Ok(None)
        }
    } else {
        let (path, new_last_modified) = download(http).await?;

        state.downloaded_last_modified = Some(new_last_modified);
        state.overwrite_into(&(), db)?;

        Ok(Some(path))
    }
}

fn parse_folder_date(file_name: &str) -> Option<OffsetDateTime> {
    let (date, hms) = file_name.rsplit_once('-')?;
    let date = parse_iso_date(date).ok()?;

    if hms.len() != 6 {
        return None;
    }
    let hours = hms[..2].parse::<u8>().ok()?;
    let minutes = hms[2..4].parse::<u8>().ok()?;
    let seconds = hms[4..].parse::<u8>().ok()?;
    let time = Time::from_hms(hours, minutes, seconds).ok()?;

    Some(PrimitiveDateTime::new(date, time).assume_utc())
}

fn import_dump(
    dump_date: String,
    db: &Database,
    tx_sender: std::sync::mpsc::SyncSender<Operation>,
) -> anyhow::Result<()> {
    let path = Path::new(&dump_date);
    let data_folder = path.join("data");

    // Now we can import the crates structure.

    apply_crate_changes(&data_folder, &tx_sender, db)?;
    apply_keyword_changes(&data_folder, &tx_sender, db)?;
    apply_category_changes(&data_folder, &tx_sender, db)?;
    let version_crates = apply_version_changes(&data_folder, &tx_sender, db)?;
    apply_version_download_changes(&data_folder, &tx_sender, db, &version_crates)?;

    let mut state = ImportState::get(&(), db)?.expect("downloading inserts state");
    state.contents.last_dump_imported = Some(dump_date);
    tx_sender.send(Operation::update_serialized::<ImportState>(
        state.header,
        &state.contents,
    )?)?;

    Ok(())
}

fn apply_crate_changes(
    data_folder: &Path,
    tx: &std::sync::mpsc::SyncSender<Operation>,
    db: &Database,
) -> anyhow::Result<()> {
    // Gather the keywords and categories for the crates
    println!("Parsing crate keywords.");
    let mut keyword_ids_by_crate = load_crate_keywords(data_folder)?;
    println!("Parsing crate categories.");
    let mut category_ids_by_crate = load_crate_categories(data_folder)?;
    println!("Parsing crate owners.");
    let mut owners = load_crate_owners(data_folder)?;

    println!("Parsing crates.");
    let mut crates = csv::Reader::from_reader(std::fs::File::open(data_folder.join("crates.csv"))?);
    for row in crates.deserialize() {
        let cr: Crate = row?;
        let id = cr.id;
        let cr = schema::Crate {
            created_at: cr.created_at,
            description: cr.description,
            documentation: cr.documentation,
            downloads: cr.downloads,
            homepage: cr.homepage,
            max_upload_size: cr.max_upload_size,
            name: cr.name,
            readme: cr.readme,
            repository: cr.repository,
            updated_at: cr.updated_at,
            keywords: keyword_ids_by_crate.remove(&cr.id).unwrap_or_default(),
            category_ids: category_ids_by_crate.remove(&cr.id).unwrap_or_default(),
            owners: owners.remove(&cr.id).unwrap_or_default(),
        };

        if let Some(existing) = schema::Crate::get(&id, db)? {
            if existing.contents == cr {
                continue;
            }
        }

        tx.send(Operation::overwrite_serialized::<schema::Crate, _>(
            &id, &cr,
        )?)?;
    }

    Ok(())
}

fn load_crate_keywords(path: &Path) -> anyhow::Result<HashMap<u64, HashSet<u64>>> {
    let mut crate_keywords =
        csv::Reader::from_reader(std::fs::File::open(path.join("crates_keywords.csv"))?);
    let mut keyword_ids_by_crate = HashMap::new();
    for row in crate_keywords.deserialize() {
        let row: CrateKeywords = row?;
        let keywords = keyword_ids_by_crate
            .entry(row.crate_id)
            .or_insert_with(HashSet::default);
        keywords.insert(row.keyword_id);
    }
    Ok(keyword_ids_by_crate)
}

fn load_crate_categories(path: &Path) -> anyhow::Result<HashMap<u64, HashSet<u64>>> {
    let mut crate_categories =
        csv::Reader::from_reader(std::fs::File::open(path.join("crates_categories.csv"))?);
    let mut category_ids_by_crate = HashMap::new();
    for row in crate_categories.deserialize() {
        let row: CrateCategories = row?;
        let categories = category_ids_by_crate
            .entry(row.crate_id)
            .or_insert_with(HashSet::default);
        categories.insert(row.category_id);
    }
    Ok(category_ids_by_crate)
}

fn load_crate_owners(path: &Path) -> anyhow::Result<HashMap<u64, HashSet<OwnerId>>> {
    let mut crate_categories =
        csv::Reader::from_reader(std::fs::File::open(path.join("crate_owners.csv"))?);
    let mut owners_by_crate = HashMap::new();
    for row in crate_categories.deserialize() {
        let row: CrateOwners = row?;
        let categories = owners_by_crate
            .entry(row.crate_id)
            .or_insert_with(HashSet::default);
        let owner = match row.owner_kind {
            0 => OwnerId::User(row.owner_id),
            1 => OwnerId::Team(row.owner_id),
            _ => anyhow::bail!("expected owner kind: {}", row.owner_kind),
        };
        categories.insert(owner);
    }
    Ok(owners_by_crate)
}

fn apply_keyword_changes(
    data_folder: &Path,
    tx: &std::sync::mpsc::SyncSender<Operation>,
    db: &Database,
) -> anyhow::Result<()> {
    let mut existing_keywords = schema::Keyword::all(db)
        .query()?
        .into_iter()
        .map(|d| (d.header.id, d))
        .collect::<HashMap<_, _>>();
    let mut keywords =
        csv::Reader::from_reader(std::fs::File::open(data_folder.join("keywords.csv"))?);
    for row in keywords.deserialize() {
        let row: Keywords = row?;
        let new = schema::Keyword {
            keyword: row.keyword,
        };
        if let Some(existing) = existing_keywords.remove(&row.id) {
            if existing.contents != new {
                tx.send(Operation::update_serialized::<schema::Keyword>(
                    existing.header,
                    &new,
                )?)?;
            }
        } else {
            tx.send(Operation::insert_serialized::<schema::Keyword>(
                Some(&row.id),
                &new,
            )?)?;
        }
    }

    Ok(())
}

fn apply_category_changes(
    data_folder: &Path,
    tx: &std::sync::mpsc::SyncSender<Operation>,
    db: &Database,
) -> anyhow::Result<()> {
    let mut existing_categories = schema::Category::all(db)
        .query()?
        .into_iter()
        .map(|d| (d.header.id, d))
        .collect::<HashMap<_, _>>();
    let mut keywords =
        csv::Reader::from_reader(std::fs::File::open(data_folder.join("categories.csv"))?);
    for row in keywords.deserialize() {
        let row: Categories = row?;
        let new = schema::Category {
            category: row.category,
            created_at: row.created_at,
            description: row.description,
            path: row.path,
            slug: row.slug,
        };
        if let Some(existing) = existing_categories.remove(&row.id) {
            if existing.contents != new {
                tx.send(Operation::update_serialized::<schema::Category>(
                    existing.header,
                    &new,
                )?)?;
            }
        } else {
            tx.send(Operation::insert_serialized::<schema::Category>(
                Some(&row.id),
                &new,
            )?)?;
        }
    }

    Ok(())
}

/// Updates the Version collection and returns a mapping of version_id to their
/// crate id.
fn apply_version_changes(
    data_folder: &Path,
    tx: &std::sync::mpsc::SyncSender<Operation>,
    db: &Database,
) -> anyhow::Result<HashMap<u64, u64>> {
    println!("Parsing versions");
    let mut existing_versions = schema::Version::all(db)
        .query()?
        .into_iter()
        .map(|d| (d.header.id, d))
        .collect::<HashMap<_, _>>();
    let mut version_id_to_crate = HashMap::with_capacity(existing_versions.len());
    let mut versions =
        csv::Reader::from_reader(std::fs::File::open(data_folder.join("versions.csv"))?);
    for row in versions.deserialize() {
        let row: Versions = row?;
        version_id_to_crate.insert(row.id, row.crate_id);
        let new = schema::Version {
            crate_id: row.crate_id,
            checksum: row.checksum,
            created_at: row.created_at,
            updated_at: row.updated_at,
            crate_size: row.crate_size,
            downloads: row.downloads,
            features: row.features,
            license: row.license,
            links: row.links,
            version: row.num,
            published_by: row.published_by,
            yanked: row.yanked == Some('t'),
        };
        if let Some(existing) = existing_versions.remove(&row.id) {
            if existing.contents != new {
                tx.send(Operation::update_serialized::<schema::Version>(
                    existing.header,
                    &new,
                )?)?;
            }
        } else {
            tx.send(Operation::insert_serialized::<schema::Version>(
                Some(&row.id),
                &new,
            )?)?;
        }
    }

    Ok(version_id_to_crate)
}

fn apply_version_download_changes(
    data_folder: &Path,
    tx: &std::sync::mpsc::SyncSender<Operation>,
    db: &Database,
    version_crates: &HashMap<u64, u64>,
) -> anyhow::Result<()> {
    println!("Parsing version downloads");
    // We only want to import the most recent download numbers. We re-import the previous 7 days to adjust for any changes to download numbers.
    let last_imported = schema::VersionDownloads::all(db)
        .limit(1)
        .descending()
        .query()?
        .into_iter()
        .next()
        .map(|dl| dl.header.id.date - 7);

    let mut downloads = csv::Reader::from_reader(std::fs::File::open(
        data_folder.join("version_downloads.csv"),
    )?);
    for row in downloads.deserialize() {
        let row: VersionDownloads = row?;
        let date = parse_iso_date(&row.date)?;
        // 365 requires 9 bits.
        let date = CalendarDate::from(date);
        if last_imported.map_or(false, |last_imported| date < last_imported) {
            continue;
        }

        let key = VersionDownloadKey {
            date,
            version_id: row.version_id,
        };
        tx.send(Operation::overwrite_serialized::<
            schema::VersionDownloads,
            _,
        >(
            &key,
            &schema::VersionDownloads {
                crate_id: *version_crates.get(&row.version_id).ok_or_else(|| {
                    anyhow::anyhow!("invalid version download: unknown version_id")
                })?,
                downloads: row.downloads,
            },
        )?)?;
    }

    Ok(())
}

fn parse_iso_date(date: &str) -> anyhow::Result<time::Date> {
    let mut parts = date.split('-');
    let (Some(year), Some(month), Some(day)) = (parts.next(), parts.next(), parts.next())
        else { anyhow::bail!("invalid date format") };
    let year = year.parse::<i32>()?;
    let month = month.parse::<u8>()?;
    let month = Month::try_from(month)?;
    let day = day.parse::<u8>()?;
    Ok(Date::from_calendar_date(year, month, day)?)
}

#[derive(Deserialize, Clone, Debug)]
pub struct Crate {
    created_at: String,
    description: String,
    documentation: String,
    downloads: Option<u64>,
    homepage: String,
    id: u64,
    max_upload_size: Option<u64>,
    name: String,
    readme: String,
    repository: String,
    updated_at: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Categories {
    category: String,
    crates_cnt: u64,
    created_at: String,
    description: String,
    id: u64,
    path: String,
    slug: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CrateCategories {
    crate_id: u64,
    category_id: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CrateKeywords {
    keyword_id: u64,
    crate_id: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CrateOwners {
    crate_id: u64,
    created_at: String,
    created_by: Option<u64>,
    owner_id: u64,
    owner_kind: u8,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Keywords {
    crates_cnt: u64,
    created_at: String,
    id: u64,
    keyword: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct VersionDownloads {
    date: String,
    downloads: u64,
    version_id: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Versions {
    checksum: String,
    crate_id: u64,
    crate_size: Option<u64>,
    created_at: String,
    downloads: u64,
    features: String,
    id: u64,
    license: String,
    links: String,
    num: String,
    published_by: Option<u64>,
    updated_at: String,
    yanked: Option<char>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Users {
    gh_avatar: String,
    gh_id: u64,
    gh_login: String,
    id: u64,
    name: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Teams {
    avatar: String,
    github_id: u64,
    id: u64,
    login: String,
    name: String,
    org_id: u64,
}
