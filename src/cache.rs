use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, Weak};

use bonsaidb::core::schema::SerializedView;
use bonsaidb::local::Database;

use crate::schema::{CalendarDate, CratesByNormalizedName, DownloadsByDate};

#[derive(Debug, Clone)]
pub struct Cache {
    thread: flume::Sender<Command>,
    data: Arc<Data>,
}

impl Cache {
    pub fn new(database: Database) -> anyhow::Result<Self> {
        let (sender, receiver) = flume::unbounded();
        sender.send(Command::Refresh)?;
        let cache = Self {
            thread: sender,
            data: Arc::new(Data {
                database,
                crates: RwLock::default(),
                crates_by_name: RwLock::default(),
            }),
        };

        let cache_for_thread = Arc::downgrade(&cache.data);
        std::thread::Builder::new()
            .name(String::from("cacher"))
            .spawn(move || cache_thread(receiver, cache_for_thread))?;

        Ok(cache)
    }

    pub fn refresh(&self) -> anyhow::Result<()> {
        Ok(self.thread.send(Command::Refresh)?)
    }

    pub fn crates(&self) -> anyhow::Result<RwLockReadGuard<'_, HashMap<u64, CachedCrate>>> {
        self.data
            .crates
            .read()
            .map_err(|_| anyhow::anyhow!("crates rwlock poisoned"))
    }

    pub fn crates_by_name(&self) -> anyhow::Result<RwLockReadGuard<'_, HashMap<String, u64>>> {
        self.data
            .crates_by_name
            .read()
            .map_err(|_| anyhow::anyhow!("crates_by_name rwlock poisoned"))
    }
}

#[derive(Debug)]
struct Data {
    database: Database,
    crates: RwLock<HashMap<u64, CachedCrate>>,
    crates_by_name: RwLock<HashMap<String, u64>>,
}

impl Data {
    fn refresh_crates(&self) -> anyhow::Result<()> {
        let crates_by_name = CratesByNormalizedName::entries(&self.database).query()?;
        let recent_downloads_start =
            time::OffsetDateTime::now_utc().date() - time::Duration::days(30);
        let mut recent_downloads_by_crate = HashMap::with_capacity(crates_by_name.len());
        for mapping in DownloadsByDate::entries(&self.database)
            .with_key_range((CalendarDate::from(recent_downloads_start), 0)..)
            .reduce_grouped()?
        {
            let crate_downloads = recent_downloads_by_crate
                .entry(mapping.key.1)
                .or_insert(0_u64);
            *crate_downloads += mapping.value;
        }

        let (crates, crates_by_name) = crates_by_name
            .into_iter()
            .map(|mapping| {
                let id = mapping.source.id.deserialize().expect("invalid id");
                let recent_downloads = recent_downloads_by_crate.get(&id).copied().unwrap_or(0);
                (
                    (
                        id,
                        CachedCrate {
                            name: mapping.value.name,
                            description: mapping.value.description,
                            downloads: mapping.value.downloads,
                            keywords: mapping.value.keywords,
                            recent_downloads,
                        },
                    ),
                    (mapping.key, id),
                )
            })
            .unzip();

        let mut cached_crates = self
            .crates
            .write()
            .map_err(|_| anyhow::anyhow!("crates rwlock poisoned"))?;
        *cached_crates = crates;
        drop(cached_crates);

        let mut cached_crates = self
            .crates_by_name
            .write()
            .map_err(|_| anyhow::anyhow!("crates_by_name rwlock poisoned"))?;
        *cached_crates = crates_by_name;
        drop(cached_crates);

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CachedCrate {
    pub name: String,
    pub description: String,
    pub keywords: HashSet<u64>,
    pub downloads: u64,
    pub recent_downloads: u64,
}

enum Command {
    Refresh,
}

fn cache_thread(commands: flume::Receiver<Command>, cache: Weak<Data>) -> anyhow::Result<()> {
    while let Ok(command) = commands.recv() {
        if let Some(cache) = cache.upgrade() {
            match command {
                Command::Refresh => {
                    cache.refresh_crates()?;
                }
            }
        } else {
            break;
        }
    }

    Ok(())
}
