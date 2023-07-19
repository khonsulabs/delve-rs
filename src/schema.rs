use std::collections::{HashMap, HashSet};
use std::iter::{Peekable, Sum};
use std::ops::AddAssign;
use std::str::Chars;

use bonsaidb::core::connection::RangeRef;
use bonsaidb::core::document::{CollectionDocument, Emit};
use bonsaidb::core::key::Key;
use bonsaidb::core::schema::{
    Collection, CollectionViewSchema, ReduceResult, Schema, View, ViewMapResult, ViewMappedValue,
};
use serde::{Deserialize, Serialize};

#[derive(Schema, Debug)]
#[schema(name = "delve-rs", collections = [Crate, Keyword, Category, ImportState, Version, VersionDownloads])]
pub struct CrateIndex;

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[collection(name = "import-state", primary_key = ())]
pub struct ImportState {
    pub downloaded_last_modified: Option<String>,
    #[serde(default)]
    pub last_dump_imported: Option<String>,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[collection(name = "crates", primary_key = u64, views = [CratesByNormalizedName, CratesByKeyword])]
pub struct Crate {
    pub created_at: String,
    pub description: String,
    pub documentation: String,
    pub downloads: Option<u64>,
    pub homepage: String,
    pub max_upload_size: Option<u64>,
    pub name: String,
    pub readme: String,
    pub repository: String,
    pub updated_at: String,
    pub keywords: HashSet<u64>,
    pub category_ids: HashSet<u64>,
    pub owners: HashSet<OwnerId>,
}

impl Crate {
    pub fn normalized_name(name: &str) -> String {
        name.chars()
            .map(|ch| {
                if ch == '-' {
                    '_'
                } else {
                    ch.to_ascii_lowercase()
                }
            })
            .collect()
    }
}

#[derive(View, Clone, Debug)]
#[view(name = "by-name", collection = Crate, key = String, value = CrateInfo)]
pub struct CratesByNormalizedName;

impl CollectionViewSchema for CratesByNormalizedName {
    type View = Self;

    fn version(&self) -> u64 {
        1
    }

    fn lazy(&self) -> bool {
        false
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key_and_value(
            Crate::normalized_name(&document.contents.name),
            CrateInfo {
                name: document.contents.name,
                description: document.contents.description,
                keywords: document.contents.keywords,
                downloads: document.contents.downloads.unwrap_or(0),
            },
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CrateInfo {
    pub name: String,
    pub downloads: u64,
    pub description: String,
    pub keywords: HashSet<u64>,
}

#[derive(View, Clone, Debug)]
#[view(name = "by-keyword", collection = Crate, key = u64, value = u64)]
pub struct CratesByKeyword;

impl CollectionViewSchema for CratesByKeyword {
    type View = Self;

    fn lazy(&self) -> bool {
        false
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document
            .contents
            .keywords
            .into_iter()
            .map(|id| document.header.emit_key_and_value(id, 1))
            .collect()
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum OwnerId {
    User(u64),
    Team(u64),
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[collection(name = "keywords", primary_key = u64, views = [Keywords])]
pub struct Keyword {
    pub keyword: String,
}

#[derive(View, Clone, Debug)]
#[view(name = "by-keyword", collection = Keyword, key = String)]
pub struct Keywords;

impl CollectionViewSchema for Keywords {
    type View = Self;

    fn lazy(&self) -> bool {
        false
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key(document.contents.keyword)
    }
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[collection(name = "categories", primary_key = u64)]
pub struct Category {
    pub category: String,
    pub created_at: String,
    pub description: String,
    pub path: String,
    pub slug: String,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[collection(name = "versions", primary_key = u64, views = [VersionsByCrate])]
pub struct Version {
    pub crate_id: u64,
    pub checksum: String,
    pub created_at: String,
    pub updated_at: String,
    pub crate_size: Option<u64>,
    pub downloads: u64,
    pub features: String,
    pub license: String,
    pub links: String,
    pub version: String,
    pub published_by: Option<u64>,
    pub yanked: bool,
}

#[derive(View, Clone, Debug)]
#[view(name = "by-crate", collection = Version, key = u64, value = VersionSummary)]
pub struct VersionsByCrate;

impl CollectionViewSchema for VersionsByCrate {
    type View = Self;

    fn lazy(&self) -> bool {
        false
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key_and_value(
            document.contents.crate_id,
            VersionSummary {
                version: document.contents.version,
                yanked: document.contents.yanked,
            },
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionSummary {
    pub version: String,
    pub yanked: bool,
}

#[derive(Collection, Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[collection(name = "version-downloads", primary_key = VersionDownloadKey, views = [DownloadsByDate])]
pub struct VersionDownloads {
    pub crate_id: u64,
    pub downloads: u64,
}

#[derive(Key, Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct VersionDownloadKey {
    pub version_id: u64,
    pub date: CalendarDate,
}

#[derive(View, Clone, Debug)]
#[view(name = "by-date", collection = VersionDownloads, key = (CalendarDate, u64), value = u64)]
pub struct DownloadsByDate;

impl CollectionViewSchema for DownloadsByDate {
    type View = Self;

    fn version(&self) -> u64 {
        1
    }

    fn lazy(&self) -> bool {
        false
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document.header.emit_key_and_value(
            (document.header.id.date, document.contents.crate_id),
            document.contents.downloads,
        )
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}

#[derive(Key, Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DateAndCrate {
    pub date: CalendarDate,
    pub crate_id: u64,
}

impl DateAndCrate {
    pub fn range_for_after_date(date: CalendarDate) -> RangeRef<'static, DateAndCrate> {
        RangeRef::from(DateAndCrate { date, crate_id: 0 }..)
    }
}

#[derive(Key, Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CalendarDate(u32);

impl From<time::Date> for CalendarDate {
    fn from(value: time::Date) -> Self {
        let year = u32::try_from(value.year()).expect("negative years are unsupported");
        assert!(year < 2_u32.pow(22));
        Self(year << 9 | value.ordinal() as u32)
    }
}

impl From<CalendarDate> for time::Date {
    fn from(value: CalendarDate) -> Self {
        let year = i32::try_from(value.0 >> 9).expect("year out of range");
        let ordinal = (value.0 & 0x1FF) as u16;
        time::Date::from_ordinal_date(year, ordinal).expect("date out of range")
    }
}

impl std::ops::Sub<u32> for CalendarDate {
    type Output = Self;

    fn sub(self, rhs: u32) -> Self::Output {
        let date = time::Date::from(self);
        Self::from(date - time::Duration::days(rhs as i64))
    }
}
