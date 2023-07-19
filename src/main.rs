use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    time::Instant,
};

use bonsaidb::{
    core::{connection::StorageConnection, key::Key, schema::SerializedView},
    local::{
        config::{Builder, StorageConfiguration},
        Database, Storage,
    },
};
use tantivy::{
    collector::TopDocs,
    query::QueryParser,
    schema::{Field, Schema, Value, FAST, INDEXED, STORED, TEXT},
    Index,
};

use crate::cache::{Cache, CachedCrate};

mod cache;
mod dump;
mod schema;
mod webserver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let storage = Storage::open(
        StorageConfiguration::default()
            .path("delve-rs.bonsaidb")
            .with_schema::<schema::CrateIndex>()?,
    )?;
    let db = storage.create_database::<schema::CrateIndex>("delve", true)?;
    let cache = Cache::new(db.clone())?;

    let mut search_schema = tantivy::schema::Schema::builder();
    let id = search_schema.add_u64_field("id", INDEXED | STORED);
    let name = search_schema.add_text_field("name", TEXT);
    let description = search_schema.add_text_field("description", TEXT);
    let readme = search_schema.add_text_field("readme", TEXT);
    let search_schema = search_schema.build();

    std::fs::create_dir("delve-rs.bonsaidb/tantivy")?;
    let index = SearchIndex {
        index: Index::create_in_dir("delve-rs.bonsaidb/tantivy", search_schema.clone())?,
        id,
        name,
        description,
        readme,
    };

    if std::env::args().len() <= 1 {
        dump::import_continuously(db, cache, index).await?;
        println!("About to exit.");
        // webserver::run(db, cache, index).await?;
    } else {
        let q = std::env::args().nth(1).expect("length checked");
        let start = Instant::now();
        query(&q, &db, &cache, &index)?;
        println!("Query executed in {}us", start.elapsed().as_micros());
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct SearchIndex {
    pub index: Index,
    pub id: Field,
    pub name: Field,
    pub description: Field,
    pub readme: Field,
}

#[derive(Key, Debug, Clone)]
struct Foo<'k> {
    string: Cow<'k, str>,
}

#[derive(Debug)]
struct CrateResult {
    confidence: f32,
    popularity: f32,
    result: CachedCrate,
}

fn query(
    query: &str,
    db: &Database,
    cache: &Cache,
    index: &SearchIndex,
) -> anyhow::Result<Vec<CrateResult>> {
    let mut crate_scores = HashMap::new();

    let mut total_words = 0;
    for word in query.split_ascii_whitespace() {
        if word.is_empty() {
            continue;
        }

        total_words += 1;
        let normalized_query = schema::Crate::normalized_name(word);
        let lowercase_query = word.to_ascii_lowercase();

        // Build matches based on the crate names
        let crates_by_name = cache.crates_by_name()?;
        for (normalized_name, crate_id) in crates_by_name.iter() {
            if let Some(name_score) = TextScore::score(&normalized_query, normalized_name) {
                let score = crate_scores
                    .entry(*crate_id)
                    .or_insert_with(QueryScore::default);
                score.name.push(name_score);
                score.matched_words.insert(word);
            }
        }

        // Adjust matches based on keyword matches.
        for mapping in schema::Keywords::entries(db)
            .with_key_prefix(&lowercase_query)
            .query()?
        {
            if let Some(keyword_score) = TextScore::score(word, &mapping.key) {
                for crate_with_keyword in schema::CratesByKeyword::entries(db)
                    .with_key(&mapping.source.id.deserialize::<u64>()?)
                    .query()?
                {
                    let score = crate_scores
                        .entry(crate_with_keyword.source.id.deserialize::<u64>()?)
                        .or_insert_with(QueryScore::default);
                    score.keywords.push(keyword_score);
                    score.matched_words.insert(word);
                }
            }
        }
    }

    // Search for crates that contain this word in their description/readme
    let search_index = index.index.reader()?;
    let searcher = search_index.searcher();
    let query_parser = QueryParser::for_index(
        &index.index,
        vec![index.name, index.description, index.readme],
    );
    if let Ok(query) = query_parser.parse_query(query) {
        for (search_score, doc) in search_index
            .searcher()
            .search(&query, &TopDocs::with_limit(1_000))?
        {
            if let Ok(doc) = searcher.doc(doc) {
                if let Some(Value::U64(crate_id)) = doc.get_first(index.id) {
                    let score = crate_scores
                        .entry(*crate_id)
                        .or_insert_with(QueryScore::default);
                    score.index_score = Some(search_score);
                }
            }
        }
    }
    // for mapping in schema::CratesByWord::entries(db).with_key(word).query()? {
    //     let score = crate_scores
    //         .entry(mapping.source.id.deserialize::<u64>()?)
    //         .or_insert_with(QueryScore::default);
    //     score.word_locations += mapping.value;
    //     score.matched_words.insert(word);
    // }

    // Sort the result set and get rid of everything that didn't match all
    // search terms.
    let mut results = Vec::<(f32, f32, u64)>::with_capacity(crate_scores.len().max(1000));
    for (id, score) in &crate_scores {
        if score.matched_words.len() == total_words || score.index_score.is_some() {
            let calculated = score.calculated_score();
            let insert_at =
                match results.binary_search_by(|(ascore, _, _)| calculated.total_cmp(ascore)) {
                    Ok(insert_at) => insert_at,
                    Err(insert_at) => insert_at,
                };
            if insert_at < 1000 {
                results.insert(insert_at, (calculated, 0.0, *id));
                if results.len() > 1000 {
                    results.truncate(1000);
                }
            }
        }
    }

    if results.is_empty() {
        return Ok(Vec::new());
    }

    // Build a confidence score
    let maximum_confidence = results.first().expect("at least one result").0;
    let mut total_downloads = 0;
    let mut total_recent_downloads = 0;
    let mut all_crates = HashMap::with_capacity(results.len());
    let crates = cache.crates()?;
    for (_, _, crate_id) in &results {
        if let Some(c) = crates.get(crate_id) {
            total_downloads += c.downloads;
            total_recent_downloads += c.recent_downloads;

            all_crates.insert(*crate_id, c.clone());
        }
    }

    // Adjust the scores based on percentage of downloads across these search results.
    for (confidence, popularity, id) in &mut results {
        let Some(c) = all_crates.get(id) else { continue };

        // Adjust confidence to be a percentage of the highest crate
        *confidence /= maximum_confidence;

        // Prioritize crates that have more recent downloads
        let all_time_downloads_percent = c.downloads as f32 / total_downloads as f32;
        let recent_downloads_percent = c.recent_downloads as f32 / total_recent_downloads as f32;
        *popularity = (recent_downloads_percent * 4. + all_time_downloads_percent) / 5.;
    }

    let maximum_popularity = results
        .iter()
        .map(|(_, popularity, _)| *popularity)
        .reduce(|a, b| {
            if a.total_cmp(&b) == Ordering::Greater {
                a
            } else {
                b
            }
        })
        .unwrap_or(1.);

    results.sort_by(|a, b| {
        (b.0 * (b.1 / maximum_popularity)).total_cmp(&(a.0 * (a.1 / maximum_popularity)))
    });

    let mut final_results = Vec::with_capacity(results.len());
    for (confidence, popularity, id) in results {
        let Some(c) = all_crates.remove(&id) else { continue };
        final_results.push(CrateResult {
            confidence,
            popularity,
            result: c,
        });
    }

    Ok(final_results)
}

#[derive(Default, Debug)]
struct QueryScore<'a> {
    matched_words: HashSet<&'a str>,
    index_score: Option<f32>,
    name: Vec<TextScore>,
    keywords: Vec<TextScore>,
    category: Vec<TextScore>,
}

impl<'a> QueryScore<'a> {
    fn calculated_score(&self) -> f32 {
        // self.name
        //     .iter()
        //     .map(TextScore::calculated_score)
        //     .sum::<f32>()
        //     * 100.
        //     + (self
        //         .keywords
        //         .iter()
        //         .map(TextScore::calculated_score)
        //         .sum::<f32>()
        //         * 50.)
        //     + self
        //         .category
        //         .iter()
        //         .map(TextScore::calculated_score)
        //         .sum::<f32>()
        //         * 50.
        //     +
        self.index_score.unwrap_or(0.)
    }
}

#[derive(Clone, Copy, Debug)]
enum TextScore {
    ExactMatch,
    StartsWith { match_percent: f32 },
    EndsWith { match_percent: f32 },
    Contains { match_percent: f32 },
}

impl TextScore {
    pub fn score(needle: &str, haystack: &str) -> Option<Self> {
        let same_length = needle.len() == haystack.len();
        haystack
            .find(needle)
            .map(|offset| Self::score_offset(offset, same_length, haystack.len(), needle.len()))
            .or_else(|| {
                needle.find(haystack).map(|offset| {
                    Self::score_offset(offset, same_length, needle.len(), haystack.len())
                })
            })
    }

    fn score_offset(
        offset: usize,
        same_length: bool,
        haystack_len: usize,
        needle_len: usize,
    ) -> Self {
        let match_percent = needle_len as f32 / haystack_len as f32;
        if offset == 0 {
            if same_length {
                Self::ExactMatch
            } else {
                Self::StartsWith { match_percent }
            }
        } else if offset == haystack_len - needle_len {
            Self::EndsWith { match_percent }
        } else {
            Self::Contains { match_percent }
        }
    }

    fn calculated_score(&self) -> f32 {
        match self {
            TextScore::ExactMatch => 100.,
            TextScore::StartsWith { match_percent } => 10. * match_percent * match_percent,
            TextScore::EndsWith { match_percent } => 10. * match_percent * match_percent,
            TextScore::Contains { match_percent } => *match_percent * *match_percent,
        }
    }
}
