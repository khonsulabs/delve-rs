use askama::Template;
use axum::{
    extract::{RawQuery, State},
    http::header::CONTENT_TYPE,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use bonsaidb::local::Database;

use serde::Deserialize;

use crate::{cache::Cache, CrateResult, SearchIndex};

pub(super) async fn run(
    database: Database,
    cache: Cache,
    search_index: SearchIndex,
) -> anyhow::Result<()> {
    // build our application with a single route
    let app = axum::Router::new()
        .route("/about", get(|| async { "Hello, World!" }))
        .route(
            "/style.css",
            get(|| async {
                (
                    [(CONTENT_TYPE, "text/css")],
                    include_str!("./assets/style.css"),
                )
            }),
        )
        .route("/:slug", get(|| async { "Hello, Slug!" }))
        .route("/", get(index));

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(
            app.with_state((database, cache, search_index))
                .into_make_service(),
        )
        .await?;

    Ok(())
}

#[derive(Deserialize, Debug)]
struct Query {
    q: String,
}

async fn index(
    State((db, cache, search_index)): State<(Database, Cache, SearchIndex)>,
    RawQuery(query): RawQuery,
) -> Response {
    if let Some(query) = query {
        let query = serde_urlencoded::from_str(&query).unwrap_or(Query { q: query });
        let results = super::query(&query.q, &db, &cache, &search_index).unwrap();
        Html(
            SearchResults {
                query: query.q,
                results,
            }
            .render()
            .expect("invalid template data"),
        )
        .into_response()
        // Html(format!(
        //     "<ol>{}</ol>",
        //     results
        //         .into_iter()
        //         .map(|result| {
        //             format!(
        //                 "<li><a href=\"https://crates.io/crates/{name}\">{name}</a> - {}</li>",
        //                 result.score,
        //                 name = result.result.name,
        //             )
        //         })
        //         .collect::<String>()
        // ))
        // .into_response()
    } else {
        Html(Index.render().expect("invalid template data")).into_response()
    }
}

#[derive(Template, Debug)]
#[template(path = "results.html")]
struct SearchResults {
    query: String,
    results: Vec<CrateResult>,
}

#[derive(Template, Debug)]
#[template(path = "index.html")]
struct Index;
