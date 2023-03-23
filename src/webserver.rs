use askama::Template;
use axum::extract::{RawQuery, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use bonsaidb::local::Database;

use serde::Deserialize;

use crate::cache::Cache;
use crate::CrateResult;

pub async fn run(database: Database, cache: Cache) -> anyhow::Result<()> {
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
        .serve(app.with_state((database, cache)).into_make_service())
        .await?;

    Ok(())
}

#[derive(Deserialize, Debug)]
struct Query {
    q: String,
}

async fn index(
    State((db, cache)): State<(Database, Cache)>,
    RawQuery(query): RawQuery,
) -> Response {
    if let Some(query) = query {
        let query = serde_urlencoded::from_str(&query).unwrap_or(Query { q: query });
        let results = super::query(&query.q, &db, &cache).unwrap();
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
