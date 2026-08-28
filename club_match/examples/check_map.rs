//! Regression harness for [`club_match::match_name`].
//!
//! `cache/club_map.json` is an accumulated record of Transfermarkt club names
//! and the dataset ids they were resolved to. Re-running the matcher over those
//! names says whether a change to the normalization or the noise list still
//! reproduces every mapping the tools have relied on.
//!
//!     cargo run --release -p club_match --example check_map -- \
//!         [data_dir] [club_map.json]
//!
//! Disagreements are printed; agreements and "no longer matched by name" (the
//! entry may have come from an override) are only counted.

use std::path::PathBuf;

use club_match::{NameMatch, build_club_index, collisions, load_club_map, match_name};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let data_dir = PathBuf::from(args.next().unwrap_or_else(|| "data".into()));
    let map_path =
        PathBuf::from(args.next().unwrap_or_else(|| "history_scraper/cache/club_map.json".into()));

    let index = build_club_index(&data_dir)?;
    let cached = load_club_map(&map_path);
    println!(
        "{} clubs, {} aliases; checking {} cached mappings",
        index.display.len(),
        index.aliases.len(),
        cached.resolved.len()
    );

    let clashes = collisions(&index);
    println!("\n{} club(s) share their identifying tokens with another:", clashes.len());
    for group in &clashes {
        let names: Vec<String> = group
            .iter()
            .map(|id| format!("{id}={}", index.display.get(id).map(String::as_str).unwrap_or("?")))
            .collect();
        println!("  {}", names.join(" | "));
    }
    println!();

    let (mut agree, mut wrong, mut missed, mut ambiguous, mut unnamed) = (0, 0, 0, 0, 0);
    let mut ids: Vec<_> = cached.resolved.keys().copied().collect();
    ids.sort_unstable();
    for tm in ids {
        let expected = cached.resolved[&tm];
        let Some(name) = cached.names.get(&tm) else {
            unnamed += 1;
            continue;
        };
        match match_name(&index, name) {
            NameMatch::Unique(got) if got == expected => agree += 1,
            NameMatch::Unique(got) => {
                wrong += 1;
                println!(
                    "  WRONG  tm {tm} {name:?} -> {got} ({}), cached {expected} ({})",
                    index.display.get(&got).map(String::as_str).unwrap_or("?"),
                    index.display.get(&expected).map(String::as_str).unwrap_or("?"),
                );
            }
            NameMatch::Ambiguous(opts) => {
                ambiguous += 1;
                println!(
                    "  AMBIG  tm {tm} {name:?} -> {:?}, cached {expected} ({})",
                    opts,
                    index.display.get(&expected).map(String::as_str).unwrap_or("?"),
                );
            }
            NameMatch::None => {
                missed += 1;
                println!(
                    "  MISS   tm {tm} {name:?}, cached {expected} ({})",
                    index.display.get(&expected).map(String::as_str).unwrap_or("?"),
                );
            }
        }
    }
    println!(
        "\nagree {agree}, wrong {wrong}, ambiguous {ambiguous}, missed {missed}, \
         no cached name {unnamed}"
    );
    Ok(())
}
