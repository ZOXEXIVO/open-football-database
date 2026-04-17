//! `compiler` — walks the tree under `../data/` and emits a single gzipped
//! `database.db` document consumable by the main game's loaders.
//!
//! Layout read:
//!   data/continents.json
//!   data/countries.json
//!   data/national_competitions.json
//!   data/{country_code}/names.json
//!   data/{country_code}/{league_slug}/league.json
//!   data/{country_code}/{league_slug}/{club_slug}/club.json
//!   data/{country_code}/{league_slug}/{club_slug}/players/*.json
//!
//! Output (gzipped JSON):
//!   {
//!     "version": "0.01",
//!     "continents": [ ... ],
//!     "countries":  [ ... ],
//!     "national_competitions": [ ... ],
//!     "leagues":    [ { ...league.json fields..., "country_code": "mt" }, ... ],
//!     "clubs":      [ { ...club.json fields...,   "country_code": "mt",
//!                       "teams": [ { ..., "league_id": 120 } ] }, ... ],
//!     "names":      [ { ...names.json fields...,  "country_code": "mt" }, ... ],
//!     "players":    [ { ...player fields... }, ... ]
//!   }
//!
//! Path-derived context (`country_code`, `league_id`) is baked into each record
//! so the runtime loader never needs the on-disk tree to reconstruct relationships.

use std::env;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Map, Value};

const OUTPUT_VERSION: &str = "0.01";
const DEFAULT_DATA_DIR: &str = "../data";
const DEFAULT_OUT_FILE: &str = "database.db";

struct Args {
    data_dir: PathBuf,
    out_file: PathBuf,
}

fn parse_args() -> Args {
    let mut data_dir: Option<PathBuf> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(
                    it.next().expect("--data-dir needs a value"),
                ));
            }
            "--out" => {
                out_file = Some(PathBuf::from(
                    it.next().expect("--out needs a value"),
                ));
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown arg: {other}");
                print_help();
                std::process::exit(2);
            }
        }
    }
    Args {
        data_dir: data_dir.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR)),
        out_file: out_file.unwrap_or_else(|| PathBuf::from(DEFAULT_OUT_FILE)),
    }
}

fn print_help() {
    println!(
        "compiler — build database.db from the data tree\n\n\
         Usage: compiler [--data-dir PATH] [--out PATH]\n\n\
         Options:\n\
         \x20 --data-dir PATH   Source tree root (default: ./{DEFAULT_DATA_DIR})\n\
         \x20 --out PATH        Output file (default: ./{DEFAULT_OUT_FILE})\n"
    );
}

struct Counts {
    continents: usize,
    countries: usize,
    national_competitions: usize,
    leagues: usize,
    clubs: usize,
    names: usize,
    players: usize,
}

fn main() -> Result<()> {
    let args = parse_args();

    if !args.data_dir.is_dir() {
        anyhow::bail!("data directory not found: {}", args.data_dir.display());
    }

    // Top-level static tables, each expected to be a JSON array.
    let continents = read_top_level_array(&args.data_dir, "continents.json")?;
    let countries = read_top_level_array(&args.data_dir, "countries.json")?;
    let national_competitions =
        read_top_level_array(&args.data_dir, "national_competitions.json")?;

    let mut leagues: Vec<Value> = Vec::new();
    let mut clubs: Vec<Value> = Vec::new();
    let mut names: Vec<Value> = Vec::new();
    let mut players: Vec<Value> = Vec::new();

    let mut country_entries: Vec<_> = read_sorted_dir(&args.data_dir)?;
    country_entries.retain(|p| p.is_dir());

    for country_dir in country_entries {
        let country_code = dir_name(&country_dir)?.to_string();

        let names_path = country_dir.join("names.json");
        if names_path.is_file() {
            let mut v = read_json(&names_path)?;
            insert_country_code(&mut v, &country_code);
            names.push(v);
        }

        let mut league_entries = read_sorted_dir(&country_dir)?;
        league_entries.retain(|p| p.is_dir());

        for league_dir in league_entries {
            let league_json = league_dir.join("league.json");
            if !league_json.is_file() {
                continue;
            }

            let mut league_val = read_json(&league_json)?;
            let league_id = league_val
                .get("id")
                .and_then(|v| v.as_u64())
                .with_context(|| format!("missing/invalid id in {}", league_json.display()))?;
            insert_country_code(&mut league_val, &country_code);
            leagues.push(league_val);

            let mut club_entries = read_sorted_dir(&league_dir)?;
            club_entries.retain(|p| p.is_dir());

            for club_dir in club_entries {
                let club_json = club_dir.join("club.json");
                if !club_json.is_file() {
                    continue;
                }

                let mut club_val = read_json(&club_json)?;
                insert_country_code(&mut club_val, &country_code);
                stamp_main_team_league_id(&mut club_val, league_id);
                clubs.push(club_val);

                let players_dir = club_dir.join("players");
                if players_dir.is_dir() {
                    let mut player_files = read_sorted_dir(&players_dir)?;
                    player_files.retain(|p| {
                        p.is_file()
                            && p.extension()
                                .and_then(|s| s.to_str())
                                .map(|s| s.eq_ignore_ascii_case("json"))
                                .unwrap_or(false)
                    });
                    for player_path in player_files {
                        let v = read_json(&player_path)?;
                        players.push(v);
                    }
                }
            }
        }
    }

    let counts = Counts {
        continents: continents.len(),
        countries: countries.len(),
        national_competitions: national_competitions.len(),
        leagues: leagues.len(),
        clubs: clubs.len(),
        names: names.len(),
        players: players.len(),
    };

    // Build the top-level document. Use a Map to keep a stable key order.
    let mut root = Map::new();
    root.insert("version".into(), Value::String(OUTPUT_VERSION.into()));
    root.insert("continents".into(), Value::Array(continents));
    root.insert("countries".into(), Value::Array(countries));
    root.insert(
        "national_competitions".into(),
        Value::Array(national_competitions),
    );
    root.insert("leagues".into(), Value::Array(leagues));
    root.insert("clubs".into(), Value::Array(clubs));
    root.insert("names".into(), Value::Array(names));
    root.insert("players".into(), Value::Array(players));
    let document = Value::Object(root);

    let uncompressed = serde_json::to_vec(&document).context("serialize output JSON")?;

    let out_tmp = args.out_file.with_extension("db.tmp");
    {
        let file = File::create(&out_tmp)
            .with_context(|| format!("create {}", out_tmp.display()))?;
        let mut enc = GzEncoder::new(BufWriter::new(file), Compression::default());
        enc.write_all(&uncompressed).context("gzip write")?;
        enc.finish().context("gzip finish")?.flush().ok();
    }
    fs::rename(&out_tmp, &args.out_file).with_context(|| {
        format!("rename {} -> {}", out_tmp.display(), args.out_file.display())
    })?;

    let compressed_size = fs::metadata(&args.out_file)?.len();
    println!(
        "wrote {}: v{} — {} continents, {} countries, {} national_competitions, \
         {} leagues, {} clubs, {} names, {} players \
         ({:.2} MB uncompressed, {:.2} MB gzipped)",
        args.out_file.display(),
        OUTPUT_VERSION,
        counts.continents,
        counts.countries,
        counts.national_competitions,
        counts.leagues,
        counts.clubs,
        counts.names,
        counts.players,
        uncompressed.len() as f64 / 1_048_576.0,
        compressed_size as f64 / 1_048_576.0,
    );

    Ok(())
}

fn read_sorted_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|e| e.path())
        .collect();
    // Sorted output makes the compiled artifact deterministic across runs/platforms.
    out.sort();
    Ok(out)
}

fn dir_name(p: &Path) -> Result<&str> {
    p.file_name()
        .and_then(|s| s.to_str())
        .with_context(|| format!("non-utf8 path component: {}", p.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse {}", path.display()))
}

/// Read a top-level JSON file expected to contain an array. Returns the array's
/// items as `Vec<Value>`, or errors if the file isn't present or isn't an array.
fn read_top_level_array(data_dir: &Path, file_name: &str) -> Result<Vec<Value>> {
    let path = data_dir.join(file_name);
    let v = read_json(&path)?;
    match v {
        Value::Array(items) => Ok(items),
        _ => anyhow::bail!("{} must contain a JSON array", path.display()),
    }
}

fn insert_country_code(v: &mut Value, code: &str) {
    if let Some(obj) = v.as_object_mut() {
        obj.insert("country_code".into(), Value::String(code.into()));
    }
}

fn stamp_main_team_league_id(club: &mut Value, league_id: u64) {
    let Some(teams) = club.get_mut("teams").and_then(|v| v.as_array_mut()) else {
        return;
    };
    for team in teams {
        let is_main = team
            .get("team_type")
            .and_then(|v| v.as_str())
            .map(|s| s == "Main")
            .unwrap_or(false);
        if is_main {
            if let Some(obj) = team.as_object_mut() {
                obj.insert("league_id".into(), Value::from(league_id));
            }
        }
    }
}
