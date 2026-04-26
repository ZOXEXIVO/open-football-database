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
//!   data/{country_code}/free_agents/*.json   (optional — clubless players)
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

use std::collections::HashSet;

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Map, Value};

const OUTPUT_VERSION: &str = "0.01";
const DEFAULT_DATA_DIR: &str = "../data";
const DEFAULT_OUT_FILE: &str = r"D:\Projects\open-football\src\database\src\data\database.db";

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

    // Satellite directories (those with `parent_club` in club.json) are
    // collected here and folded into their parents after the directory walk
    // completes — that way merges work regardless of file/directory ordering.
    let mut satellites: Vec<SatelliteSpec> = Vec::new();

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

        // Free agents live in `data/{cc}/free_agents/*.json`. They have no
        // league or club, so they bypass the league/club walk below and feed
        // straight into the players list. The hydrator distinguishes them
        // by the absent (or zero) `club_id` field.
        let free_agents_dir = country_dir.join("free_agents");
        if free_agents_dir.is_dir() {
            let mut player_files = read_sorted_dir(&free_agents_dir)?;
            player_files.retain(|p| {
                p.is_file()
                    && p.extension()
                        .and_then(|s| s.to_str())
                        .map(|s| s.eq_ignore_ascii_case("json"))
                        .unwrap_or(false)
            });
            for player_path in player_files {
                let v = read_json(&player_path)?;
                validate_player_history(&v, &player_path)?;
                players.push(v);
            }
        }

        let mut league_entries = read_sorted_dir(&country_dir)?;
        league_entries.retain(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|s| s.to_str())
                    .map(|s| s != "free_agents")
                    .unwrap_or(true)
        });

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

                // Satellite directory: defer the merge into its parent until
                // every directory has been read.
                if let Some(parent) = take_parent_club(&mut club_val) {
                    let mut player_records: Vec<Value> = Vec::new();
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
                            validate_player_history(&v, &player_path)?;
                            player_records.push(v);
                        }
                    }

                    satellites.push(SatelliteSpec {
                        parent_id: parent.id,
                        team_type: parent.team_type,
                        league_id,
                        satellite_club: club_val,
                        players: player_records,
                        source_path: club_json.clone(),
                    });
                    continue;
                }

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
                        validate_player_history(&v, &player_path)?;
                        players.push(v);
                    }
                }
            }
        }
    }

    // Fold satellite clubs (those with `parent_club`) into their parents.
    // Done after the full walk so the parent club entry is guaranteed to exist
    // regardless of directory ordering.
    apply_satellites(&mut clubs, &mut players, satellites)?;

    // Cross-check every `history[].club_id` against the loaded club set.
    // Unknown ids still compile (the hydrator tolerates them by rendering empty
    // club/league cells) but almost always indicate a typo in scraper output.
    let known_club_ids: HashSet<u64> = clubs
        .iter()
        .filter_map(|c| c.get("id").and_then(|v| v.as_u64()))
        .collect();
    let mut unknown_refs: Vec<(u64, u64)> = Vec::new();
    for p in &players {
        let Some(pid) = p.get("id").and_then(|v| v.as_u64()) else {
            continue;
        };
        let Some(history) = p.get("history").and_then(|v| v.as_array()) else {
            continue;
        };
        for item in history {
            if let Some(cid) = item.get("c").and_then(|v| v.as_u64()) {
                if !known_club_ids.contains(&cid) {
                    unknown_refs.push((pid, cid));
                }
            }
        }
    }
    if !unknown_refs.is_empty() {
        eprintln!(
            "warning: {} history entr{} reference unknown club_id:",
            unknown_refs.len(),
            if unknown_refs.len() == 1 { "y" } else { "ies" },
        );
        for (pid, cid) in unknown_refs.iter().take(20) {
            eprintln!("  player {pid} -> club_id {cid}");
        }
        if unknown_refs.len() > 20 {
            eprintln!("  ... and {} more", unknown_refs.len() - 20);
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

/// Sanity-check the `history` field on a player record. Each entry must be an
/// object with numeric `season` and `club_id`. Shape errors are fatal — they
/// mean the scraper produced garbage and the resulting DB would silently drop
/// rows at hydration time.
fn validate_player_history(v: &Value, path: &Path) -> Result<()> {
    let Some(history) = v.get("history") else {
        return Ok(());
    };
    let arr = history.as_array().with_context(|| {
        format!("history in {} must be an array", path.display())
    })?;
    for (i, item) in arr.iter().enumerate() {
        let obj = item.as_object().with_context(|| {
            format!("history[{i}] in {} must be an object", path.display())
        })?;
        obj.get("s").and_then(|v| v.as_u64()).with_context(|| {
            format!(
                "history[{i}].s (season) in {} must be an unsigned integer",
                path.display()
            )
        })?;
        obj.get("c").and_then(|v| v.as_u64()).with_context(|| {
            format!(
                "history[{i}].c (club_id) in {} must be an unsigned integer",
                path.display()
            )
        })?;
    }
    Ok(())
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

/// Pending merge of a satellite directory into a parent club.
struct SatelliteSpec {
    /// Parent club id named in `parent_club.id`.
    parent_id: u64,
    /// Sub-team slot to fill in the parent (typically `"B"`).
    team_type: String,
    /// League id of the directory the satellite lives in — gets stamped onto
    /// the new sub-team so it competes in that league.
    league_id: u64,
    /// The satellite's full club.json (with `parent_club` already removed).
    satellite_club: Value,
    /// Player records read from the satellite's `players/` folder.
    players: Vec<Value>,
    /// Original satellite club.json path, for error messages.
    source_path: PathBuf,
}

/// Pull the `parent_club` field off a club value (if present) and return it.
/// Mutates `club` so the field doesn't leak into the compiled output.
struct ParentClubRef {
    id: u64,
    team_type: String,
}

fn take_parent_club(club: &mut Value) -> Option<ParentClubRef> {
    let obj = club.as_object_mut()?;
    let raw = obj.remove("parent_club")?;
    let parent_obj = raw.as_object()?;
    let id = parent_obj.get("id").and_then(|v| v.as_u64())?;
    let team_type = parent_obj
        .get("team_type")
        .and_then(|v| v.as_str())
        .unwrap_or("B")
        .to_string();
    Some(ParentClubRef { id, team_type })
}

fn apply_satellites(
    clubs: &mut [Value],
    players: &mut Vec<Value>,
    satellites: Vec<SatelliteSpec>,
) -> Result<()> {
    for spec in satellites {
        let SatelliteSpec {
            parent_id,
            team_type,
            league_id,
            satellite_club,
            players: sat_players,
            source_path,
        } = spec;

        // Locate the parent club in the already-collected list.
        let parent = clubs
            .iter_mut()
            .find(|c| c.get("id").and_then(|v| v.as_u64()) == Some(parent_id))
            .with_context(|| {
                format!(
                    "{} declares parent_club id {} but no such club was found",
                    source_path.display(),
                    parent_id
                )
            })?;

        // Pull out the satellite's Main team (the squad we want to attach as
        // the parent's `team_type` sub-team). Other teams in the satellite's
        // teams[] (e.g. its own youth side) are dropped — the parent already
        // has its own youth competitions.
        let satellite_main = satellite_club
            .get("teams")
            .and_then(|v| v.as_array())
            .and_then(|arr| {
                arr.iter().find(|t| {
                    t.get("team_type").and_then(|v| v.as_str()) == Some("Main")
                })
            })
            .cloned()
            .with_context(|| {
                format!(
                    "{} has no Main team — satellite directories must define one",
                    source_path.display()
                )
            })?;

        // Reshape the Main entry into a sub-team of the parent: change its
        // team_type and stamp the enclosing league id.
        let mut sub_team = satellite_main;
        if let Some(obj) = sub_team.as_object_mut() {
            obj.insert("team_type".into(), Value::String(team_type.clone()));
            obj.insert("league_id".into(), Value::from(league_id));
        }

        // Append to parent.teams[].
        let parent_teams = parent
            .get_mut("teams")
            .and_then(|v| v.as_array_mut())
            .with_context(|| {
                format!(
                    "parent club {} has no teams[] to attach satellite to",
                    parent_id
                )
            })?;
        // If the parent already declares a slot of this team_type (the human-
        // curated route — e.g. a hand-named "ural-b" entry inside Ural's
        // club.json), keep that as canonical and skip the auto-append. Players
        // still get folded in below via team_type_hint.
        // Otherwise, append the satellite Main as a new sub-team. Two
        // satellites both targeting the same parent+team_type would silently
        // race here — flag it.
        let existing_idx = parent_teams.iter().position(|t| {
            t.get("team_type").and_then(|v| v.as_str()) == Some(team_type.as_str())
        });
        if existing_idx.is_none() {
            parent_teams.push(sub_team);
        } else {
            // Make sure the pre-declared slot has the right league_id stamped.
            let team = &mut parent_teams[existing_idx.unwrap()];
            if let Some(obj) = team.as_object_mut() {
                obj.entry("league_id".to_string())
                    .or_insert(Value::from(league_id));
            }
        }

        // Rewrite each satellite player so it belongs to the parent club but
        // is forced into the sub-team bucket via team_type_hint.
        for mut player in sat_players {
            if let Some(obj) = player.as_object_mut() {
                obj.insert("club_id".into(), Value::from(parent_id));
                obj.insert(
                    "team_type_hint".into(),
                    Value::String(team_type.clone()),
                );
            }
            players.push(player);
        }
    }
    Ok(())
}
