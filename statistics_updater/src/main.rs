//! `statistics_updater` — for every player JSON under `data/` that carries an
//! `ids.transfermarkt.com` id and an empty `history`, fetch the player's
//! Transfermarkt "Detailed stats" page and populate `history` with entries for
//! top-flight domestic league seasons only:
//!     { "s": <season>, "c": <internal_club_id>, "p": <league_apps> }
//!
//! Club mapping is driven by `ids.transfermarkt.com` on each `club.json`, so
//! matching is an exact integer lookup instead of fuzzy name resolution.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use serde_json::Value;
use walkdir::WalkDir;

const DEFAULT_DATA_DIR: &str = "data";
const TM_HOST: &str = "https://www.transfermarkt.com";
const USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";
const REQUEST_DELAY_MS: u64 = 3000;

struct HistoryRow {
    season_start: i64,
    club_id: i64,
    apps: i64,
}

fn main() -> Result<()> {
    let data_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIR));
    let data_dir = data_dir
        .canonicalize()
        .with_context(|| format!("data dir not found: {}", data_dir.display()))?;

    // Optional 2nd arg: substring filter applied to each player path, so we
    // can scope a run to a single club (e.g. "real-madrid") without losing
    // the cross-league club index.
    let filter = std::env::args().nth(2);

    println!("data dir: {}", data_dir.display());
    if let Some(ref f) = filter {
        println!("filter: {f}");
    }

    let clubs = build_club_index(&data_dir)?;
    println!("indexed {} clubs by transfermarkt.com id", clubs.len());

    let players: Vec<PathBuf> = find_player_files(&data_dir)
        .into_iter()
        .filter(|p| match &filter {
            Some(f) => p.to_string_lossy().contains(f.as_str()),
            None => true,
        })
        .collect();
    println!("found {} player files", players.len());

    let mut updated = 0usize;
    let mut no_id = 0usize;
    let mut has_history = 0usize;
    let mut failed = 0usize;
    let mut no_domestic_rows = 0usize;

    for (i, path) in players.iter().enumerate() {
        match process_player(path, &clubs) {
            Ok(ProcessOutcome::NoTmId) => no_id += 1,
            Ok(ProcessOutcome::HistoryPresent) => has_history += 1,
            Ok(ProcessOutcome::Updated { rows, skipped }) => {
                if rows == 0 {
                    no_domestic_rows += 1;
                } else {
                    updated += 1;
                }
                println!(
                    "  [{}/{}] {} — {} rows, {} skipped",
                    i + 1,
                    players.len(),
                    short(path, &data_dir),
                    rows,
                    skipped
                );
                let _ = std::io::stdout().flush();
                sleep(Duration::from_millis(REQUEST_DELAY_MS));
            }
            Err(e) => {
                failed += 1;
                eprintln!("  [FAIL] {}: {e:#}", short(path, &data_dir));
                let _ = std::io::stderr().flush();
                sleep(Duration::from_millis(REQUEST_DELAY_MS));
            }
        }
    }

    println!(
        "\ndone: updated={updated} no_domestic_rows={no_domestic_rows} \
         skipped_no_id={no_id} skipped_has_history={has_history} failed={failed}"
    );
    Ok(())
}

enum ProcessOutcome {
    NoTmId,
    HistoryPresent,
    Updated { rows: usize, skipped: usize },
}

fn process_player(path: &Path, clubs: &ClubIndex) -> Result<ProcessOutcome> {
    let text = fs::read_to_string(path)?;
    let json: Value = serde_json::from_str(&text)
        .with_context(|| format!("invalid json: {}", path.display()))?;

    let tm_id = json
        .get("ids")
        .and_then(|v| v.get("transfermarkt.com"))
        .and_then(|v| v.as_str());
    let Some(tm_id) = tm_id else {
        return Ok(ProcessOutcome::NoTmId);
    };
    if tm_id.is_empty() {
        return Ok(ProcessOutcome::NoTmId);
    }

    if let Some(arr) = json.get("history").and_then(|v| v.as_array()) {
        if !arr.is_empty() {
            return Ok(ProcessOutcome::HistoryPresent);
        }
    }

    let html = fetch_player_html(tm_id)?;
    let raw_rows = parse_performance_table(&html)?;

    let mut rows: Vec<HistoryRow> = Vec::new();
    let mut skipped = 0usize;
    for r in &raw_rows {
        if !r.is_domestic_top_flight {
            skipped += 1;
            continue;
        }
        let Some(apps) = r.apps else { continue };
        if apps == 0 {
            continue; // transfermarkt shows "-" for zero — don't emit noise
        }
        match clubs.lookup_tm(&r.club_tm_id) {
            Some(club_id) => rows.push(HistoryRow {
                season_start: r.season_start,
                club_id,
                apps,
            }),
            None => {
                skipped += 1;
                eprintln!(
                    "      unmapped tm club: {} ({:?}) season {}",
                    r.club_tm_id, r.club_name, r.season_start
                );
            }
        }
    }

    // Reverse: transfermarkt lists newest first; we want oldest first.
    rows.reverse();

    if rows.is_empty() {
        return Ok(ProcessOutcome::Updated {
            rows: 0,
            skipped,
        });
    }

    let new_text = replace_history(&text, &rows)?;
    fs::write(path, new_text)?;

    Ok(ProcessOutcome::Updated {
        rows: rows.len(),
        skipped,
    })
}

fn find_player_files(data_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for entry in WalkDir::new(data_dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(parent) = path.parent().and_then(|p| p.file_name()).and_then(|s| s.to_str())
        else {
            continue;
        };
        if parent != "players" {
            continue;
        }
        out.push(path.to_path_buf());
    }
    out.sort();
    out
}

// ----------------------------------------------------------------------------
// Club index: transfermarkt club id  →  internal club id
// ----------------------------------------------------------------------------

struct ClubIndex {
    by_tm_id: HashMap<String, i64>,
}

impl ClubIndex {
    fn len(&self) -> usize {
        self.by_tm_id.len()
    }
    fn lookup_tm(&self, tm_id: &str) -> Option<i64> {
        self.by_tm_id.get(tm_id).copied()
    }
}

fn build_club_index(data_dir: &Path) -> Result<ClubIndex> {
    let mut by_tm_id: HashMap<String, i64> = HashMap::new();
    for entry in WalkDir::new(data_dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.file_name().and_then(|s| s.to_str()) != Some("club.json") {
            continue;
        }
        let Ok(text) = fs::read_to_string(path) else { continue };
        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
        let Some(cid) = v.get("id").and_then(|x| x.as_i64()) else { continue };
        let Some(tm) = v
            .get("ids")
            .and_then(|x| x.get("transfermarkt.com"))
            .and_then(|x| x.as_str())
        else {
            continue;
        };
        by_tm_id.insert(tm.to_string(), cid);
    }
    Ok(ClubIndex { by_tm_id })
}

// ----------------------------------------------------------------------------
// Transfermarkt fetching & parsing
// ----------------------------------------------------------------------------

fn fetch_player_html(tm_id: &str) -> Result<String> {
    // Primary: direct TM. Fallback: web.archive.org (TM frequently serves a
    // "Human Verification" 405 to scripted clients; the archive has public
    // snapshots we can read instead).
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(30))
        .user_agent(USER_AGENT)
        .redirects(5)
        .build();

    let direct_url =
        format!("{TM_HOST}/a/leistungsdatendetails/spieler/{tm_id}");
    match try_fetch(&agent, &direct_url) {
        Ok(body) if body.contains(r#"class="items""#) => return Ok(body),
        Ok(_) => {} // fall through to archive
        Err(_) => {}
    }

    let mut last_err: Option<anyhow::Error> = None;
    for ts in &["2026", "20251201", "20251001", "20250601"] {
        let url = format!(
            "https://web.archive.org/web/{ts}/https://www.transfermarkt.com\
             /a/leistungsdatendetails/spieler/{tm_id}"
        );
        match try_fetch(&agent, &url) {
            Ok(body) if body.contains(r#"class="items""#) => return Ok(body),
            Ok(_) => {
                last_err = Some(anyhow!("archive.org {ts}: no items table"));
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("all fetches failed for tm_id {tm_id}")))
}

fn try_fetch(agent: &ureq::Agent, url: &str) -> Result<String> {
    let resp = agent
        .get(url)
        .set(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )
        .set("Accept-Language", "en-US,en;q=0.9")
        .call()
        .with_context(|| format!("GET {url}"))?;
    let body = resp
        .into_string()
        .with_context(|| format!("read body {url}"))?;
    Ok(body)
}

struct RawRow {
    season_start: i64,
    club_name: String,
    club_tm_id: String,
    apps: Option<i64>,
    is_domestic_top_flight: bool,
}

fn parse_performance_table(html: &str) -> Result<Vec<RawRow>> {
    let table_re =
        Regex::new(r#"(?s)<table[^>]*class="items"[^>]*>(.*?)</table>"#)?;
    let Some(caps) = table_re.captures(html) else {
        return Err(anyhow!("items table not found"));
    };
    let tbody_re = Regex::new(r#"(?s)<tbody>(.*?)</tbody>"#)?;
    let Some(bcaps) = tbody_re.captures(caps.get(1).unwrap().as_str()) else {
        return Err(anyhow!("tbody not found inside items table"));
    };
    let tbody = bcaps.get(1).unwrap().as_str();

    let row_re = Regex::new(r#"(?s)<tr[^>]*>(.*?)</tr>"#)?;
    let cell_re = Regex::new(r#"(?s)<td[^>]*>(.*?)</td>"#)?;

    // First <td>:  season "25/26"
    // Third <td>:  competition <a href="/.../wettbewerb/RU1/...">Premier Liga</a>
    // Fourth <td>: club <a title="Spartak Moscow" href="/.../verein/232/saison_id/..."
    // Fifth <td>:  appearances (integer or "-")
    let comp_href_re =
        Regex::new(r#"href="/[^"]*/(wettbewerb|pokalwettbewerb)/([A-Z0-9]+)/"#)?;
    let club_href_re = Regex::new(
        r#"<a title="([^"]+)" href="/[^"]+/startseite/verein/(\d+)/"#,
    )?;

    let mut out = Vec::new();
    for rcaps in row_re.captures_iter(tbody) {
        let row_html = rcaps.get(1).unwrap().as_str();
        let cells: Vec<&str> = cell_re
            .captures_iter(row_html)
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        if cells.len() < 6 {
            continue;
        }
        let season_txt = strip_tags(cells[0]);
        let Some(season_start) = parse_season_start(season_txt.trim()) else {
            continue;
        };
        let comp_cell = cells[2];
        let (comp_kind, comp_code) = match comp_href_re.captures(comp_cell) {
            Some(c) => (c.get(1).unwrap().as_str().to_string(),
                        c.get(2).unwrap().as_str().to_string()),
            None => continue,
        };
        let is_domestic_top_flight =
            comp_kind == "wettbewerb" && comp_code.ends_with('1');

        let club_cell = cells[3];
        let Some(club_caps) = club_href_re.captures(club_cell) else {
            continue;
        };
        let club_name = club_caps.get(1).unwrap().as_str().to_string();
        let club_tm_id = club_caps.get(2).unwrap().as_str().to_string();

        let apps_txt = strip_tags(cells[4]).trim().to_string();
        let apps = apps_txt.parse::<i64>().ok();

        out.push(RawRow {
            season_start,
            club_name,
            club_tm_id,
            apps,
            is_domestic_top_flight,
        });
    }
    Ok(out)
}

fn strip_tags(s: &str) -> String {
    let re = Regex::new(r"<[^>]+>").unwrap();
    re.replace_all(s, "").replace("&nbsp;", " ").trim().to_string()
}

/// "25/26" → 2025, "99/00" → 1999 (TM uses 2-digit years).
fn parse_season_start(s: &str) -> Option<i64> {
    let first = s.split('/').next()?;
    let yy: i64 = first.trim().parse().ok()?;
    if yy < 0 || yy > 99 {
        return None;
    }
    // Transfermarkt spans 1900s lower half... practically a football career
    // straddles 1990..=current. Use 70 as the century pivot: 70..=99 → 19xx,
    // 00..=69 → 20xx.
    let full = if yy >= 70 { 1900 + yy } else { 2000 + yy };
    Some(full)
}

// ----------------------------------------------------------------------------
// History replacement (surgical text edit, preserves surrounding formatting)
// ----------------------------------------------------------------------------

fn replace_history(original: &str, rows: &[HistoryRow]) -> Result<String> {
    let formatted = format_history(rows);

    // Existing history block → replace in place.
    let re = Regex::new(r#"(?s)"history":\s*\[[^\]]*\]"#)?;
    if re.is_match(original) {
        return Ok(re.replace(original, formatted.as_str()).into_owned());
    }

    // No history block: insert before the object's closing `}` (which lives
    // at column 0 on its own line for every player file in this repo).
    let insert_re = Regex::new(r"(?s)(,?)\s*\n\}\s*\n?\Z")?;
    let Some(m) = insert_re.find(original) else {
        return Err(anyhow!("no closing brace to insert before"));
    };
    let mut out = String::with_capacity(original.len() + formatted.len() + 8);
    out.push_str(&original[..m.start()]);
    out.push_str(",\n  ");
    out.push_str(&formatted);
    out.push_str("\n}\n");
    Ok(out)
}

fn format_history(rows: &[HistoryRow]) -> String {
    if rows.is_empty() {
        return "\"history\": []".to_string();
    }
    let mut out = String::from("\"history\": [\n");
    for (i, r) in rows.iter().enumerate() {
        out.push_str(&format!(
            "    {{ \"s\": {}, \"c\": {}, \"p\": {} }}",
            r.season_start, r.club_id, r.apps
        ));
        if i + 1 < rows.len() {
            out.push(',');
        }
        out.push('\n');
    }
    out.push_str("  ]");
    out
}

fn short(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .display()
        .to_string()
}
