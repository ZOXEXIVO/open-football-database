//! Transfermarkt-club → dataset-club resolution, shared by `history_scraper`
//! and `transfer_scraper`.
//!
//! Resolution is three tiers, applied in this order by the callers:
//!
//!   1. `club_overrides.json` — hand-written `{ "<tm id>": <db id> | null }`.
//!      `null` means "this club is deliberately not in the dataset": drop the
//!      row silently instead of reporting it as a miss.
//!   2. `cache/club_map.json` — every club a previous run resolved by name.
//!      Both tools read and write this file, so a club one of them works out
//!      is immediately known to the other.
//!   3. [`match_name`] — normalized-name matching against the club tree.
//!
//! Only the last tier lives here as logic; the first two are file formats this
//! crate loads and saves so the two binaries cannot drift apart on them.
//!
//! Name matching is deliberately conservative: an ambiguous name resolves to
//! nothing and is reported for a manual override, because a wrong club id is
//! far more expensive than a missing one — it silently attributes a career to
//! the wrong side.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value};
use walkdir::WalkDir;

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

/// Latin-1/Latin-2/Turkish letters folded to ASCII. Transfermarkt and the
/// dataset disagree constantly on diacritics ("Vllaznia Shkodër" vs
/// "Vllaznia Shkoder"), so every comparison happens on folded text.
fn fold(c: char) -> &'static str {
    match c {
        'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'ā' | 'ă' | 'ą' => "a",
        'æ' => "ae",
        'ç' | 'ć' | 'č' | 'ĉ' | 'ċ' => "c",
        'ď' | 'đ' | 'ð' => "d",
        'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e",
        'ğ' | 'ĝ' | 'ġ' | 'ģ' => "g",
        'ĥ' | 'ħ' => "h",
        'í' | 'ì' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' | 'ı' => "i",
        'ĵ' => "j",
        'ķ' => "k",
        'ĺ' | 'ļ' | 'ľ' | 'ł' => "l",
        'ñ' | 'ń' | 'ņ' | 'ň' => "n",
        'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'ø' | 'ō' | 'ŏ' | 'ő' => "o",
        'œ' => "oe",
        'ŕ' | 'ŗ' | 'ř' => "r",
        'ś' | 'ŝ' | 'ş' | 'š' | 'ș' => "s",
        'ß' => "ss",
        'ţ' | 'ť' | 'ŧ' | 'ț' => "t",
        'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u",
        'ŵ' => "w",
        'ý' | 'ÿ' | 'ŷ' => "y",
        'ź' | 'ż' | 'ž' => "z",
        _ => "",
    }
}

/// Lowercase, fold diacritics, and reduce every run of punctuation to a single
/// space. `"Al-Ahli (UAE)"` → `"al ahli uae"`.
fn normalize(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars().flat_map(|c| c.to_lowercase()) {
        if c.is_ascii_alphanumeric() {
            out.push(c);
        } else {
            let folded = fold(c);
            if folded.is_empty() {
                if !out.ends_with(' ') {
                    out.push(' ');
                }
            } else {
                out.push_str(folded);
            }
        }
    }
    out.trim().to_string()
}

/// Club-type abbreviations and generic words that carry no identity. Dropping
/// them is what lets "Wolves FC" reach "Wolves" and "NK Varazdin" reach
/// "Varazdin". A token belongs here only once [`collisions`] confirms it does
/// not merge two dataset clubs — "sporting" and "atletico" are kept for that
/// reason, while "real" is droppable because no two clubs in the tree differ
/// only by it.
const NOISE: &[&str] = &[
    // Club-type acronyms. Transfermarkt spells them out where the dataset
    // carries FM's bare short name, so almost every one of these is a prefix
    // that has to come off before "RSC Anderlecht" can reach "Anderlecht".
    "fc", "afc", "cfc", "kfc", "fk", "fck", "cf", "sc", "sco", "scr", "ac", "acf", "acr", "acd",
    "aca", "as", "asd", "ss", "ssc", "ssd", "us", "usd", "uc", "ud", "ue", "sv", "tsv", "tsg",
    "vfb", "vfl", "vfr", "bsc", "bsv", "bv", "spvgg", "kv", "kvc", "krc", "kaa", "rc", "rcd",
    "rsc", "rkc", "rb", "cd", "ca", "ce", "ec", "sec", "sd", "sad", "gd", "ad", "ae", "aa",
    "cs", "csd", "nk", "hnk", "gnk", "mnk", "kf", "ks", "ksc", "fsv", "msv", "sk", "ik", "if",
    "bk", "aik", "ff", "gif", "jk", "jfk", "mfk", "sfk", "zfk", "ofk", "psv", "pec", "fbc",
    "fci", "sbv", "ska", "en", "cdsc",
    // Words for "club" and "football" in the languages the dataset covers.
    "club", "clube", "calcio", "futbol", "football", "futebol", "fussball", "fotball",
    "fotboll", "esporte", "esportivo", "sportif", "sportiva", "sportive", "kulubu", "spor",
    "the", "de", "do", "da", "of",
    // Abbreviated qualifiers Transfermarkt writes with a full stop, which
    // normalization has already reduced to a bare token.
    "dep", "depvo", "def", "sp", "atl", "olym", "gim", "univ", "real", "lr",
];

/// Tokens that identify a *second* team rather than the club itself. They are
/// held apart from the core name and must match exactly on both sides, so
/// "Nîmes B" can never resolve to Nîmes — the dataset models B sides as their
/// own clubs and crediting a reserve spell to the parent would be wrong.
/// `"m"` is here for the Russian reserve suffix ("Rodina-M"): exactly one club
/// in the dataset carries a standalone `m`, and it is that kind of side.
const MARKERS: &[&str] = &[
    "b", "ii", "iii", "2", "3", "c", "m", "jong", "reserve", "reserves", "amateure", "amateurs",
    "u17", "u18", "u19", "u20", "u21", "u23", "youth", "academy",
];

/// Transliteration variants of the same word. Cyrillic names reach the two
/// sides through different romanizations, and a token that differs by one
/// letter simply fails to match — Transfermarkt's "Dynamo Brest" missed the
/// dataset's "Dinamo Brest" and was resolved to Stade Brestois instead.
/// Extend this rather than loosening the token comparison itself.
const TOKEN_ALIASES: &[(&str, &str)] = &[
    ("dynamo", "dinamo"),
    ("dynamos", "dinamo"),
    ("shakhtyor", "shakhtar"),
    ("shakhtior", "shakhtar"),
];

fn canonical(token: &str) -> &str {
    for (from, to) in TOKEN_ALIASES {
        if *from == token {
            return to;
        }
    }
    token
}

/// A lone letter is always an initial from a longer club type ("G. Ajaccio",
/// "R. Strasbourg"); the ones that mean a second team are caught by [`MARKERS`]
/// before this is reached.
fn is_noise(token: &str) -> bool {
    NOISE.contains(&token) || token.chars().count() == 1
}

/// Position is deliberately not consulted. The dataset's own slugs put the
/// marker in the middle ("zenit-2-st-petersburg", "rodina-2-moscow"), so a
/// "only counts when last" rule lets "Zenit St. Petersburg" match Zenit 2 by
/// subset. The cost is that an abbreviated name whose initial collides with a
/// marker — "Nott'm Forest", "M. Tel Aviv" — goes unmapped and needs an
/// override, which is the safe direction to fail in.
fn is_marker(token: &str) -> bool {
    MARKERS.contains(&token)
}

/// Transfermarkt disambiguates same-named clubs with a trailing bracket —
/// "Al-Ahli (UAE)", "CA Unión (Santa Fe)". That is metadata about which club is
/// meant, not part of the name, so it is removed before tokenizing rather than
/// left to be absorbed by a subset rule.
fn strip_qualifier(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut depth = 0usize;
    for c in raw.chars() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// A name split into the part that identifies the club and the part that
/// identifies which of its teams is meant.
struct Parts {
    core: Vec<String>,
    markers: BTreeSet<String>,
}

fn parts(raw: &str) -> Parts {
    let normalized = normalize(&strip_qualifier(raw));
    let mut core = Vec::new();
    let mut markers = BTreeSet::new();
    for token in normalized.split_whitespace() {
        if is_marker(token) {
            markers.insert(token.to_string());
        } else if !is_noise(token) {
            core.push(canonical(token).to_string());
        }
    }
    // A name that is nothing but noise ("FC") keeps its tokens rather than
    // matching everything.
    if core.is_empty() && markers.is_empty() {
        core = normalized.split_whitespace().map(str::to_string).collect();
    }
    Parts { core, markers }
}

// ---------------------------------------------------------------------------
// The club index
// ---------------------------------------------------------------------------

struct Entry {
    id: u64,
    core: BTreeSet<String>,
    markers: BTreeSet<String>,
}

/// Every club in the dataset, indexed for name lookup.
pub struct ClubIndex {
    /// Fully normalized name (noise included) → club ids answering to it.
    /// Public because both binaries report its size as a sanity figure.
    pub aliases: HashMap<String, Vec<u64>>,
    /// Club id → the dataset's own name, for readable reports.
    pub display: HashMap<u64, String>,
    /// Club id → `country/league`, which is what actually tells two clubs of
    /// the same name apart when a report asks for a decision.
    pub location: HashMap<u64, String>,
    entries: Vec<Entry>,
}

impl ClubIndex {
    /// `"Arsenal (gb/premier-league)"` — the form the unmapped report needs so
    /// the reader can pick between same-named clubs without opening the tree.
    pub fn describe(&self, id: u64) -> String {
        let name = self.display.get(&id).map(String::as_str).unwrap_or("?");
        match self.location.get(&id) {
            Some(where_) => format!("{name} ({where_})"),
            None => name.to_string(),
        }
    }
}

/// Read every `club.json` under `data_dir`.
///
/// Only the club's own `id`/`name` is indexed — never `teams[]`, whose age-group
/// entries share the club's name under different ids and would make every club
/// ambiguous with itself. Satellite sides (Real Sociedad B, Benfica B) are their
/// own `club.json` with their own id, so they index naturally and separately.
pub fn build_club_index(data_dir: &Path) -> Result<ClubIndex> {
    let mut aliases: HashMap<String, Vec<u64>> = HashMap::new();
    let mut display = HashMap::new();
    let mut location = HashMap::new();
    let mut entries = Vec::new();

    for entry in WalkDir::new(data_dir).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() || entry.file_name() != "club.json" {
            continue;
        }
        let path = entry.path();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))?;
        let v: Value = serde_json::from_str(&text)
            .with_context(|| format!("parse {}", path.display()))?;
        let (Some(id), Some(name)) = (
            v.get("id").and_then(|x| x.as_u64()),
            v.get("name").and_then(|x| x.as_str()),
        ) else {
            continue;
        };
        display.insert(id, name.to_string());
        // `data/<country>/<league>/<club>/club.json` — the two components above
        // the club directory are what identify it among its namesakes.
        if let Ok(rel) = path.strip_prefix(data_dir) {
            let parts: Vec<&str> = rel
                .components()
                .filter_map(|c| c.as_os_str().to_str())
                .collect();
            if parts.len() >= 3 {
                location.insert(id, format!("{}/{}", parts[0], parts[1]));
            }
        }

        let mut names = vec![name.to_string()];
        // The directory slug is a second spelling of the same club and costs
        // nothing when it normalizes to the same string.
        if let Some(slug) = path.parent().and_then(|p| p.file_name()).and_then(|s| s.to_str()) {
            names.push(slug.replace('-', " "));
        }
        for n in &names {
            let key = normalize(n);
            if key.is_empty() {
                continue;
            }
            let bucket = aliases.entry(key).or_default();
            if !bucket.contains(&id) {
                bucket.push(id);
            }
            // Every spelling gets its own entry: the slug is often the name
            // Transfermarkt uses when the dataset stores the formal one
            // ("wolves" vs "Wolverhampton"), and only indexed spellings take
            // part in the token passes.
            let p = parts(n);
            let core: BTreeSet<String> = p.core.into_iter().collect();
            if core.is_empty() {
                continue;
            }
            if !entries
                .iter()
                .any(|e: &Entry| e.id == id && e.core == core && e.markers == p.markers)
            {
                entries.push(Entry {
                    id,
                    core,
                    markers: p.markers,
                });
            }
        }
    }

    Ok(ClubIndex {
        aliases,
        display,
        location,
        entries,
    })
}

/// Dataset clubs that reduce to the same identifying tokens, and so can never
/// be told apart by name. Growing [`NOISE`] widens the net but risks merging
/// distinct clubs; this is the check that says whether it did.
pub fn collisions(index: &ClubIndex) -> Vec<Vec<u64>> {
    let mut groups: BTreeMap<(Vec<String>, Vec<String>), BTreeSet<u64>> = BTreeMap::new();
    for e in &index.entries {
        let key = (
            e.core.iter().cloned().collect(),
            e.markers.iter().cloned().collect(),
        );
        groups.entry(key).or_default().insert(e.id);
    }
    groups
        .into_values()
        .filter(|ids| ids.len() > 1)
        .map(|ids| ids.into_iter().collect())
        .collect()
}

/// Outcome of a name lookup. `Ambiguous` carries the candidates so the caller
/// can print them next to a suggested override line.
pub enum NameMatch {
    Unique(u64),
    Ambiguous(Vec<u64>),
    None,
}

fn verdict(mut ids: Vec<u64>) -> NameMatch {
    ids.sort_unstable();
    ids.dedup();
    match ids.len() {
        0 => NameMatch::None,
        1 => NameMatch::Unique(ids[0]),
        _ => NameMatch::Ambiguous(ids),
    }
}

/// Resolve a Transfermarkt club name to a dataset club id.
///
/// Three passes, each strictly narrower than the next, and all of them require
/// the team markers to agree exactly:
///
///   1. the whole normalized string matches an indexed name;
///   2. the identifying tokens match as a set — this is what bridges "Wolves
///      FC" → "Wolves" and "NK Varazdin" → "Varazdin";
///   3. one side's tokens are a subset of the other's. Both directions happen
///      constantly: the dataset stores FM's short names, so Transfermarkt's
///      "Arda Kardzhali" has to reach "Arda", while its "Leipzig" has to reach
///      "RB Leipzig". Candidates sharing the most tokens win; a tie is reported
///      as ambiguous rather than guessed.
///
/// Pass 3 is loose enough that two guards carry it: a dataset name may only be
/// the shorter side if it still contains the queried name's leading word, and
/// Transfermarkt's bracketed disambiguators are stripped in [`parts`] rather
/// than left for a subset to absorb.
pub fn match_name(index: &ClubIndex, raw: &str) -> NameMatch {
    let key = normalize(raw);
    if key.is_empty() {
        return NameMatch::None;
    }
    if let Some(ids) = index.aliases.get(&key) {
        return verdict(ids.clone());
    }

    let q = parts(raw);
    if q.core.is_empty() {
        return NameMatch::None;
    }
    let q_core: BTreeSet<String> = q.core.iter().cloned().collect();

    let exact: Vec<u64> = index
        .entries
        .iter()
        .filter(|e| e.markers == q.markers && e.core == q_core)
        .map(|e| e.id)
        .collect();
    if !exact.is_empty() {
        return verdict(exact);
    }

    let head = q.core[0].as_str();
    let mut best = 0usize;
    let mut best_ids: Vec<u64> = Vec::new();
    for e in &index.entries {
        if e.markers != q.markers || e.core.is_empty() {
            continue;
        }
        let shorter = e.core.is_subset(&q_core);
        let longer = q_core.is_subset(&e.core);
        if !shorter && !longer {
            continue;
        }
        // A dataset name that drops the queried club's leading word is naming
        // something else: "Skövde AIK" is not AIK and "ETSV Hamburg" is not
        // Hamburger SV, however neatly the remaining tokens nest.
        if shorter && !longer && !e.core.contains(head) {
            continue;
        }
        let shared = e.core.intersection(&q_core).count();
        if shared > best {
            best = shared;
            best_ids = vec![e.id];
        } else if shared == best {
            best_ids.push(e.id);
        }
    }
    verdict(best_ids)
}

// ---------------------------------------------------------------------------
// club_overrides.json
// ---------------------------------------------------------------------------

/// Strip `//` line comments and trailing commas.
///
/// The unmapped report prints override lines with a `// club name — N rows`
/// tail precisely so they can be pasted straight into the file, which only
/// works if the file tolerates what was pasted. Quoted strings are respected so
/// a club actually called "A // B" survives.
fn relaxed_json(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        let bytes = line.as_bytes();
        let mut in_string = false;
        let mut cut = line.len();
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'\\' if in_string => i += 1,
                b'"' => in_string = !in_string,
                b'/' if !in_string && bytes.get(i + 1) == Some(&b'/') => {
                    cut = i;
                    break;
                }
                _ => {}
            }
            i += 1;
        }
        out.push_str(line[..cut].trim_end());
        out.push('\n');
    }
    // Drop any comma that now has nothing but whitespace between it and the
    // closing brace — pasting a report line at the end of the file leaves one.
    let src: Vec<char> = out.chars().collect();
    let mut cleaned = String::with_capacity(out.len());
    let mut in_string = false;
    let mut i = 0;
    while i < src.len() {
        let c = src[i];
        if in_string {
            cleaned.push(c);
            if c == '\\' {
                if let Some(next) = src.get(i + 1) {
                    cleaned.push(*next);
                    i += 2;
                    continue;
                }
            } else if c == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if c == '"' {
            in_string = true;
        } else if c == ',' {
            let next = src[i + 1..].iter().find(|c| !c.is_whitespace());
            if matches!(next, Some('}') | Some(']')) {
                i += 1;
                continue;
            }
        }
        cleaned.push(c);
        i += 1;
    }
    cleaned
}

/// `{ "<tm club id>": <db club id> | null }`. A missing file is not an error —
/// it just means nothing has been overridden yet.
pub fn load_overrides(path: &Path) -> Result<HashMap<u64, Option<u64>>> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Ok(HashMap::new());
    };
    let v: Value = serde_json::from_str(&relaxed_json(&text))
        .with_context(|| format!("parse {}", path.display()))?;
    let obj = v
        .as_object()
        .with_context(|| format!("{} must be a JSON object", path.display()))?;
    let mut out = HashMap::new();
    for (k, val) in obj {
        let Ok(tm) = k.parse::<u64>() else {
            anyhow::bail!("{}: key {k:?} is not a Transfermarkt club id", path.display());
        };
        let mapped = match val {
            Value::Null => None,
            other => Some(other.as_u64().with_context(|| {
                format!("{}: value for {k} must be a club id or null", path.display())
            })?),
        };
        out.insert(tm, mapped);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// cache/club_map.json
// ---------------------------------------------------------------------------

/// The name-resolution cache: `{ "<tm id>": { "db": <id>, "name": "<tm name>" } }`.
/// The name is kept so a re-run can report and re-check a mapping without
/// re-fetching the page that supplied it.
pub struct ClubMap {
    pub resolved: HashMap<u64, u64>,
    pub names: HashMap<u64, String>,
}

/// Never fails: a missing or corrupt cache is simply an empty one, since every
/// entry can be recomputed.
pub fn load_club_map(path: &Path) -> ClubMap {
    let mut resolved = HashMap::new();
    let mut names = HashMap::new();
    if let Ok(text) = std::fs::read_to_string(path) {
        if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(&text) {
            for (k, val) in obj {
                let Ok(tm) = k.parse::<u64>() else { continue };
                if let Some(db) = val.get("db").and_then(|d| d.as_u64()) {
                    resolved.insert(tm, db);
                }
                if let Some(name) = val.get("name").and_then(|n| n.as_str()) {
                    names.insert(tm, name.to_string());
                }
            }
        }
    }
    ClubMap { resolved, names }
}

/// Write the cache back, sorted by Transfermarkt id so the file diffs cleanly.
/// Names without a resolved id are kept too — knowing what a club is called is
/// what makes the unmapped report readable next time.
pub fn save_club_map(path: &Path, resolved: &HashMap<u64, u64>, names: &HashMap<u64, String>) {
    let mut merged: BTreeMap<u64, Map<String, Value>> = BTreeMap::new();
    for (tm, db) in resolved {
        merged.entry(*tm).or_default().insert("db".into(), Value::from(*db));
    }
    for (tm, name) in names {
        merged
            .entry(*tm)
            .or_default()
            .insert("name".into(), Value::String(name.clone()));
    }
    let doc: Map<String, Value> = merged
        .into_iter()
        .map(|(tm, entry)| (tm.to_string(), Value::Object(entry)))
        .collect();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(text) = serde_json::to_string_pretty(&Value::Object(doc)) {
        let _ = std::fs::write(path, text);
    }
}
