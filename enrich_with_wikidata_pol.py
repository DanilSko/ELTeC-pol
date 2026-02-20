"""
Enrich ELTeC Polish metadata with Wikidata IDs for authors and works.

Author lookup:  via VIAF ID (P214) — same as before.
Work lookup:    1. via VIAF ID if title-ids is populated, otherwise
                2. by matching the Polish title label in Wikidata, constrained
                   to the author's QID (P50) when available, to avoid
                   false positives from common titles.
"""

import pandas as pd
import requests
import time
import re

# ── Config ──────────────────────────────────────────────────────────────────
INPUT_FILE  = "ELTeC-pol_metadata.tsv"
OUTPUT_FILE = "ELTeC-pol_metadata_wikidata.tsv"
SPARQL_URL  = "https://query.wikidata.org/sparql"
HEADERS     = {"User-Agent": "ELTeC-enrichment/1.0 (research project)"}
LABEL_LANG = "pl"   # language tag for title label matching in Wikidata
DELAY       = 0.5   # seconds between requests – be polite to Wikidata


# ── Helpers ──────────────────────────────────────────────────────────────────
def extract_viaf_id(url: str) -> str | None:
    """Pull the numeric VIAF identifier out of a URL like https://viaf.org/viaf/12345/"""
    if not isinstance(url, str) or url.strip() in ("", "NA"):
        return None
    m = re.search(r"viaf\.org/viaf/(\d+)", url)
    return m.group(1) if m else None


def normalize_title(title: str) -> str:
    """
    Strip subtitles and extra punctuation to improve label matching.
    E.g. 'Coningsby: or, The New Generation' → 'Coningsby'
    """
    # Cut at common subtitle separators
    title = re.split(r"\s*[:;]\s*", title)[0]
    # Remove trailing punctuation
    title = title.strip(" .,")
    return title


def sparql_query(query: str, label: str = "") -> list[dict]:
    """Execute a SPARQL query and return the bindings list."""
    try:
        resp = requests.get(
            SPARQL_URL,
            params={"query": query, "format": "json"},
            headers=HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["results"]["bindings"]
    except Exception as e:
        print(f"  Warning: SPARQL error{' for ' + label if label else ''}: {e}")
        return []


def first_qid(bindings: list[dict], var: str = "item") -> str | None:
    if bindings:
        uri = bindings[0][var]["value"]
        return uri.rsplit("/", 1)[-1]
    return None


def viaf_to_wikidata(viaf_id: str) -> str | None:
    """Look up a Wikidata QID by VIAF ID (P214)."""
    query = f"""
    SELECT ?item WHERE {{
      ?item wdt:P214 "{viaf_id}" .
    }} LIMIT 1
    """
    return first_qid(sparql_query(query, viaf_id))


def title_to_wikidata(title: str, author_qid: str | None = None) -> str | None:
    """
    Look up a Wikidata QID for a literary work by its title label.

    Strategy (most precise first):
      1. Exact label match + author constraint (P50) — if author_qid is known.
      2. Exact label match alone (no author filter) — fallback, more risk of
         false positives, but still useful for unambiguous titles.

    Only items typed as written works (Q7725634 / Q47461344) or their
    subclasses are considered, to filter out disambiguation pages etc.
    """
    short_title = normalize_title(title)
    # Escape any double quotes in the title
    escaped = short_title.replace('"', '\\"')

    # Author-constrained query (precise)
    if author_qid:
        query = f"""
        SELECT ?item WHERE {{
          ?item rdfs:label "{escaped}"@{LABEL_LANG} ;
                wdt:P50 wd:{author_qid} .
        }} LIMIT 1
        """
        result = first_qid(sparql_query(query, escaped))
        if result:
            return result

    # Label-only query scoped to written-work types (broader fallback)
    query = f"""
    SELECT ?item WHERE {{
      ?item rdfs:label "{escaped}"@{LABEL_LANG} .
      ?item wdt:P31/wdt:P279* wd:Q7725634 .   # instance of (subclass of) written work
    }} LIMIT 1
    """
    result = first_qid(sparql_query(query, escaped))
    if result:
        return result

    # If short title differs from original, also try the full title
    full_escaped = title.replace('"', '\\"')
    if full_escaped != escaped:
        query = f"""
        SELECT ?item WHERE {{
          ?item rdfs:label "{full_escaped}"@{LABEL_LANG} .
          ?item wdt:P31/wdt:P279* wd:Q7725634 .
        }} LIMIT 1
        """
        result = first_qid(sparql_query(query, full_escaped))

    return result


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    df = pd.read_csv(INPUT_FILE, sep="\t", dtype=str)
    print(f"Loaded {len(df)} rows.")

    author_cache: dict[str, str | None] = {}   # viaf_id  → QID
    work_cache:   dict[str, str | None] = {}   # title str → QID

    author_qids = []
    work_qids   = []

    for i, row in df.iterrows():
        row_num = f"[{i+1}/{len(df)}]"

        # ── Author (unchanged: VIAF lookup) ─────────────────────────────────
        author_viaf = extract_viaf_id(row.get("author-ids", ""))
        if author_viaf and author_viaf not in author_cache:
            print(f"{row_num} Author VIAF {author_viaf}  → {row['author-name']}")
            author_cache[author_viaf] = viaf_to_wikidata(author_viaf)
            time.sleep(DELAY)
        author_qid = author_cache.get(author_viaf) if author_viaf else None
        author_qids.append(author_qid)

        # ── Work: VIAF first, then title lookup ──────────────────────────────
        work_viaf = extract_viaf_id(row.get("title-ids", ""))
        title     = str(row.get("title", "")).strip()

        if work_viaf:
            # Prefer VIAF when available
            cache_key = f"viaf:{work_viaf}"
            if cache_key not in work_cache:
                print(f"{row_num} Work VIAF {work_viaf}  → {title[:60]}")
                work_cache[cache_key] = viaf_to_wikidata(work_viaf)
                time.sleep(DELAY)
            work_qids.append(work_cache[cache_key])

        elif title and title != "nan":
            # Fall back to title label search
            cache_key = f"title:{title}"
            if cache_key not in work_cache:
                print(f"{row_num} Work title search → {title[:60]}")
                work_cache[cache_key] = title_to_wikidata(title, author_qid)
                time.sleep(DELAY)
            work_qids.append(work_cache[cache_key])

        else:
            work_qids.append(None)

    df["author_wikidata_id"] = author_qids
    df["work_wikidata_id"]   = work_qids

    df.to_csv(OUTPUT_FILE, sep="\t", index=False)
    print(f"\nDone. Results saved to '{OUTPUT_FILE}'.")

    found_authors = df["author_wikidata_id"].notna().sum()
    found_works   = df["work_wikidata_id"].notna().sum()
    print(f"Authors matched: {found_authors}/{len(df)}")
    print(f"Works   matched: {found_works}/{len(df)}")


if __name__ == "__main__":
    main()
