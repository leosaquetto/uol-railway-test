import base64
import json
import os
import random
import re
import string
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import certifi
import requests
import urllib3
from requests.exceptions import HTTPError, RequestException, SSLError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"
FALLBACK_LIST_URL = f"{BASE_URL}/"

REPO_OWNER = os.environ.get("REPO_OWNER", "leosaquetto")
REPO_NAME = os.environ.get("REPO_NAME", "uol-bot")
TARGET_BRANCH = os.environ.get("TARGET_BRANCH", "main")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()

STATUS_RUNTIME_FILE = "status_runtime.json"
SEEN_CACHE_FILE = "railway_seen_links.json"
HISTORY_FILE = "historico_leouol.json"
PENDING_FILE = "pending_offers.json"

MAX_DETAIL_FETCHES = int(os.environ.get("MAX_DETAIL_FETCHES", "4"))
MAX_SEEN_LINKS = int(os.environ.get("MAX_SEEN_LINKS", "300"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)


def log(msg: str) -> None:
    print(msg, flush=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def github_api_url(path: str) -> str:
    return f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{path}"


def build_headers_json() -> Dict[str, str]:
    return {
        "User-Agent": "uol-railway-collector",
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json",
    }


def base64_encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def base64_decode(text: str) -> Optional[str]:
    try:
        return base64.b64decode(str(text).replace("\n", "")).decode("utf-8")
    except Exception:
        return None


def github_get_file(path: str) -> Dict[str, Any]:
    try:
        resp = requests.get(
            github_api_url(path),
            headers=build_headers_json(),
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 404:
            return {"ok": True, "exists": False, "content": None, "sha": None}
        data = resp.json()
        if not resp.ok:
            return {"ok": False, "error": f"github get {resp.status_code}: {data}"}
        raw = base64_decode(data.get("content", ""))
        return {"ok": True, "exists": True, "content": raw, "sha": data.get("sha")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def github_put_file(path: str, content: str, message: str) -> Dict[str, Any]:
    current = github_get_file(path)
    if not current["ok"]:
        return {"ok": False, "error": f"falha leitura prévia {path}: {current['error']}"}

    body = {"message": message, "content": base64_encode(content), "branch": TARGET_BRANCH}
    if current.get("sha"):
        body["sha"] = current["sha"]

    try:
        resp = requests.put(
            github_api_url(path),
            headers=build_headers_json(),
            json=body,
            timeout=REQUEST_TIMEOUT,
        )
        data = resp.json()
        if resp.ok and data.get("commit"):
            return {"ok": True, "data": data}
        return {"ok": False, "error": f"github put {resp.status_code}: {data}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = (
        text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
    )
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def html_to_text(html: str) -> str:
    if not html:
        return ""
    text = html
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</p>", "\n\n", text, flags=re.I)
    text = re.sub(r"</div>", "\n", text, flags=re.I)
    text = re.sub(r"<li[^>]*>", "\n• ", text, flags=re.I)
    text = re.sub(r"</li>", "", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def absolutize_url(url: Optional[str]) -> str:
    if not url:
        return ""
    url = str(url).strip()
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return BASE_URL + url
    return f"{BASE_URL}/{url}"


def normalize_link(url: str) -> str:
    return str(url or "").strip()


def slugify_text(value: str) -> str:
    value = clean_text(value).lower()
    replacements = {
        "á": "a", "à": "a", "â": "a", "ã": "a", "ä": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "í": "i", "ì": "i", "î": "i", "ï": "i",
        "ó": "o", "ò": "o", "ô": "o", "õ": "o", "ö": "o",
        "ú": "u", "ù": "u", "û": "u", "ü": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        value = value.replace(src, dst)
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def build_offer_id_from_link(link: str, fallback_title: str = "") -> str:
    link = normalize_link(link)
    m = re.search(r"/([^/?#]+)$", link)
    if m:
        return m.group(1).strip().lower()
    return slugify_text(fallback_title or link)


def normalize_text_key(value: Optional[str]) -> str:
    raw = clean_text(value or "").lower()
    if not raw:
        return ""
    replacements = {
        "á": "a", "à": "a", "ã": "a", "â": "a", "ä": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "í": "i", "ì": "i", "î": "i", "ï": "i",
        "ó": "o", "ò": "o", "ô": "o", "õ": "o", "ö": "o",
        "ú": "u", "ù": "u", "û": "u", "ü": "u",
        "ç": "c",
    }
    for src, dst in replacements.items():
        raw = raw.replace(src, dst)
    raw = re.sub(r"https?://", "", raw)
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw)
    return raw.strip("-")


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = build_offer_id_from_link(raw)
    return normalize_text_key(raw)


def pick_description_anchor(description: str) -> str:
    if not description:
        return ""
    lines = [clean_text(x) for x in str(description).splitlines()]
    filtered = []
    blacklist_starts = (
        "beneficio valido",
        "valido ate",
        "local",
        "quando",
        "importante",
        "regras de resgate",
        "atencao",
        "atenção",
        "enviar cupons por e-mail",
        "preencha os campos abaixo",
        "e-mail",
        "mensagem",
        "enviar",
    )
    for line in lines:
        low = normalize_text_key(line)
        if not low or len(low) < 12:
            continue
        if any(low.startswith(normalize_text_key(x)) for x in blacklist_starts):
            continue
        filtered.append(low)
    return filtered[0][:160] if filtered else ""


def build_dedupe_key(title: str, validity: Optional[str], description: str) -> str:
    title_key = normalize_text_key(title)
    validity_key = normalize_text_key(validity or "")
    desc_key = pick_description_anchor(description)
    parts = [x for x in [title_key, validity_key, desc_key] if x]
    return "|".join(parts)


def pad(n: int) -> str:
    return str(n).zfill(2)


def build_snapshot_id() -> str:
    d = datetime.now()
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    return f"{d.year}{pad(d.month)}{pad(d.day)}_{pad(d.hour)}{pad(d.minute)}{pad(d.second)}_{rand}"


def build_request_headers(referer: str) -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": referer,
        "Cache-Control": "no-cache",
    }


def fetch_once(url: str, referer: str, verify_value) -> requests.Response:
    headers = build_request_headers(referer)
    resp = requests.get(
        url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        verify=verify_value,
        allow_redirects=True,
    )
    return resp


def fetch_text_with_fallback() -> Tuple[str, str]:
    candidates = [
        (LIST_URL, BASE_URL + "/"),
        (FALLBACK_LIST_URL, BASE_URL + "/"),
    ]

    last_error = None

    for url, referer in candidates:
        try:
            resp = fetch_once(url, referer, certifi.where())
            resp.raise_for_status()
            log(f"coleta ok via: {url}")
            return resp.text, url
        except SSLError:
            try:
                resp = fetch_once(url, referer, False)
                resp.raise_for_status()
                log(f"coleta ok via fallback ssl: {url}")
                return resp.text, url
            except Exception as e:
                last_error = e
                log(f"falha coleta via {url}: {e}")
        except HTTPError as e:
            last_error = e
            log(f"http error via {url}: {e}")
        except RequestException as e:
            last_error = e
            log(f"request error via {url}: {e}")

    raise RuntimeError(str(last_error) if last_error else "falha ao coletar vitrine")


def fetch_text(url: str, referer: str = BASE_URL + "/") -> str:
    try:
        resp = fetch_once(url, referer, certifi.where())
        resp.raise_for_status()
        return resp.text
    except SSLError:
        resp = fetch_once(url, referer, False)
        resp.raise_for_status()
        return resp.text


def extract_offer_cards(html: str, limit: int = 60) -> List[Dict[str, Any]]:
    cards = []
    card_regex = re.compile(
        r'<div class="col-12 col-sm-4 col-md-3 mb-3 beneficio"[\s\S]*?<!-- Fim div beneficio -->',
        re.I,
    )
    blocks = card_regex.findall(html)

    for block in blocks:
        if len(cards) >= limit:
            break
        try:
            category_match = re.search(r'data-categoria="([^"]*)"', block, re.I)
            href_match = re.search(r'<a href="([^"]+)"', block, re.I)
            title_match = re.search(r'<p class="titulo mb-0">([\s\S]*?)</p>', block, re.I)
            partner_match = re.search(
                r'<img[^>]+data-src="([^"]*\/parceiros\/[^"]+)"[^>]*alt="([^"]*)"[^>]*title="([^"]*)"',
                block,
                re.I,
            )
            benefit_img_match = re.search(
                r'<div class="col-12 thumb text-center lazy" data-src="([^"]*\/beneficios\/[^"]+)"',
                block,
                re.I,
            )

            link = absolutize_url(href_match.group(1)) if href_match else ""
            title = clean_text(title_match.group(1)) if title_match else ""
            category = clean_text(category_match.group(1)) if category_match else ""
            partner_img = absolutize_url(partner_match.group(1)) if partner_match else ""
            partner_alt = clean_text(partner_match.group(2)) if partner_match else ""
            partner_title = clean_text(partner_match.group(3)) if partner_match else ""
            benefit_img = absolutize_url(benefit_img_match.group(1)) if benefit_img_match else ""
            offer_id = build_offer_id_from_link(link, title)

            if not link or not title:
                continue

            cards.append(
                {
                    "id": offer_id,
                    "link": link,
                    "title": title,
                    "category": category,
                    "partner_img_url": partner_img,
                    "partner_name": partner_title or partner_alt or "",
                    "img_url": benefit_img,
                }
            )
        except Exception:
            continue

    return cards


def extract_title_from_detail(html: str) -> str:
    for regex in [
        re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I),
        re.compile(r"<h1[^>]*>([\s\S]*?)</h1>", re.I),
    ]:
        m = regex.search(html)
        if m:
            title = clean_text(m.group(1))
            if title:
                return title
    return ""


def extract_validity_from_detail(html: str) -> str:
    regexes = [
        re.compile(r"[Bb]enefício válido de[^.!?\n]*[.!?]?", re.I),
        re.compile(r"[Vv]álido até[^.!?\n]*[.!?]?", re.I),
        re.compile(r"\d{2}/\d{2}/\d{4}[\s\S]{0,80}\d{2}/\d{2}/\d{4}", re.I),
    ]
    for regex in regexes:
        m = regex.search(html)
        if m:
            return clean_text(m.group(0))
    return ""


def extract_description_from_detail(html: str) -> str:
    regexes = [
        re.compile(
            r'class=["\'][^"\']*info-beneficio[^"\']*["\'][^>]*>([\s\S]*?)(?:<script|<footer|class=["\'][^"\']*box-compartilhar)',
            re.I,
        ),
        re.compile(r'id=["\']beneficio["\'][^>]*>([\s\S]*?)(?:<script|<footer)', re.I),
    ]
    for regex in regexes:
        m = regex.search(html)
        if m:
            txt = html_to_text(m.group(1))
            if len(txt) >= 20:
                return txt[:4000]
    return ""


def extract_detail_image_from_detail(html: str) -> str:
    matches = re.finditer(r'<img[^>]+(?:data-src|data-original|data-lazy|src)="([^"]+)"', html, re.I)
    for m in matches:
        src = absolutize_url(m.group(1))
        if "/beneficios/" in src or "/campanhasdeingresso/" in src:
            return src
    return ""


def fetch_offer_detail_data(offer: Dict[str, Any]) -> Dict[str, Any]:
    try:
        html = fetch_text(offer["link"], LIST_URL)
        if not html or len(html.strip()) < 1000:
            return {
                "ok": False,
                "url": offer["link"],
                "title": offer["title"],
                "html_length": len(html) if html else 0,
                "validity": "",
                "description": "",
                "detail_img_url": "",
                "error": "html detalhe vazia ou curta",
            }

        detail_title = extract_title_from_detail(html) or offer["title"]
        validity = extract_validity_from_detail(html)
        description = extract_description_from_detail(html)
        detail_img = extract_detail_image_from_detail(html)

        return {
            "ok": True,
            "url": offer["link"],
            "title": detail_title,
            "html_length": len(html),
            "validity": validity,
            "description": description,
            "detail_img_url": detail_img,
            "has_validity": bool(validity),
            "has_description": bool(description),
            "error": "",
        }
    except RequestException as e:
        return {
            "ok": False,
            "url": offer["link"],
            "title": offer["title"],
            "html_length": 0,
            "validity": "",
            "description": "",
            "detail_img_url": "",
            "error": str(e),
        }


def load_seen_cache() -> Dict[str, Any]:
    result = github_get_file(SEEN_CACHE_FILE)
    if not result["ok"]:
        return {"seen": [], "updated_at": "", "error": result["error"]}
    if not result["exists"] or not result["content"]:
        return {"seen": [], "updated_at": "", "error": ""}
    try:
        data = json.loads(result["content"])
        seen = data.get("seen", [])
        return {
            "seen": [normalize_link(x) for x in seen if normalize_link(x)],
            "updated_at": str(data.get("updated_at") or ""),
            "error": "",
        }
    except Exception as e:
        return {"seen": [], "updated_at": "", "error": str(e)}


def save_seen_cache(seen_links: List[str]) -> Dict[str, Any]:
    unique = []
    seen_set = set()
    for link in seen_links:
        norm = normalize_link(link)
        if not norm or norm in seen_set:
            continue
        seen_set.add(norm)
        unique.append(norm)
    payload = {"seen": unique[-MAX_SEEN_LINKS:], "updated_at": now_iso()}
    return github_put_file(
        SEEN_CACHE_FILE,
        json.dumps(payload, indent=2, ensure_ascii=False),
        f"update railway seen links {now_iso()}",
    )


def load_status_runtime() -> Dict[str, Any]:
    result = github_get_file(STATUS_RUNTIME_FILE)
    if not result["ok"] or not result["exists"] or not result["content"]:
        return {
            "scriptable": {
                "last_started_at": "",
                "last_finished_at": "",
                "status": "",
                "summary": "",
                "offers_seen": 0,
                "new_offers": 0,
                "pending_count": 0,
                "last_error": "",
            }
        }
    try:
        return json.loads(result["content"])
    except Exception:
        return {
            "scriptable": {
                "last_started_at": "",
                "last_finished_at": "",
                "status": "",
                "summary": "",
                "offers_seen": 0,
                "new_offers": 0,
                "pending_count": 0,
                "last_error": "",
            }
        }


def save_status_runtime(state: Dict[str, Any]) -> Dict[str, Any]:
    return github_put_file(
        STATUS_RUNTIME_FILE,
        json.dumps(state, indent=2, ensure_ascii=False),
        f"update status runtime by railway {now_iso()}",
    )


def load_history_data() -> Dict[str, Any]:
    result = github_get_file(HISTORY_FILE)
    if not result["ok"] or not result["exists"] or not result["content"]:
        return {"ids": [], "dedupe_keys": []}
    try:
        return json.loads(result["content"])
    except Exception:
        return {"ids": [], "dedupe_keys": []}


def extract_history_sets(history_data: Dict[str, Any]) -> tuple[Set[str], Set[str]]:
    ids = history_data.get("ids", [])
    dedupe_keys = history_data.get("dedupe_keys", [])
    if not isinstance(ids, list):
        ids = []
    if not isinstance(dedupe_keys, list):
        dedupe_keys = []
    id_set = {normalize_offer_key(x) for x in ids if normalize_offer_key(x)}
    dedupe_set = {str(x).strip() for x in dedupe_keys if str(x).strip()}
    return id_set, dedupe_set


def load_pending_data() -> Dict[str, Any]:
    result = github_get_file(PENDING_FILE)
    if not result["ok"] or not result["exists"] or not result["content"]:
        return {"offers": []}
    try:
        return json.loads(result["content"])
    except Exception:
        return {"offers": []}


def extract_pending_sets(pending_data: Dict[str, Any]) -> tuple[Set[str], Set[str]]:
    offers = pending_data.get("offers", [])
    if not isinstance(offers, list):
        offers = []
    id_set: Set[str] = set()
    dedupe_set: Set[str] = set()
    for o in offers:
        offer_key = normalize_offer_key(o.get("id") or o.get("link"))
        if offer_key:
            id_set.add(offer_key)

        dedupe_key = str(o.get("dedupe_key") or "").strip()
        if not dedupe_key:
            dedupe_key = build_dedupe_key(
                title=o.get("title") or o.get("preview_title") or "",
                validity=o.get("validity"),
                description=o.get("description") or "",
            )
        if dedupe_key:
            dedupe_set.add(dedupe_key)
    return id_set, dedupe_set


def load_pending_count() -> int:
    pending_data = load_pending_data()
    offers = pending_data.get("offers", [])
    return len(offers) if isinstance(offers, list) else 0


def set_scriptable_status_start(state: Dict[str, Any]) -> Dict[str, Any]:
    current_pending = load_pending_count()
    state["scriptable"] = {
        "last_started_at": now_iso(),
        "last_finished_at": state.get("scriptable", {}).get("last_finished_at", ""),
        "status": "running",
        "summary": "railway collector iniciado",
        "offers_seen": 0,
        "new_offers": 0,
        "pending_count": current_pending,
        "last_error": "",
    }
    return state


def set_scriptable_status_finish(
    state: Dict[str, Any],
    status_value: str,
    summary: str,
    offers_seen: int,
    new_offers: int,
    pending_count: int,
    last_error: str = "",
) -> Dict[str, Any]:
    state["scriptable"] = {
        "last_started_at": state.get("scriptable", {}).get("last_started_at", ""),
        "last_finished_at": now_iso(),
        "status": status_value,
        "summary": summary,
        "offers_seen": offers_seen,
        "new_offers": new_offers,
        "pending_count": pending_count,
        "last_error": last_error,
    }
    return state


def verify_written_file(path: str) -> None:
    check = github_get_file(path)
    if not check["ok"] or not check["exists"]:
        raise RuntimeError(f"arquivo não confirmado após gravação: {path}")


def main() -> int:
    if not GITHUB_TOKEN:
        log("erro: GITHUB_TOKEN ausente")
        return 1

    snapshot_id = build_snapshot_id()
    html_path = f"snapshots/snapshot_{snapshot_id}.html"
    meta_path = f"snapshots/snapshot_{snapshot_id}.json"
    detail_meta_path = f"snapshots/detail_{snapshot_id}.json"

    status = load_status_runtime()
    seen_cache = load_seen_cache()
    seen_links = seen_cache.get("seen", [])

    history_data = load_history_data()
    history_ids, history_dedupe = extract_history_sets(history_data)

    pending_data = load_pending_data()
    pending_ids, pending_dedupe = extract_pending_sets(pending_data)

    status = set_scriptable_status_start(status)
    save_status_runtime(status)

    try:
        html, source_url = fetch_text_with_fallback()
        if not html or len(html.strip()) < 1000:
            current_pending = load_pending_count()
            status = set_scriptable_status_finish(
                status,
                "erro",
                "html vazia ou curta demais",
                0,
                0,
                current_pending,
                "html vazia",
            )
            save_status_runtime(status)
            log("erro: html vazia ou curta demais")
            return 1

        all_offers = extract_offer_cards(html, 60)

        id_candidates: List[Dict[str, Any]] = []
        for offer in all_offers:
            offer_key = normalize_offer_key(offer.get("id") or offer.get("link"))
            if not offer_key:
                continue
            if offer_key in history_ids or offer_key in pending_ids:
                continue
            id_candidates.append(offer)

        seen_set = set(seen_links)
        prioritized_candidates = sorted(
            id_candidates,
            key=lambda o: normalize_link(o["link"]) in seen_set,
        )

        detail_results = []
        selected_links_for_seen: List[str] = []
        real_new_count = 0
        tested_count = 0
        ok_count = 0

        for offer in prioritized_candidates:
            if real_new_count >= MAX_DETAIL_FETCHES:
                break

            tested_count += 1
            detail = fetch_offer_detail_data(offer)

            final_title = detail.get("title") or offer["title"]
            final_validity = detail.get("validity") or ""
            final_description = detail.get("description") or ""
            dedupe_key = build_dedupe_key(final_title, final_validity, final_description)

            if dedupe_key and (dedupe_key in history_dedupe or dedupe_key in pending_dedupe):
                log(f"pulada por dedupe oficial: {final_title}")
                continue

            if detail["ok"]:
                ok_count += 1

            detail_results.append(
                {
                    "index": real_new_count + 1,
                    "id": offer.get("id", ""),
                    "link": offer["link"],
                    "card_title": offer["title"],
                    "category": offer["category"],
                    "partner_name": offer["partner_name"],
                    "partner_img_url": offer["partner_img_url"],
                    "card_img_url": offer["img_url"],
                    "detail_ok": detail["ok"],
                    "detail_title": detail.get("title", ""),
                    "detail_html_length": detail.get("html_length", 0),
                    "validity": detail.get("validity", ""),
                    "has_validity": bool(detail.get("validity")),
                    "description": detail.get("description", ""),
                    "description_preview": (detail.get("description", "") or "")[:500],
                    "has_description": bool(detail.get("description")),
                    "detail_img_url": detail.get("detail_img_url", ""),
                    "dedupe_key": dedupe_key,
                    "error": detail.get("error", ""),
                }
            )

            selected_links_for_seen.append(normalize_link(offer["link"]))
            real_new_count += 1

        meta = {
            "snapshot_id": snapshot_id,
            "created_at": now_iso(),
            "source_url": source_url,
            "html_path": html_path,
            "html_length": len(html),
            "total_offers_found": len(all_offers),
            "detail_candidate_count": len(id_candidates),
            "total_new_offers_found": real_new_count,
            "tested_detail_count": tested_count,
            "cache_size_before": len(seen_links),
            "history_size": len(history_ids),
            "pending_size": len(pending_ids),
            "context": "railway",
        }

        put_html = github_put_file(html_path, html, f"railway snapshot html {snapshot_id}")
        if not put_html["ok"]:
            raise RuntimeError(put_html["error"])
        verify_written_file(html_path)

        put_meta = github_put_file(
            meta_path,
            json.dumps(meta, indent=2, ensure_ascii=False),
            f"railway snapshot meta {snapshot_id}",
        )
        if not put_meta["ok"]:
            raise RuntimeError(put_meta["error"])
        verify_written_file(meta_path)

        detail_meta = {
            "snapshot_id": snapshot_id,
            "tested_at": now_iso(),
            "tested_count": tested_count,
            "detail_ok_count": ok_count,
            "detail_fail_count": tested_count - ok_count,
            "cache_size_before": len(seen_links),
            "cache_size_after": len(seen_links) + len(selected_links_for_seen),
            "offers": detail_results,
        }

        put_detail = github_put_file(
            detail_meta_path,
            json.dumps(detail_meta, indent=2, ensure_ascii=False),
            f"railway detail meta {snapshot_id}",
        )
        if not put_detail["ok"]:
            raise RuntimeError(put_detail["error"])
        verify_written_file(detail_meta_path)

        merged_seen = seen_links + selected_links_for_seen
        save_seen = save_seen_cache(merged_seen)
        if not save_seen["ok"]:
            raise RuntimeError(save_seen["error"])

        current_pending = load_pending_count()
        status = set_scriptable_status_finish(
            status,
            "ok",
            f"railway coleta ok: {snapshot_id} | origem {source_url} | vitrine {len(all_offers)} | novas reais {real_new_count} | detalhes {ok_count}/{tested_count}",
            len(all_offers),
            real_new_count,
            current_pending,
            "",
        )
        save_status_runtime(status)

        csv_line = (
            f'{now_iso()},true,200,{len(html)},{len(all_offers)},'
            f"{real_new_count},{tested_count},{ok_count},false,\"\""
        )
        log(csv_line)
        return 0

    except Exception as e:
        current_pending = load_pending_count()
        status = set_scriptable_status_finish(
            status,
            "erro",
            "erro geral no railway collector",
            0,
            0,
            current_pending,
            str(e),
        )
        save_status_runtime(status)
        safe_error = str(e).replace('"', "'")
        log(f'{now_iso()},false,0,0,0,0,0,0,false,"{safe_error}"')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
