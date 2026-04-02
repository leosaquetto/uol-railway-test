import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import certifi
import requests
import urllib3
from requests.exceptions import HTTPError, RequestException, SSLError

BASE_URL = "https://clube.uol.com.br"
LIST_URL = f"{BASE_URL}/?order=new"
FALLBACK_LIST_URL = f"{BASE_URL}/"

REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "20"))
TEST_DETAIL_FETCH = os.environ.get("TEST_DETAIL_FETCH", "1").strip() == "1"
MAX_DETAIL_TESTS = int(os.environ.get("MAX_DETAIL_TESTS", "1"))

USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def log(msg: str) -> None:
    print(msg, flush=True)


def clean_text(text: Optional[str]) -> str:
    if not text:
        return ""
    text = str(text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    text = re.sub(r"^ +| +$", "", text, flags=re.MULTILINE)
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
    return clean_text(text)


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


def get_offer_id(link: str) -> str:
    try:
        clean_link = str(link).split("?")[0].rstrip("/")
        return clean_link.split("/")[-1]
    except Exception:
        return str(link or "").strip()


def normalize_offer_key(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = get_offer_id(raw)
    raw = re.sub(r"https?://", "", raw)
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-{2,}", "-", raw)
    return raw.strip("-")


def uniq_by(items: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    out = []
    seen = set()
    for item in items:
        key = key_fn(item)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def is_bad_banner_url(url: Optional[str]) -> bool:
    u = str(url or "").lower()
    if not u:
        return True
    return (
        "loader.gif" in u
        or "/static/images/loader.gif" in u
        or "/parceiros/" in u
        or "/rodape/" in u
        or "icon-instagram" in u
        or "icon-facebook" in u
        or "icon-twitter" in u
        or "icon-youtube" in u
        or "instagram.png" in u
        or "facebook.png" in u
        or "twitter.png" in u
        or "youtube.png" in u
        or "share-" in u
        or "social" in u
        or "logo-uol" in u
        or "logo_uol" in u
    )


def is_likely_benefit_banner(url: Optional[str]) -> bool:
    u = str(url or "").lower()
    if not u or is_bad_banner_url(u):
        return False
    return (
        "/beneficios/" in u
        or "/campanhasdeingresso/" in u
        or "cloudfront.net" in u
    )


def build_headers(referer: Optional[str] = None) -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": referer or (BASE_URL + "/"),
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def fetch_once(session: requests.Session, url: str, referer: Optional[str], verify_value) -> requests.Response:
    return session.get(
        url,
        headers=build_headers(referer),
        timeout=REQUEST_TIMEOUT,
        verify=verify_value,
        allow_redirects=True,
    )


def fetch_with_fallback(session: requests.Session, url: str, referer: Optional[str] = None) -> Dict[str, Any]:
    started = time.perf_counter()

    try:
        r = fetch_once(session, url, referer, certifi.where())
        elapsed = round(time.perf_counter() - started, 3)
        return {
            "ok": r.ok,
            "status_code": r.status_code,
            "text": r.text if r.ok else "",
            "elapsed_s": elapsed,
            "url": url,
            "error": "" if r.ok else f"http {r.status_code}",
            "used_insecure_fallback": False,
        }
    except SSLError as e:
        try:
            r = fetch_once(session, url, referer, False)
            elapsed = round(time.perf_counter() - started, 3)
            return {
                "ok": r.ok,
                "status_code": r.status_code,
                "text": r.text if r.ok else "",
                "elapsed_s": elapsed,
                "url": url,
                "error": "" if r.ok else f"http {r.status_code}",
                "used_insecure_fallback": True,
            }
        except HTTPError as http_e:
            elapsed = round(time.perf_counter() - started, 3)
            status_code = getattr(http_e.response, "status_code", None)
            return {
                "ok": False,
                "status_code": status_code,
                "text": "",
                "elapsed_s": elapsed,
                "url": url,
                "error": f"ssl fallback http {status_code}",
                "used_insecure_fallback": True,
            }
        except RequestException as req_e:
            elapsed = round(time.perf_counter() - started, 3)
            return {
                "ok": False,
                "status_code": None,
                "text": "",
                "elapsed_s": elapsed,
                "url": url,
                "error": f"ssl fallback fail: {req_e}",
                "used_insecure_fallback": True,
            }
    except HTTPError as e:
        elapsed = round(time.perf_counter() - started, 3)
        status_code = getattr(e.response, "status_code", None)
        return {
            "ok": False,
            "status_code": status_code,
            "text": "",
            "elapsed_s": elapsed,
            "url": url,
            "error": f"http {status_code}",
            "used_insecure_fallback": False,
        }
    except RequestException as e:
        elapsed = round(time.perf_counter() - started, 3)
        return {
            "ok": False,
            "status_code": None,
            "text": "",
            "elapsed_s": elapsed,
            "url": url,
            "error": str(e),
            "used_insecure_fallback": False,
        }


def get_html(url: str) -> Dict[str, Any]:
    session = requests.Session()
    candidates = [(url, BASE_URL + "/")]
    if url == LIST_URL:
        candidates.append((FALLBACK_LIST_URL, BASE_URL + "/"))

    last_result = None
    for candidate_url, referer in candidates:
        result = fetch_with_fallback(session, candidate_url, referer)
        last_result = result
        if result["ok"] and result["text"]:
            return result

    return last_result or {
        "ok": False,
        "status_code": None,
        "text": "",
        "elapsed_s": 0,
        "url": url,
        "error": "sem resposta",
        "used_insecure_fallback": False,
    }


def extract_all_img_meta_from_block_html(block_html: str) -> List[Dict[str, Any]]:
    imgs: List[Dict[str, Any]] = []
    for m in re.finditer(r"<img([^>]+)>", block_html, re.I):
        attrs = m.group(1) or ""
        src_match = (
            re.search(r'data-src=["\']([^"\']+)["\']', attrs, re.I)
            or re.search(r'data-original=["\']([^"\']+)["\']', attrs, re.I)
            or re.search(r'data-lazy=["\']([^"\']+)["\']', attrs, re.I)
            or re.search(r'src=["\']([^"\']+)["\']', attrs, re.I)
        )
        if not src_match:
            continue
        src = absolutize_url(src_match.group(1))
        if not src or src.startswith("data:image"):
            continue

        class_match = re.search(r'class=["\']([^"\']+)["\']', attrs, re.I)
        title_match = re.search(r'title=["\']([^"\']+)["\']', attrs, re.I)
        alt_match = re.search(r'alt=["\']([^"\']+)["\']', attrs, re.I)
        width_match = re.search(r'width=["\']([^"\']+)["\']', attrs, re.I)
        height_match = re.search(r'height=["\']([^"\']+)["\']', attrs, re.I)

        try:
            width = int(width_match.group(1)) if width_match else 0
        except Exception:
            width = 0

        try:
            height = int(height_match.group(1)) if height_match else 0
        except Exception:
            height = 0

        class_names = (class_match.group(1) if class_match else "").lower()
        title = (title_match.group(1) if title_match else "").strip().lower()
        alt = (alt_match.group(1) if alt_match else "").strip().lower()

        imgs.append(
            {
                "src": src,
                "title": title,
                "alt": alt,
                "class_name": class_names,
                "width": width,
                "height": height,
                "is_partner_path": "/parceiros/" in src,
                "is_partner_like": (
                    "/parceiros/" in src
                    or "logo" in class_names
                    or "brand" in class_names
                    or "parceiro" in class_names
                    or "logo" in alt
                    or bool(title)
                    or (0 < width <= 220)
                    or (0 < height <= 120)
                ),
            }
        )

    return uniq_by(imgs, lambda x: x["src"])


def choose_images_from_block_html(block_html: str) -> Dict[str, str]:
    all_imgs = extract_all_img_meta_from_block_html(block_html)
    partner_img_url = ""
    img_url = ""

    partner_candidates = [img for img in all_imgs if img["is_partner_like"] or img["is_partner_path"]]
    if partner_candidates:
        partner_img_url = partner_candidates[0]["src"]

    banner_candidates = [
        img for img in all_imgs
        if (not partner_img_url or img["src"] != partner_img_url) and is_likely_benefit_banner(img["src"])
    ]
    if banner_candidates:
        img_url = banner_candidates[-1]["src"]

    if not img_url:
        fallback_candidates = [
            img for img in all_imgs
            if (not partner_img_url or img["src"] != partner_img_url) and not is_bad_banner_url(img["src"])
        ]
        if fallback_candidates:
            img_url = fallback_candidates[-1]["src"]

    if not partner_img_url and len(all_imgs) >= 2:
        for img in all_imgs:
            if img["src"] != img_url:
                partner_img_url = img["src"]
                break

    return {"img_url": img_url, "partner_img_url": partner_img_url}


def parse_offers_like_latest_flow(html: str) -> List[Dict[str, Any]]:
    cards = []
    card_regex = re.compile(
        r'<div class="col-12 col-sm-4 col-md-3 mb-3 beneficio"[\s\S]*?<!-- Fim div beneficio -->',
        re.I,
    )
    blocks = card_regex.findall(html)

    for block_html in blocks:
        try:
            href_match = re.search(r'<a[^>]+href=["\']([^"\']+)["\']', block_html, re.I)
            title_match = re.search(r'<p class="titulo mb-0">([\s\S]*?)</p>', block_html, re.I)
            category_match = re.search(r'data-categoria=["\']([^"\']+)["\']', block_html, re.I)

            if not href_match or not title_match:
                continue

            link = absolutize_url(href_match.group(1))
            title = clean_text(re.sub(r"<[^>]+>", " ", title_match.group(1)))
            category = clean_text(category_match.group(1)) if category_match else ""
            images = choose_images_from_block_html(block_html)

            if not link or not title:
                continue

            cards.append(
                {
                    "id": get_offer_id(link),
                    "preview_title": title,
                    "title": title,
                    "link": link,
                    "original_link": link,
                    "category": category,
                    "img_url": images["img_url"],
                    "partner_img_url": images["partner_img_url"],
                }
            )
        except Exception:
            continue

    if cards:
        return uniq_by(cards, lambda o: normalize_offer_key(o.get("id") or o.get("link")))

    # fallback mais próximo do scraper python
    offers = []
    block_regex = re.compile(r"<div[^>]+beneficio[^>]*>([\s\S]*?)</div>", re.I)
    for block_html in block_regex.findall(html):
        href_match = re.search(r'<a[^>]+href=["\']([^"\']+)["\']', block_html, re.I)
        title_match = (
            re.search(r'class=["\'][^"\']*titulo[^"\']*["\'][^>]*>([\s\S]*?)<', block_html, re.I)
            or re.search(r"<h3[^>]*>([\s\S]*?)</h3>", block_html, re.I)
            or re.search(r"<h2[^>]*>([\s\S]*?)</h2>", block_html, re.I)
        )
        if not href_match or not title_match:
            continue

        link = absolutize_url(href_match.group(1))
        title = clean_text(re.sub(r"<[^>]+>", " ", title_match.group(1)))
        images = choose_images_from_block_html(block_html)

        if not link or not title:
            continue

        offers.append(
            {
                "id": get_offer_id(link),
                "preview_title": title,
                "title": title,
                "link": link,
                "original_link": link,
                "category": "",
                "img_url": images["img_url"],
                "partner_img_url": images["partner_img_url"],
            }
        )

    return uniq_by(offers, lambda o: normalize_offer_key(o.get("id") or o.get("link")))


def extract_title_from_detail(html: str, preview_title: str) -> str:
    for regex in [
        re.compile(r"<h2[^>]*>([\s\S]*?)</h2>", re.I),
        re.compile(r"<h1[^>]*>([\s\S]*?)</h1>", re.I),
    ]:
        m = regex.search(html)
        if m:
            title = clean_text(re.sub(r"<[^>]+>", " ", m.group(1)))
            if title:
                return title
    return preview_title


def extract_validity_from_detail(html: str) -> str:
    for regex in [
        re.compile(r"[Bb]enefício válido de[^.!?\n]*[.!?]?", re.I),
        re.compile(r"[Vv]álido até[^.!?\n]*[.!?]?", re.I),
        re.compile(r"\d{2}/\d{2}/\d{4}[\s\S]{0,80}\d{2}/\d{2}/\d{4}", re.I),
    ]:
        m = regex.search(html)
        if m:
            return clean_text(re.sub(r"<[^>]+>", " ", m.group(0)))
    return ""


def extract_description_from_detail(html: str) -> str:
    for regex in [
        re.compile(r'class=["\'][^"\']*info-beneficio[^"\']*["\'][^>]*>([\s\S]*?)(?:<script|<footer|class=["\'][^"\']*box-compartilhar)', re.I),
        re.compile(r'id=["\']beneficio["\'][^>]*>([\s\S]*?)(?:<script|<footer)', re.I),
    ]:
        m = regex.search(html)
        if m:
            txt = html_to_text(m.group(1))
            if len(txt) >= 20:
                return txt[:4000]
    return ""


def extract_detail_image_from_detail(html: str) -> str:
    for m in re.finditer(r'<img[^>]+(?:data-src|data-original|data-lazy|src)=["\']([^"\']+)["\']', html, re.I):
        src = absolutize_url(m.group(1))
        if is_likely_benefit_banner(src):
            return src

    for m in re.finditer(r'<img[^>]+(?:data-src|data-original|data-lazy|src)=["\']([^"\']+)["\']', html, re.I):
        src = absolutize_url(m.group(1))
        if src and not is_bad_banner_url(src):
            return src

    return ""


def fetch_detail_probe(offer: Dict[str, Any]) -> Dict[str, Any]:
    result = get_html(offer["link"])
    if not result["ok"] or not result["text"]:
        return {
            "ok": False,
            "status_code": result["status_code"],
            "elapsed_s": result["elapsed_s"],
            "error": result["error"],
            "title": offer["title"],
            "validity": "",
            "description_preview": "",
            "detail_img_url": "",
            "html_length": 0,
        }

    html = result["text"]
    title = extract_title_from_detail(html, offer["title"])
    validity = extract_validity_from_detail(html)
    description = extract_description_from_detail(html)
    detail_img = extract_detail_image_from_detail(html)

    return {
        "ok": True,
        "status_code": result["status_code"],
        "elapsed_s": result["elapsed_s"],
        "error": "",
        "title": title,
        "validity": validity,
        "description_preview": description[:500],
        "detail_img_url": detail_img,
        "html_length": len(html),
    }


def run_probe() -> Dict[str, Any]:
    result = get_html(LIST_URL)
    html = result["text"] if result["ok"] else ""

    offers = parse_offers_like_latest_flow(html) if html else []
    sample_offers = offers[:3]

    detail_tests = []
    if TEST_DETAIL_FETCH and offers:
        for offer in offers[:MAX_DETAIL_TESTS]:
            detail_tests.append(
                {
                    "link": offer["link"],
                    "card_title": offer["title"],
                    "detail": fetch_detail_probe(offer),
                }
            )

    summary = {
        "tested_at": now_utc_iso(),
        "list_probe": {
            "ok": result["ok"],
            "status_code": result["status_code"],
            "elapsed_s": result["elapsed_s"],
            "error": result["error"],
            "used_insecure_fallback": result["used_insecure_fallback"],
            "html_length": len(html),
            "offers_found": len(offers),
        },
        "offers_sample": sample_offers,
        "detail_tests": detail_tests,
    }

    return summary


def print_human_summary(summary: Dict[str, Any]) -> None:
    list_probe = summary["list_probe"]
    line = (
        f'{summary["tested_at"]} | '
        f'list_ok={list_probe["ok"]} | '
        f'status={list_probe["status_code"]} | '
        f'html={list_probe["html_length"]} | '
        f'offers={list_probe["offers_found"]} | '
        f'tempo={list_probe["elapsed_s"]}s | '
        f'fallback_ssl={list_probe["used_insecure_fallback"]}'
    )
    log(line)

    if list_probe["error"]:
        log(f'erro_lista={list_probe["error"]}')

    for i, offer in enumerate(summary["offers_sample"], start=1):
        log(
            f'oferta_{i} | '
            f'id={offer.get("id")} | '
            f'titulo={offer.get("title")} | '
            f'link={offer.get("link")}'
        )

    for i, detail in enumerate(summary["detail_tests"], start=1):
        d = detail["detail"]
        log(
            f'detalhe_{i} | '
            f'ok={d["ok"]} | '
            f'status={d["status_code"]} | '
            f'tempo={d["elapsed_s"]}s | '
            f'titulo={d["title"]} | '
            f'validade={"sim" if d["validity"] else "nao"} | '
            f'descricao={"sim" if d["description_preview"] else "nao"}'
        )


if __name__ == "__main__":
    summary = run_probe()
    print_human_summary(summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
