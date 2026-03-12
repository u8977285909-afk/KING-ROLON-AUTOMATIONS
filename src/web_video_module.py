# -*- coding: utf-8 -*-
"""
KR VIDEO MODULE — PRO STABLE ✅
(LAYOUT YOUTUBE + RANKING + SEARCH SUPREME + THUMBNAILS + THUMB EDITOR)
Compatible con KING ROLON AUTOMATIONS
"""

from __future__ import annotations

import os
import re
import unicodedata
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from flask import (abort, jsonify, redirect, render_template, request,
                   send_file, session, url_for)

from database import add_video
from database import delete_video as db_delete_video
from database import get_video_by_id, list_videos_by_user

# Public feed
try:
    from database import list_public_videos  # type: ignore
except Exception:
    list_public_videos = None  # type: ignore

# Followers
try:
    from database import is_following  # type: ignore
except Exception:
    is_following = None  # type: ignore

# Likes
try:
    from database import count_video_likes  # type: ignore
    from database import is_video_liked  # type: ignore
    from database import toggle_video_like  # type: ignore
except Exception:
    toggle_video_like = None  # type: ignore
    is_video_liked = None  # type: ignore
    count_video_likes = None  # type: ignore

# Collections / Library
try:
    from database import count_video_collections  # type: ignore
    from database import is_video_collected  # type: ignore
    from database import list_my_library  # type: ignore
    from database import toggle_video_collect  # type: ignore
except Exception:
    toggle_video_collect = None  # type: ignore
    is_video_collected = None  # type: ignore
    list_my_library = None  # type: ignore
    count_video_collections = None  # type: ignore

# Views
try:
    from database import get_video_views_count  # type: ignore
    from database import register_video_view  # type: ignore
except Exception:
    register_video_view = None  # type: ignore
    get_video_views_count = None  # type: ignore

# Comments
try:
    from database import add_video_comment  # type: ignore
    from database import count_video_comments  # type: ignore
    from database import list_video_comments  # type: ignore
except Exception:
    add_video_comment = None  # type: ignore
    list_video_comments = None  # type: ignore
    count_video_comments = None  # type: ignore

# Points
try:
    from database import award_points, get_connection  # type: ignore
except Exception:
    award_points = None  # type: ignore
    get_connection = None  # type: ignore

# Search history / trends
try:
    from database import list_recent_searches  # type: ignore
    from database import list_trending_searches  # type: ignore
    from database import record_search_query  # type: ignore
except Exception:
    list_recent_searches = None  # type: ignore
    list_trending_searches = None  # type: ignore
    record_search_query = None  # type: ignore

# Search supreme
try:
    from database import get_video_hashtags  # type: ignore
except Exception:
    get_video_hashtags = None  # type: ignore

try:
    from database import search_creators  # type: ignore
except Exception:
    search_creators = None  # type: ignore

try:
    from database import search_videos_by_hashtag  # type: ignore
except Exception:
    search_videos_by_hashtag = None  # type: ignore

try:
    from database import search_videos_semantic  # type: ignore
except Exception:
    search_videos_semantic = None  # type: ignore

try:
    from database import list_trending_hashtags  # type: ignore
except Exception:
    list_trending_hashtags = None  # type: ignore

# Thumbnail transform
try:
    from database import get_video_thumbnail_transform  # type: ignore
    from database import update_video_thumbnail_transform  # type: ignore
except Exception:
    get_video_thumbnail_transform = None  # type: ignore
    update_video_thumbnail_transform = None  # type: ignore


_ALLOWED_VIDEO_EXT = {".mp4", ".webm", ".mov"}
_ALLOWED_VIDEO_MIME = {"video/mp4", "video/webm", "video/quicktime"}

_ALLOWED_THUMB_EXT = {".jpg", ".jpeg", ".png", ".webp"}
_ALLOWED_THUMB_MIME = {"image/jpeg", "image/png", "image/webp"}

_ALLOWED_VISIBILITY = {"public", "contacts", "private"}
_ALLOWED_SORTS = {"viral", "for_you", "recent"}
_ALLOWED_SEARCH_AREAS = {"videos", "library"}
_ALLOWED_SEARCH_KINDS = {"general", "creator", "hashtag", "semantic"}

PTS_UPLOAD = 5

_SEARCH_STOPWORDS = {
    "de", "la", "el", "los", "las", "y", "o", "u", "a", "en", "con", "sin",
    "para", "por", "un", "una", "unos", "unas", "del", "al", "mi", "tu",
    "su", "the", "and", "or", "for", "with", "from", "this", "that",
}


# =========================================================
# INTERNAL HELPERS
# =========================================================
def _norm_visibility(v: Optional[str]) -> str:
    vv = (v or "").strip().lower()
    return vv if vv in _ALLOWED_VISIBILITY else "public"


def _norm_sort(v: Optional[str]) -> str:
    vv = (v or "").strip().lower()
    return vv if vv in _ALLOWED_SORTS else "viral"


def _norm_search_area(v: Optional[str]) -> str:
    vv = (v or "").strip().lower()
    return vv if vv in _ALLOWED_SEARCH_AREAS else "videos"


def _norm_search_kind(v: Optional[str]) -> str:
    vv = (v or "").strip().lower()
    return vv if vv in _ALLOWED_SEARCH_KINDS else "general"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _norm_thumb_pos_x(value: Any) -> float:
    return _clamp(_to_float(value, 50.0), -5000.0, 5000.0)


def _norm_thumb_pos_y(value: Any) -> float:
    return _clamp(_to_float(value, 50.0), -5000.0, 5000.0)


def _norm_thumb_scale(value: Any) -> float:
    return _clamp(_to_float(value, 1.0), 0.1, 10.0)


def _safe_video_filename(name: str) -> str:
    name = (name or "").strip()
    name = Path(name).name
    name = name.replace("\\", "/").split("/")[-1]
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_\-\.]", "_", name)
    name = re.sub(r"\.{2,}", ".", name)
    name = name.strip("._-")
    return name or "video"


def _safe_thumb_filename(name: str) -> str:
    name = (name or "").strip()
    name = Path(name).name
    name = name.replace("\\", "/").split("/")[-1]
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_\-\.]", "_", name)
    name = re.sub(r"\.{2,}", ".", name)
    name = name.strip("._-")
    return name or "thumb"


def _sanitize_stored_filename(name: str) -> str:
    name = (name or "").strip()
    name = Path(name).name
    name = name.replace("\\", "/").split("/")[-1].strip()
    return name


def _is_owner_filename(filename: str, user_id: int) -> bool:
    try:
        return str(filename).startswith(f"{int(user_id)}_")
    except Exception:
        return False


def _file_is_inside_dir(file_path: Path, root_dir: Path) -> bool:
    try:
        fp = file_path.resolve()
        rd = root_dir.resolve()
        return str(fp).startswith(str(rd) + os.sep) or fp == rd
    except Exception:
        return False


def _like_system_ready() -> bool:
    return bool(toggle_video_like and is_video_liked and count_video_likes)


def _collection_system_ready() -> bool:
    return bool(toggle_video_collect and is_video_collected)


def _library_system_ready() -> bool:
    return bool(list_my_library)


def _upload_points_ready() -> bool:
    return bool(award_points and get_connection)


def _views_ready() -> bool:
    return bool(register_video_view and get_video_views_count)


def _comments_ready() -> bool:
    return bool(add_video_comment and list_video_comments and count_video_comments)


def _search_history_ready() -> bool:
    return bool(record_search_query and list_recent_searches and list_trending_searches)


def _hashtags_ready() -> bool:
    return bool(get_video_hashtags)


def _creator_search_ready() -> bool:
    return bool(search_creators)


def _hashtag_search_ready() -> bool:
    return bool(search_videos_by_hashtag)


def _semantic_search_ready() -> bool:
    return bool(search_videos_semantic)


def _thumb_transform_ready() -> bool:
    return bool(get_video_thumbnail_transform and update_video_thumbnail_transform)


def _award_once(
    receiver_id: int,
    action: str,
    points: int,
    ref_id: str,
    ref_type: str = "",
    meta: Optional[Any] = None,
) -> bool:
    if not _upload_points_ready():
        return False

    try:
        rid = int(receiver_id)
    except Exception:
        return False

    action = (action or "").strip().lower()
    ref_id = (ref_id or "").strip()
    pts = int(points or 0)
    if not action or not ref_id or pts == 0:
        return False

    try:
        with get_connection() as conn:  # type: ignore
            r = conn.execute(
                """
                SELECT 1 FROM points_ledger
                WHERE user_id=? AND action=? AND ref_id=?
                LIMIT 1;
                """,
                (rid, action, ref_id),
            ).fetchone()
            if r:
                return False
    except Exception:
        return False

    try:
        award_points(  # type: ignore
            rid,
            action=action,
            points=pts,
            ref_type=(ref_type or "").strip(),
            ref_id=ref_id,
            meta=meta,
        )
        return True
    except Exception:
        return False


def _safe_get_video(video_id: str):
    video_id = (video_id or "").strip()
    if not video_id:
        return None
    try:
        return get_video_by_id(video_id)
    except Exception:
        return None


def _get_session_id() -> str:
    sid = session.get("kr_sid")
    if not sid:
        sid = uuid4().hex
        session["kr_sid"] = sid
    return str(sid)


def _client_ip() -> str:
    return (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or ""
    ).strip()


def _can_view_contacts(me: int, owner_id: int) -> bool:
    if int(me) == int(owner_id):
        return True
    if not is_following:
        return False
    try:
        return bool(is_following(me, owner_id) or is_following(owner_id, me))  # type: ignore
    except Exception:
        return False


def _can_view_video(me: int, owner_id: int, visibility: str) -> bool:
    if int(me) == int(owner_id):
        return True
    if visibility == "public":
        return True
    if visibility == "contacts":
        return _can_view_contacts(me, owner_id)
    return False


def _like_state(video_id: str, me: int):
    liked = False
    likes = 0
    if _like_system_ready() and video_id:
        try:
            liked = bool(is_video_liked(video_id, me))  # type: ignore
            likes = int(count_video_likes(video_id))  # type: ignore
        except Exception:
            liked = False
            likes = 0
    return liked, likes


def _collect_state(video_id: str, me: int):
    collected = False
    collections_count = 0

    if _collection_system_ready() and video_id:
        try:
            collected = bool(is_video_collected(video_id, me))  # type: ignore
        except Exception:
            collected = False

    if count_video_collections and video_id:
        try:
            collections_count = int(count_video_collections(video_id))  # type: ignore
        except Exception:
            collections_count = 0

    return collected, collections_count


def _views_count(video_id: str) -> int:
    if not _views_ready() or not video_id:
        return 0
    try:
        return int(get_video_views_count(video_id))  # type: ignore
    except Exception:
        return 0


def _comments_count(video_id: str) -> int:
    if not (_comments_ready() and video_id):
        return 0
    try:
        return int(count_video_comments(video_id))  # type: ignore
    except Exception:
        return 0


def _watch_counts(video_id: str) -> dict:
    return {
        "views": _views_count(video_id),
        "shares": 0,
        "downloads": 0,
        "comments": _comments_count(video_id),
    }


def _get_thumb_transform(video_id: str) -> Dict[str, float]:
    if not (_thumb_transform_ready() and video_id):
        return {"x": 50.0, "y": 50.0, "scale": 1.0}
    try:
        data = get_video_thumbnail_transform(video_id) or {}  # type: ignore
        return {
            "x": _norm_thumb_pos_x(data.get("x", 50.0)),
            "y": _norm_thumb_pos_y(data.get("y", 50.0)),
            "scale": _norm_thumb_scale(data.get("scale", 1.0)),
        }
    except Exception:
        return {"x": 50.0, "y": 50.0, "scale": 1.0}


def _normalize_search_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text


def _search_tokens(query: str) -> List[str]:
    q = _normalize_search_text(query)
    if not q:
        return []
    return [tok for tok in q.split(" ") if tok]


def _extract_hashtags(value: Any) -> List[str]:
    text = str(value or "").strip().lower()
    if not text:
        return []
    matches = re.findall(
        r"(?:^|[\s\.,;:!\?\(\)\[\]\{\}/\\])#([a-zA-Z0-9_áéíóúüñÁÉÍÓÚÜÑ]+)",
        f" {text}",
    )
    out: List[str] = []
    seen = set()
    for tag in matches:
        norm = _normalize_search_text(tag)
        norm = re.sub(r"[^a-z0-9_]", "", norm)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def _clean_creator_query(query: str) -> str:
    q = str(query or "").strip()
    if q.startswith("@"):
        q = q[1:]
    return q.strip()


def _clean_hashtag_query(query: str) -> str:
    q = str(query or "").strip()
    if q.startswith("#"):
        q = q[1:]
    q = _normalize_search_text(q)
    q = re.sub(r"[^a-z0-9_]", "", q)
    return q.strip()


def _detect_search_kind(query: str) -> str:
    raw = str(query or "").strip()
    if not raw:
        return "general"

    if raw.startswith("@"):
        return "creator"

    if raw.startswith("#"):
        return "hashtag"

    if _extract_hashtags(raw):
        return "hashtag"

    tokens = _search_tokens(raw)
    if len(tokens) >= 3:
        return "semantic"

    return "general"


def _token_is_good(tok: str) -> bool:
    tok = (tok or "").strip().lower()
    return len(tok) >= 3 and tok not in _SEARCH_STOPWORDS and not tok.isdigit()


def _build_vocabulary(videos: List[Dict[str, Any]]) -> Dict[str, int]:
    vocab: Dict[str, int] = {}
    for v in videos:
        title_tokens = _search_tokens(v.get("title") or "")
        desc_tokens = _search_tokens(v.get("description") or "")

        for tok in title_tokens:
            if _token_is_good(tok):
                vocab[tok] = vocab.get(tok, 0) + 3

        for tok in desc_tokens:
            if _token_is_good(tok):
                vocab[tok] = vocab.get(tok, 0) + 1

    return vocab


def _suggest_query_from_videos(query: str, videos: List[Dict[str, Any]]) -> str:
    tokens = _search_tokens(query)
    if not tokens:
        return ""

    vocab = _build_vocabulary(videos)
    if not vocab:
        return ""

    vocab_words = list(vocab.keys())
    corrected: List[str] = []
    changed = False

    for tok in tokens:
        if not _token_is_good(tok) or tok in vocab:
            corrected.append(tok)
            continue

        matches = get_close_matches(tok, vocab_words, n=1, cutoff=0.74)
        if matches:
            corrected.append(matches[0])
            changed = True
        else:
            corrected.append(tok)

    candidate = " ".join(corrected).strip()
    normalized_original = _normalize_search_text(query)

    if changed and candidate and candidate != normalized_original:
        return candidate
    return ""


def _search_score(video: Dict[str, Any], tokens: List[str]) -> int:
    if not tokens:
        return 1

    title_raw = str(video.get("title") or "").strip()
    desc_raw = str(video.get("description") or "").strip()

    title = _normalize_search_text(title_raw)
    desc = _normalize_search_text(desc_raw)
    haystack = f"{title} {desc}".strip()

    if not haystack:
        return 0

    score = 0
    matched_tokens = 0
    exact_query = " ".join(tokens).strip()

    if exact_query:
        if title == exact_query:
            score += 140
        elif title.startswith(exact_query):
            score += 80
        elif exact_query in title:
            score += 50
        elif exact_query in haystack:
            score += 24

    for tok in tokens:
        tok = (tok or "").strip()
        if not tok:
            continue

        if tok == title:
            score += 60
            matched_tokens += 1
        elif title.startswith(tok):
            score += 26
            matched_tokens += 1
        elif f" {tok} " in f" {title} ":
            score += 20
            matched_tokens += 1
        elif tok in title:
            score += 14
            matched_tokens += 1
        elif f" {tok} " in f" {haystack} ":
            score += 8
            matched_tokens += 1
        elif tok in haystack:
            score += 5
            matched_tokens += 1
        else:
            compact_tok = tok.replace(" ", "")
            compact_hay = haystack.replace(" ", "")
            if compact_tok and compact_tok in compact_hay:
                score += 2

    if matched_tokens == len(tokens):
        score += 35
    elif matched_tokens >= max(1, len(tokens) - 1):
        score += 16

    try:
        views = int(video.get("views_count") or 0)
        likes = int(video.get("likes_count") or 0)
        saves = int(video.get("collections_count") or 0)

        score += min(views // 25, 20)
        score += min(likes * 2, 18)
        score += min(saves * 3, 18)
    except Exception:
        pass

    return score


def _engagement_tuple(v: Dict[str, Any]):
    try:
        return (
            int(v.get("views_count") or 0),
            int(v.get("likes_count") or 0),
            int(v.get("collections_count") or 0),
            str(v.get("created_at") or ""),
        )
    except Exception:
        return (0, 0, 0, "")


def _user_interest_tokens(me: int, area: str, limit: int = 12) -> List[str]:
    bag: List[str] = []

    for q in _recent_searches(me, area, limit=6):
        bag.extend(_search_tokens(q))

    for q in _trending_searches(area, limit=6):
        bag.extend(_search_tokens(q))

    scored: Dict[str, int] = {}
    for tok in bag:
        if _token_is_good(tok):
            scored[tok] = scored.get(tok, 0) + 1

    ordered = sorted(scored.items(), key=lambda x: (-x[1], x[0]))
    return [k for k, _ in ordered[:limit]]


def _sort_feed_videos(
    videos: List[Dict[str, Any]],
    sort_mode: str = "viral",
    me: Optional[int] = None,
    area: str = "videos",
) -> List[Dict[str, Any]]:
    sort_mode = _norm_sort(sort_mode)

    if sort_mode == "recent":
        return sorted(
            videos,
            key=lambda v: str(v.get("created_at") or ""),
            reverse=True,
        )

    if sort_mode == "for_you":
        interest_tokens = _user_interest_tokens(int(me), area) if me else []

        def fy_score(v: Dict[str, Any]):
            interest_score = _search_score(v, interest_tokens) if interest_tokens else 0
            return (
                interest_score,
                1 if v.get("is_mine") else 0,
                1 if v.get("following") else 0,
                int(v.get("views_count") or 0),
                int(v.get("likes_count") or 0),
                int(v.get("collections_count") or 0),
                str(v.get("created_at") or ""),
            )

        return sorted(videos, key=fy_score, reverse=True)

    return sorted(videos, key=_engagement_tuple, reverse=True)


def _apply_video_search_and_sort(
    videos: List[Dict[str, Any]],
    query: str,
    sort_mode: str,
    me: Optional[int] = None,
    area: str = "videos",
) -> List[Dict[str, Any]]:
    filtered = list(videos or [])
    tokens = _search_tokens(query)

    if tokens:
        ranked = []
        for v in filtered:
            score = _search_score(v, tokens)
            if score > 0:
                vv = dict(v)
                vv["_search_score"] = score
                ranked.append(vv)

        filtered = sorted(
            ranked,
            key=lambda v: (
                int(v.get("_search_score") or 0),
                int(v.get("views_count") or 0),
                int(v.get("likes_count") or 0),
                int(v.get("collections_count") or 0),
                str(v.get("created_at") or ""),
            ),
            reverse=True,
        )
    else:
        filtered = _sort_feed_videos(filtered, sort_mode=sort_mode, me=me, area=area)

    if tokens and sort_mode == "recent":
        filtered = sorted(
            filtered,
            key=lambda v: (
                int(v.get("_search_score") or 0),
                str(v.get("created_at") or ""),
            ),
            reverse=True,
        )
    elif tokens and sort_mode == "for_you":
        interest_tokens = _user_interest_tokens(int(me), area) if me else []

        filtered = sorted(
            filtered,
            key=lambda v: (
                int(v.get("_search_score") or 0),
                _search_score(v, interest_tokens) if interest_tokens else 0,
                1 if v.get("is_mine") else 0,
                1 if v.get("following") else 0,
                int(v.get("views_count") or 0),
                int(v.get("likes_count") or 0),
                int(v.get("collections_count") or 0),
            ),
            reverse=True,
        )

    for v in filtered:
        if "_search_score" in v:
            try:
                del v["_search_score"]
            except Exception:
                pass

    return filtered


def _build_search_suggestions(
    videos: List[Dict[str, Any]],
    query: str,
    limit: int = 8,
) -> List[str]:
    suggestions: List[str] = []
    seen = set()

    def add(text: str):
        raw = str(text or "").strip()
        norm = _normalize_search_text(raw)
        if not raw or not norm or norm in seen:
            return
        seen.add(norm)
        suggestions.append(raw)

    ordered = _sort_feed_videos(list(videos or []), "viral")
    normalized_query = _normalize_search_text(query)
    tokens = _search_tokens(normalized_query)

    if normalized_query:
        maybe = _suggest_query_from_videos(normalized_query, ordered)
        if maybe:
            add(maybe)

        for v in ordered:
            title = str(v.get("title") or "").strip()
            desc = str(v.get("description") or "").strip()

            if not title:
                continue

            nt = _normalize_search_text(title)
            nd = _normalize_search_text(desc)

            if normalized_query in nt or any(tok in nt for tok in tokens):
                add(title)
            elif any(tok in nd for tok in tokens):
                add(title)

            if len(suggestions) >= limit:
                return suggestions[:limit]

        vocab = _build_vocabulary(ordered)
        if tokens:
            last = tokens[-1]
            prefix = tokens[:-1]

            starts = sorted(
                [w for w in vocab.keys() if w.startswith(last) and w != last],
                key=lambda w: (-vocab[w], w),
            )
            for word in starts:
                candidate = " ".join(prefix + [word]).strip()
                add(candidate)
                if len(suggestions) >= limit:
                    return suggestions[:limit]

            close = get_close_matches(last, list(vocab.keys()), n=5, cutoff=0.72)
            for word in close:
                candidate = " ".join(prefix + [word]).strip()
                add(candidate)
                if len(suggestions) >= limit:
                    return suggestions[:limit]
    else:
        for v in ordered:
            title = str(v.get("title") or "").strip()
            if title:
                add(title)
            if len(suggestions) >= limit:
                return suggestions[:limit]

    return suggestions[:limit]


def _creator_suggestions(query: str, limit: int = 4) -> List[str]:
    if not (_creator_search_ready() and query):
        return []

    try:
        rows = search_creators(query, limit=limit, only_public=True) or []  # type: ignore
    except Exception:
        rows = []

    out: List[str] = []
    seen = set()
    for r in rows:
        username = str(r.get("username") or "").strip()
        name = str(r.get("name") or "").strip()
        candidate = f"@{username}" if username else name
        norm = _normalize_search_text(candidate)
        if candidate and norm and norm not in seen:
            seen.add(norm)
            out.append(candidate)
    return out[:limit]


def _hashtag_suggestions(query: str, videos: List[Dict[str, Any]], limit: int = 4) -> List[str]:
    out: List[str] = []
    seen = set()

    q_norm = _normalize_search_text(query)
    query_tags = _extract_hashtags(query)

    if query_tags and _hashtag_search_ready():
        base_tag = query_tags[0]
        try:
            rows = search_videos_by_hashtag(base_tag, viewer_user_id=None, limit=24) or []  # type: ignore
        except Exception:
            rows = []

        for row in rows:
            for tag in row.get("hashtags", []) or []:
                raw = str(tag or "").strip()
                norm = _normalize_search_text(raw)
                if raw and norm not in seen:
                    seen.add(norm)
                    out.append(raw)
                    if len(out) >= limit:
                        return out[:limit]

    tags_bank: Dict[str, int] = {}
    for v in videos:
        tags = v.get("hashtags") or []
        if not tags:
            text_tags = _extract_hashtags(f"{v.get('title') or ''} {v.get('description') or ''}")
            tags = [f"#{x}" for x in text_tags]

        for tag in tags:
            raw = str(tag or "").strip()
            norm = _normalize_search_text(raw).lstrip("#")
            if not norm:
                continue
            if q_norm and q_norm not in norm and norm not in q_norm:
                continue
            tags_bank[raw] = tags_bank.get(raw, 0) + 1

    ordered = sorted(tags_bank.items(), key=lambda x: (-x[1], x[0]))
    for raw, _ in ordered:
        norm = _normalize_search_text(raw)
        if raw and norm not in seen:
            seen.add(norm)
            out.append(raw)
            if len(out) >= limit:
                return out[:limit]

    if list_trending_hashtags and not out and not query:
        try:
            rows = list_trending_hashtags(limit=limit) or []  # type: ignore
        except Exception:
            rows = []
        for raw in rows:
            norm = _normalize_search_text(raw)
            if raw and norm not in seen:
                seen.add(norm)
                out.append(raw)
                if len(out) >= limit:
                    break

    return out[:limit]


def _merge_search_buckets(
    *,
    recent: Optional[List[str]] = None,
    trending: Optional[List[str]] = None,
    suggestions: Optional[List[str]] = None,
    creators: Optional[List[str]] = None,
    hashtags: Optional[List[str]] = None,
    limit: int = 10,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen = set()

    def push(items: Optional[List[str]], kind: str):
        if not items:
            return
        for item in items:
            raw = str(item or "").strip()
            norm = _normalize_search_text(raw)
            if not raw or not norm or norm in seen:
                continue
            seen.add(norm)
            out.append({"text": raw, "kind": kind})
            if len(out) >= limit:
                return

    push(recent, "recent")
    if len(out) < limit:
        push(creators, "creator")
    if len(out) < limit:
        push(hashtags, "hashtag")
    if len(out) < limit:
        push(suggestions, "suggest")
    if len(out) < limit:
        push(trending, "trend")

    return out[:limit]


def _top_rank_rows(limit: int = 10) -> List[Dict[str, Any]]:
    if not get_connection:
        return []

    q_with_user_points = """
    WITH
      f AS (
        SELECT followed_id AS user_id, COUNT(*) AS followers
        FROM followers
        GROUP BY followed_id
      ),
      vl AS (
        SELECT v.user_id AS user_id, COUNT(*) AS likes_received
        FROM video_likes l
        JOIN videos v ON v.id = l.video_id
        GROUP BY v.user_id
      ),
      vc AS (
        SELECT v.user_id AS user_id, COUNT(*) AS collections_received
        FROM video_collections c
        JOIN videos v ON v.id = c.video_id
        GROUP BY v.user_id
      )
    SELECT
      u.id AS user_id,
      COALESCE(u.name, u.email) AS display_name,
      COALESCE(p.points, 0) AS points,
      COALESCE(f.followers, 0) AS followers,
      COALESCE(vl.likes_received, 0) AS likes_received,
      COALESCE(vc.collections_received, 0) AS collections_received,
      (
        COALESCE(p.points, 0)
        + COALESCE(f.followers, 0)
        + (COALESCE(vl.likes_received, 0) * 2)
        + (COALESCE(vc.collections_received, 0) * 3)
      ) AS score
    FROM users u
    LEFT JOIN user_points p ON p.user_id = u.id
    LEFT JOIN f  ON f.user_id  = u.id
    LEFT JOIN vl ON vl.user_id = u.id
    LEFT JOIN vc ON vc.user_id = u.id
    ORDER BY score DESC, followers DESC, likes_received DESC, collections_received DESC, u.id ASC
    LIMIT ?;
    """

    q_no_points_table = """
    WITH
      f AS (
        SELECT followed_id AS user_id, COUNT(*) AS followers
        FROM followers
        GROUP BY followed_id
      ),
      vl AS (
        SELECT v.user_id AS user_id, COUNT(*) AS likes_received
        FROM video_likes l
        JOIN videos v ON v.id = l.video_id
        GROUP BY v.user_id
      ),
      vc AS (
        SELECT v.user_id AS user_id, COUNT(*) AS collections_received
        FROM video_collections c
        JOIN videos v ON v.id = c.video_id
        GROUP BY v.user_id
      )
    SELECT
      u.id AS user_id,
      COALESCE(u.name, u.email) AS display_name,
      0 AS points,
      COALESCE(f.followers, 0) AS followers,
      COALESCE(vl.likes_received, 0) AS likes_received,
      COALESCE(vc.collections_received, 0) AS collections_received,
      (
        COALESCE(f.followers, 0)
        + (COALESCE(vl.likes_received, 0) * 2)
        + (COALESCE(vc.collections_received, 0) * 3)
      ) AS score
    FROM users u
    LEFT JOIN f  ON f.user_id  = u.id
    LEFT JOIN vl ON vl.user_id = u.id
    LEFT JOIN vc ON vc.user_id = u.id
    ORDER BY score DESC, followers DESC, likes_received DESC, collections_received DESC, u.id ASC
    LIMIT ?;
    """

    try:
        with get_connection() as conn:  # type: ignore
            has_points = False
            try:
                r = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='user_points' LIMIT 1;"
                ).fetchone()
                has_points = bool(r)
            except Exception:
                has_points = False

            q = q_with_user_points if has_points else q_no_points_table
            rows = conn.execute(q, (int(limit),)).fetchall()
            return [dict(r) for r in rows] if rows else []
    except Exception:
        return []


def _default_ad_card() -> Dict[str, str]:
    return {
        "badge": "AD",
        "title": "Marketplace KR",
        "text": "Automatízate premium y crece más rápido.",
        "cta": "Ver oferta",
        "href": "/marketplace",
    }


def _gather_feed_rows(me: int) -> List[Dict[str, Any]]:
    public_other = []
    if list_public_videos:
        try:
            public_other = list_public_videos(exclude_user_id=me, limit=300) or []
        except Exception:
            public_other = []

    try:
        mine = list_videos_by_user(me, limit=300) or []
    except Exception:
        mine = []

    rows: List[Dict[str, Any]] = []
    rows.extend(public_other)
    rows.extend(mine)
    return rows


def _gather_library_rows(me: int) -> List[Dict[str, Any]]:
    if not _library_system_ready():
        return []
    try:
        return list_my_library(me, limit=400) or []  # type: ignore
    except Exception:
        return []


def _search_videos_by_creator_query(
    me: int,
    query: str,
    area: str = "videos",
    limit: int = 300,
) -> List[Dict[str, Any]]:
    q = _clean_creator_query(query)
    if not q:
        return []

    creator_ids: List[int] = []

    if _creator_search_ready():
        try:
            rows = search_creators(q, limit=12, only_public=False) or []  # type: ignore
        except Exception:
            rows = []

        for row in rows:
            try:
                creator_ids.append(int(row.get("id") or 0))
            except Exception:
                pass

    if creator_ids and get_connection:
        try:
            placeholders = ",".join("?" * len(creator_ids))
            with get_connection() as conn:  # type: ignore
                if area == "library":
                    sql = f"""
                    SELECT DISTINCT v.*
                    FROM video_collections vc
                    JOIN videos v ON v.id = vc.video_id
                    WHERE vc.user_id = ?
                      AND v.user_id IN ({placeholders})
                    ORDER BY v.created_at DESC
                    LIMIT ?;
                    """
                    params = [int(me)] + creator_ids + [int(limit)]
                else:
                    sql = f"""
                    SELECT v.*
                    FROM videos v
                    WHERE v.user_id IN ({placeholders})
                    ORDER BY v.created_at DESC
                    LIMIT ?;
                    """
                    params = creator_ids + [int(limit)]

                rows = conn.execute(sql, tuple(params)).fetchall()
                return [dict(r) for r in rows]
        except Exception:
            pass

    base_rows = _gather_library_rows(me) if area == "library" else _gather_feed_rows(me)
    decorated = _decorate_video_rows(base_rows, me)

    q_norm = _normalize_search_text(q)
    out: List[Dict[str, Any]] = []
    for v in decorated:
        owner_label = f"{v.get('creator_username') or ''} {v.get('creator_name') or ''}"
        owner_norm = _normalize_search_text(owner_label)
        if q_norm and q_norm in owner_norm:
            out.append(v)
    return out


def _search_videos_by_hashtag_query(
    me: int,
    query: str,
    area: str = "videos",
    limit: int = 300,
) -> List[Dict[str, Any]]:
    tag = _clean_hashtag_query(query)
    if not tag:
        return []

    if _hashtag_search_ready():
        try:
            rows = search_videos_by_hashtag(tag, viewer_user_id=me, limit=limit) or []  # type: ignore
            if area == "library":
                library_ids = {str(x.get("id") or "") for x in _gather_library_rows(me)}
                rows = [r for r in rows if str(r.get("id") or "") in library_ids]
            return rows
        except Exception:
            pass

    base_rows = _gather_library_rows(me) if area == "library" else _gather_feed_rows(me)
    out: List[Dict[str, Any]] = []

    for r in base_rows:
        text_tags = _extract_hashtags(f"{r.get('title') or ''} {r.get('description') or ''}")
        if tag in text_tags:
            out.append(r)

    return out


def _merge_unique_videos(*video_lists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen = set()

    for lst in video_lists:
        for item in lst or []:
            vid = str(item.get("id") or "").strip()
            if not vid or vid in seen:
                continue
            seen.add(vid)
            out.append(item)

    return out


def _ensure_thumbnail_column() -> bool:
    if not get_connection:
        return False

    try:
        with get_connection() as conn:  # type: ignore
            cols = conn.execute("PRAGMA table_info(videos);").fetchall()
            names = {str(c[1]) for c in cols}
            if "thumbnail_filename" in names:
                return True

            conn.execute("ALTER TABLE videos ADD COLUMN thumbnail_filename TEXT;")
            try:
                conn.commit()
            except Exception:
                pass
            return True
    except Exception:
        return False


def _set_video_thumbnail(video_id: str, thumbnail_filename: Optional[str]) -> bool:
    vid = str(video_id or "").strip()
    if not vid or not get_connection:
        return False

    if not _ensure_thumbnail_column():
        return False

    try:
        with get_connection() as conn:  # type: ignore
            conn.execute(
                "UPDATE videos SET thumbnail_filename=? WHERE id=?;",
                ((thumbnail_filename or "").strip() or None, vid),
            )
            try:
                conn.commit()
            except Exception:
                pass
        return True
    except Exception:
        return False


def _create_video_record(
    *,
    user_id: int,
    filename: str,
    thumbnail_filename: Optional[str],
    title: str,
    description: str,
    size_bytes: int,
    visibility: str,
    thumbnail_pos_x: float,
    thumbnail_pos_y: float,
    thumbnail_scale: float,
) -> str:
    try:
        return add_video(
            user_id=user_id,
            filename=filename,
            thumbnail_filename=thumbnail_filename,
            thumbnail_pos_x=thumbnail_pos_x,
            thumbnail_pos_y=thumbnail_pos_y,
            thumbnail_scale=thumbnail_scale,
            title=title,
            description=description,
            size_bytes=size_bytes,
            visibility=visibility,
        )
    except TypeError:
        vid = add_video(
            user_id=user_id,
            filename=filename,
            thumbnail_filename=thumbnail_filename,
            title=title,
            description=description,
            size_bytes=size_bytes,
            visibility=visibility,
        )
        if vid and thumbnail_filename:
            _set_video_thumbnail(vid, thumbnail_filename)
        if vid and _thumb_transform_ready():
            try:
                update_video_thumbnail_transform(  # type: ignore
                    vid,
                    thumbnail_pos_x=thumbnail_pos_x,
                    thumbnail_pos_y=thumbnail_pos_y,
                    thumbnail_scale=thumbnail_scale,
                    user_id=user_id,
                )
            except Exception:
                pass
        return vid


def _decorate_video_rows(rows: List[Dict[str, Any]], me: int) -> List[Dict[str, Any]]:
    clean = []

    creator_map: Dict[int, Dict[str, Any]] = {}
    video_tags_map: Dict[str, List[str]] = {}

    if get_connection:
        owner_ids = []
        video_ids = []

        for r in rows:
            if isinstance(r, dict):
                try:
                    owner_ids.append(int(r.get("user_id") or 0))
                except Exception:
                    pass
                vid = str(r.get("id") or "").strip()
                if vid:
                    video_ids.append(vid)

        owner_ids = sorted({x for x in owner_ids if x > 0})
        video_ids = sorted({x for x in video_ids if x})

        try:
            with get_connection() as conn:  # type: ignore
                if owner_ids:
                    owner_placeholders = ",".join("?" * len(owner_ids))
                    users_rows = conn.execute(
                        f"""
                        SELECT
                          id,
                          COALESCE(username, '') AS username,
                          COALESCE(name, '') AS name,
                          COALESCE(public_slug, '') AS public_slug,
                          COALESCE(avatar_url, '') AS avatar_url,
                          COALESCE(verified, 0) AS verified
                        FROM users
                        WHERE id IN ({owner_placeholders});
                        """,
                        tuple(owner_ids),
                    ).fetchall()

                    for ur in users_rows:
                        creator_map[int(ur["id"])] = dict(ur)

                if video_ids:
                    video_placeholders = ",".join("?" * len(video_ids))
                    tags_rows = conn.execute(
                        f"""
                        SELECT video_id, hashtag
                        FROM video_hashtags
                        WHERE video_id IN ({video_placeholders})
                        ORDER BY normalized_hashtag ASC;
                        """,
                        tuple(video_ids),
                    ).fetchall()

                    for tr in tags_rows:
                        vid = str(tr["video_id"] or "").strip()
                        tag = str(tr["hashtag"] or "").strip()
                        if not vid or not tag:
                            continue
                        video_tags_map.setdefault(vid, []).append(tag)
        except Exception:
            creator_map = {}
            video_tags_map = {}

    for r in rows:
        if not isinstance(r, dict):
            continue

        owner_id = int(r.get("user_id") or 0)
        vis = _norm_visibility(r.get("visibility"))
        vid = str(r.get("id") or "").strip()

        if not _can_view_video(me, owner_id, vis):
            continue

        following_now = False
        if is_following and owner_id and owner_id != me:
            try:
                following_now = bool(is_following(me, owner_id))  # type: ignore
            except Exception:
                following_now = False

        liked, likes = _like_state(vid, me)
        collected, collections_count = _collect_state(vid, me)
        views = _views_count(vid) if vid else int(r.get("views_count") or 0)
        thumb_transform = _get_thumb_transform(vid)

        creator = creator_map.get(owner_id, {})
        hashtags = video_tags_map.get(vid, [])

        clean.append(
            {
                "id": r.get("id"),
                "user_id": owner_id,
                "filename": r.get("filename"),
                "thumbnail_filename": r.get("thumbnail_filename") or "",
                "thumbnail_pos_x": thumb_transform["x"],
                "thumbnail_pos_y": thumb_transform["y"],
                "thumbnail_scale": thumb_transform["scale"],
                "title": r.get("title") or "",
                "description": r.get("description") or "",
                "size_bytes": int(r.get("size_bytes") or 0),
                "created_at": r.get("created_at") or "",
                "visibility": vis,
                "views_count": int(views),
                "is_mine": (owner_id == me),
                "can_follow": (owner_id != me),
                "following": following_now,
                "can_like": _like_system_ready(),
                "liked": liked,
                "likes_count": likes,
                "can_collect": _collection_system_ready(),
                "collected": collected,
                "collections_count": collections_count,
                "creator_username": creator.get("username") or "",
                "creator_name": creator.get("name") or "",
                "creator_public_slug": creator.get("public_slug") or "",
                "creator_avatar_url": creator.get("avatar_url") or "",
                "creator_verified": int(creator.get("verified") or 0),
                "hashtags": hashtags,
            }
        )

    return clean


def _search_area_from_scope(scope: str) -> str:
    return "library" if (scope or "").strip().lower() == "library" else "videos"


def _record_search_if_needed(me: int, query: str, area: str, kind: str = "general") -> None:
    if not _search_history_ready():
        return
    raw = (query or "").strip()
    if len(raw) < 2:
        return

    try:
        record_search_query(me, raw, area=area, kind=kind)  # type: ignore
        return
    except TypeError:
        pass
    except Exception:
        return

    try:
        record_search_query(me, raw, area=area)  # type: ignore
    except Exception:
        pass


def _recent_searches(me: int, area: str, limit: int = 5, kind: Optional[str] = None) -> List[str]:
    if not _search_history_ready():
        return []

    try:
        if kind is not None:
            return list_recent_searches(me, area=area, limit=limit, kind=kind) or []  # type: ignore
        return list_recent_searches(me, area=area, limit=limit) or []  # type: ignore
    except Exception:
        return []


def _trending_searches(area: str, limit: int = 5, kind: Optional[str] = None) -> List[str]:
    if not _search_history_ready():
        return []

    try:
        if kind is not None:
            return list_trending_searches(area=area, limit=limit, kind=kind) or []  # type: ignore
        return list_trending_searches(area=area, limit=limit) or []  # type: ignore
    except Exception:
        return []


def _run_supreme_search(
    me: int,
    area: str,
    query: str,
    sort_mode: str,
) -> Dict[str, Any]:
    raw_query = (query or "").strip()
    base_rows = _gather_library_rows(me) if area == "library" else _gather_feed_rows(me)
    base_clean = _decorate_video_rows(base_rows, me)

    if not raw_query:
        ordered = _sort_feed_videos(base_clean, sort_mode=sort_mode, me=me, area=area)
        return {
            "videos": ordered,
            "search_kind": "general",
            "did_you_mean": "",
            "creator_results": [],
            "hashtag_results": [],
            "semantic_results": [],
        }

    search_kind = _detect_search_kind(raw_query)
    did_you_mean = _suggest_query_from_videos(raw_query, base_clean)

    creator_results: List[Dict[str, Any]] = []
    hashtag_results: List[Dict[str, Any]] = []
    semantic_results: List[Dict[str, Any]] = []
    final_videos: List[Dict[str, Any]] = []

    if search_kind == "creator":
        creator_rows = _search_videos_by_creator_query(me, raw_query, area=area, limit=300)
        creator_results = _decorate_video_rows(creator_rows, me)
        final_videos = _apply_video_search_and_sort(
            creator_results,
            _clean_creator_query(raw_query),
            sort_mode,
            me=me,
            area=area,
        )

    elif search_kind == "hashtag":
        hashtag_rows = _search_videos_by_hashtag_query(me, raw_query, area=area, limit=300)
        hashtag_results = _decorate_video_rows(hashtag_rows, me)
        final_videos = _apply_video_search_and_sort(
            hashtag_results,
            _clean_hashtag_query(raw_query),
            sort_mode,
            me=me,
            area=area,
        )

    elif search_kind == "semantic":
        sem_rows = []
        if _semantic_search_ready():
            try:
                sem_rows = search_videos_semantic(raw_query, viewer_user_id=me, limit=300) or []  # type: ignore
            except Exception:
                sem_rows = []

        if area == "library":
            lib_ids = {str(x.get("id") or "") for x in base_rows}
            sem_rows = [x for x in sem_rows if str(x.get("id") or "") in lib_ids]

        semantic_results = _decorate_video_rows(sem_rows, me)
        lexical_results = _apply_video_search_and_sort(base_clean, raw_query, sort_mode, me=me, area=area)
        merged = _merge_unique_videos(lexical_results, semantic_results)
        final_videos = _apply_video_search_and_sort(merged, raw_query, sort_mode, me=me, area=area)

    else:
        lexical_results = _apply_video_search_and_sort(base_clean, raw_query, sort_mode, me=me, area=area)

        sem_rows = []
        if _semantic_search_ready():
            try:
                sem_rows = search_videos_semantic(raw_query, viewer_user_id=me, limit=180) or []  # type: ignore
            except Exception:
                sem_rows = []

        if area == "library":
            lib_ids = {str(x.get("id") or "") for x in base_rows}
            sem_rows = [x for x in sem_rows if str(x.get("id") or "") in lib_ids]

        semantic_results = _decorate_video_rows(sem_rows, me)
        merged = _merge_unique_videos(lexical_results, semantic_results)
        final_videos = _apply_video_search_and_sort(merged, raw_query, sort_mode, me=me, area=area)

    return {
        "videos": final_videos,
        "search_kind": search_kind,
        "did_you_mean": did_you_mean,
        "creator_results": creator_results,
        "hashtag_results": hashtag_results,
        "semantic_results": semantic_results,
    }


# =========================================================
# ROUTES REGISTER
# =========================================================
def register_video_routes(app, login_required, uid, log_event, project_dir):
    project_dir = Path(project_dir)

    media_dir = project_dir / "media"
    videos_dir = media_dir / "videos"
    thumbs_dir = media_dir / "video_thumbs"

    for d in (media_dir, videos_dir, thumbs_dir):
        d.mkdir(parents=True, exist_ok=True)

    app.config.setdefault("MAX_VIDEO_SIZE", 500 * 1024 * 1024)  # 500MB
    app.config.setdefault("MAX_THUMB_SIZE", 8 * 1024 * 1024)  # 8MB

    # ---------------- FEED ----------------
    @app.get("/videos", endpoint="vm_videos_feed")
    @login_required
    def videos_feed():
        me = uid()
        if not me:
            return redirect(url_for("auth"))
        me = int(me)

        search_query = (request.args.get("q") or "").strip()
        sort_mode = _norm_sort(request.args.get("sort"))
        area = "videos"
        search_kind = _detect_search_kind(search_query) if search_query else "general"

        result = _run_supreme_search(me, area, search_query, sort_mode)
        clean = result.get("videos") or []
        top_rank = _top_rank_rows(limit=10)

        if search_query:
            _record_search_if_needed(me, search_query, area, search_kind)

        return render_template(
            "videos.html",
            active_page="videos",
            videos=clean,
            me=me,
            top_rank=top_rank,
            ad_card=_default_ad_card(),
            search_query=search_query,
            sort_mode=sort_mode,
            search_kind=result.get("search_kind") or search_kind,
            result_count=len(clean),
            did_you_mean=result.get("did_you_mean") or "",
            recent_searches=_recent_searches(me, area, 5),
            trending_searches=_trending_searches(area, 5),
            creator_result_count=len(result.get("creator_results") or []),
            hashtag_result_count=len(result.get("hashtag_results") or []),
            semantic_result_count=len(result.get("semantic_results") or []),
        )

    # ---------------- UPLOAD PAGE ----------------
    @app.get("/videos/upload", endpoint="vm_upload_video_page")
    @login_required
    def upload_video_page():
        me = uid()
        if not me:
            return redirect(url_for("auth"))
        return render_template("upload_video.html", active_page="videos", me=int(me))

    # ---------------- LIBRARY ----------------
    @app.get("/videos/library", endpoint="vm_videos_library")
    @login_required
    def videos_library():
        me = uid()
        if not me:
            return redirect(url_for("auth"))
        me = int(me)

        search_query = (request.args.get("q") or "").strip()
        sort_mode = _norm_sort(request.args.get("sort"))
        area = "library"
        search_kind = _detect_search_kind(search_query) if search_query else "general"

        result = _run_supreme_search(me, area, search_query, sort_mode)
        clean = result.get("videos") or []

        if search_query:
            _record_search_if_needed(me, search_query, area, search_kind)

        return render_template(
            "videos.html",
            active_page="library",
            videos=clean,
            me=me,
            top_rank=_top_rank_rows(limit=10),
            ad_card=_default_ad_card(),
            search_query=search_query,
            sort_mode=sort_mode,
            search_kind=result.get("search_kind") or search_kind,
            result_count=len(clean),
            did_you_mean=result.get("did_you_mean") or "",
            recent_searches=_recent_searches(me, area, 5),
            trending_searches=_trending_searches(area, 5),
            creator_result_count=len(result.get("creator_results") or []),
            hashtag_result_count=len(result.get("hashtag_results") or []),
            semantic_result_count=len(result.get("semantic_results") or []),
        )

    # ---------------- API: SEARCH SUGGEST ----------------
    @app.get("/api/videos/suggest", endpoint="vm_api_video_suggest")
    @login_required
    def api_video_suggest():
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        q = (request.args.get("q") or "").strip()
        scope = (request.args.get("scope") or "feed").strip().lower()
        area = _search_area_from_scope(scope)

        if scope == "library":
            rows = _gather_library_rows(me)
        else:
            rows = _gather_feed_rows(me)

        clean = _decorate_video_rows(rows, me)
        smart_suggestions = _build_search_suggestions(clean, q, limit=8)
        did_you_mean = _suggest_query_from_videos(q, clean) if q else ""

        creator_suggests = _creator_suggestions(q, limit=4)
        hashtag_suggests = _hashtag_suggestions(q, clean, limit=4)

        recent = _recent_searches(me, area, 4)
        trending = _trending_searches(area, 4)

        mixed = _merge_search_buckets(
            recent=(recent if not q else recent[:2]),
            trending=(trending if not q else trending[:2]),
            suggestions=smart_suggestions,
            creators=creator_suggests,
            hashtags=hashtag_suggests,
            limit=10,
        )

        return jsonify(
            ok=True,
            items=mixed,
            suggestions=[x.get("text", "") for x in mixed],
            recent=recent,
            trending=trending,
            did_you_mean=did_you_mean,
        )

    # ---------------- WATCH ----------------
    @app.get("/videos/watch/<video_id>", endpoint="vm_watch_video")
    @login_required
    def watch_video(video_id: str):
        me = uid()
        if not me:
            return redirect(url_for("auth"))
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return redirect(url_for("vm_videos_feed"))

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            abort(403)

        vid = str(row.get("id") or "").strip()

        if vid and _views_ready():
            try:
                register_video_view(  # type: ignore
                    vid,
                    user_id=me,
                    session_id=_get_session_id(),
                    ip=_client_ip(),
                )
            except Exception:
                pass

        following_now = False
        if is_following and owner_id and owner_id != me:
            try:
                following_now = bool(is_following(me, owner_id))  # type: ignore
            except Exception:
                following_now = False

        liked, likes = _like_state(vid, me)
        collected, collections_count = _collect_state(vid, me)
        stats = _watch_counts(vid)
        thumb_transform = _get_thumb_transform(vid)

        hashtags = []
        if _hashtags_ready():
            try:
                hashtags = get_video_hashtags(vid) or []  # type: ignore
            except Exception:
                hashtags = []

        creator_username = ""
        creator_name = ""
        creator_public_slug = ""
        creator_verified = 0

        if get_connection and owner_id:
            try:
                with get_connection() as conn:  # type: ignore
                    ur = conn.execute(
                        """
                        SELECT
                          COALESCE(username,'') AS username,
                          COALESCE(name,'') AS name,
                          COALESCE(public_slug,'') AS public_slug,
                          COALESCE(verified,0) AS verified
                        FROM users
                        WHERE id = ?
                        LIMIT 1;
                        """,
                        (owner_id,),
                    ).fetchone()
                    if ur:
                        creator_username = str(ur["username"] or "")
                        creator_name = str(ur["name"] or "")
                        creator_public_slug = str(ur["public_slug"] or "")
                        creator_verified = int(ur["verified"] or 0)
            except Exception:
                pass

        v = {
            "id": row.get("id"),
            "user_id": owner_id,
            "filename": row.get("filename"),
            "thumbnail_filename": row.get("thumbnail_filename") or "",
            "thumbnail_pos_x": thumb_transform["x"],
            "thumbnail_pos_y": thumb_transform["y"],
            "thumbnail_scale": thumb_transform["scale"],
            "title": row.get("title") or "",
            "description": row.get("description") or "",
            "size_bytes": int(row.get("size_bytes") or 0),
            "created_at": row.get("created_at") or "",
            "visibility": vis,
            "views_count": int(stats.get("views") or 0),
            "comments_count": int(stats.get("comments") or 0),
            "is_mine": (owner_id == me),
            "can_follow": (owner_id != me),
            "following": following_now,
            "can_like": _like_system_ready(),
            "liked": liked,
            "likes_count": likes,
            "can_collect": _collection_system_ready(),
            "collected": collected,
            "collections_count": collections_count,
            "stats": stats,
            "can_comment": _comments_ready(),
            "hashtags": hashtags,
            "creator_username": creator_username,
            "creator_name": creator_name,
            "creator_public_slug": creator_public_slug,
            "creator_verified": creator_verified,
        }

        try:
            log_event("VIDEO_WATCH", user_id=me, meta={"video_id": vid, "owner_id": owner_id})
        except Exception:
            pass

        return render_template("watch_video.html", active_page="videos", video=v, me=me)

    # ---------------- API: LIKE ----------------
    @app.post("/api/videos/like/<video_id>", endpoint="vm_api_video_like")
    @login_required
    def api_video_like(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        if not _like_system_ready():
            return jsonify(ok=False, error="Likes no disponible"), 400

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return jsonify(ok=False, error="No autorizado"), 403

        try:
            liked_now = bool(toggle_video_like(video_id, me))  # type: ignore
            likes = int(count_video_likes(video_id))  # type: ignore
        except Exception:
            return jsonify(ok=False, error="No se pudo actualizar"), 500

        try:
            log_event("VIDEO_LIKE_TOGGLE", user_id=me, meta={"video_id": video_id, "liked": liked_now})
        except Exception:
            pass

        return jsonify(ok=True, liked=liked_now, likes_count=likes)

    # ---------------- API: COLLECT ----------------
    @app.post("/api/videos/collect/<video_id>", endpoint="vm_api_video_collect")
    @login_required
    def api_video_collect(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        if not _collection_system_ready():
            return jsonify(ok=False, error="Biblioteca no disponible"), 400

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return jsonify(ok=False, error="No autorizado"), 403

        try:
            collected_now = bool(toggle_video_collect(video_id, me))  # type: ignore
            col_count = int(count_video_collections(video_id)) if count_video_collections else 0  # type: ignore
        except Exception:
            return jsonify(ok=False, error="No se pudo actualizar"), 500

        try:
            log_event("VIDEO_COLLECT_TOGGLE", user_id=me, meta={"video_id": video_id, "collected": collected_now})
        except Exception:
            pass

        return jsonify(ok=True, collected=collected_now, collections_count=col_count)
    
    # ---------------- API: UPDATE THUMB TRANSFORM ----------------
    @app.post("/api/videos/thumb-transform/<video_id>", endpoint="vm_api_video_thumb_transform")
    @login_required
    def api_video_thumb_transform(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        if owner_id != me:
            return jsonify(ok=False, error="No autorizado"), 403

        if not _thumb_transform_ready():
            return jsonify(ok=False, error="Editor de miniatura no disponible"), 400

        payload = request.get_json(silent=True) or {}
        pos_x = _norm_thumb_pos_x(payload.get("x", 50.0))
        pos_y = _norm_thumb_pos_y(payload.get("y", 50.0))
        scale = _norm_thumb_scale(payload.get("scale", 1.0))

        try:
            ok = bool(
                update_video_thumbnail_transform(  # type: ignore
                    video_id,
                    thumbnail_pos_x=pos_x,
                    thumbnail_pos_y=pos_y,
                    thumbnail_scale=scale,
                    user_id=me,
                )
            )
        except Exception:
            ok = False

        if not ok:
            return jsonify(ok=False, error="No se pudo actualizar"), 500

        try:
            log_event(
                "VIDEO_THUMB_TRANSFORM_UPDATED",
                user_id=me,
                meta={"video_id": video_id, "x": pos_x, "y": pos_y, "scale": scale},
            )
        except Exception:
            pass

        return jsonify(ok=True, x=pos_x, y=pos_y, scale=scale)

    # ---------------- API: UPLOAD ----------------
    @app.post("/api/videos/upload", endpoint="vm_api_video_upload")
    @login_required
    def api_video_upload():
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        f = request.files.get("video") or request.files.get("video_file")
        thumb = request.files.get("thumbnail") or request.files.get("thumb")

        title = (request.form.get("title") or "").strip()
        description = (request.form.get("description") or "").strip()
        visibility = _norm_visibility(request.form.get("visibility"))

        thumbnail_pos_x = _norm_thumb_pos_x(request.form.get("thumbnail_pos_x", 50))
        thumbnail_pos_y = _norm_thumb_pos_y(request.form.get("thumbnail_pos_y", 50))
        thumbnail_scale = _norm_thumb_scale(request.form.get("thumbnail_scale", 1))

        if not f or not getattr(f, "filename", ""):
            return jsonify(ok=False, error="Archivo requerido"), 400

        ext = Path(f.filename).suffix.lower()
        mime = (getattr(f, "mimetype", "") or "").lower()

        if ext not in _ALLOWED_VIDEO_EXT:
            return jsonify(ok=False, error="Formato no permitido"), 400
        if mime and (mime not in _ALLOWED_VIDEO_MIME):
            return jsonify(ok=False, error="Formato no permitido"), 400

        try:
            f.stream.seek(0, os.SEEK_END)
            size = int(f.stream.tell())
            f.stream.seek(0)
        except Exception:
            size = 0

        if size <= 0:
            return jsonify(ok=False, error="Archivo inválido"), 400

        if size > int(app.config.get("MAX_VIDEO_SIZE", 500 * 1024 * 1024)):
            return jsonify(ok=False, error="Archivo demasiado grande"), 400

        safe_name = _safe_video_filename(f.filename)
        unique_name = f"{me}_{uuid4().hex}_{safe_name}"
        out_path = videos_dir / unique_name

        thumb_unique_name = None
        thumb_path = None

        if thumb and getattr(thumb, "filename", ""):
            thumb_ext = Path(thumb.filename).suffix.lower()
            thumb_mime = (getattr(thumb, "mimetype", "") or "").lower()

            if thumb_ext not in _ALLOWED_THUMB_EXT:
                return jsonify(ok=False, error="Miniatura no permitida"), 400
            if thumb_mime and (thumb_mime not in _ALLOWED_THUMB_MIME):
                return jsonify(ok=False, error="Miniatura no permitida"), 400

            try:
                thumb.stream.seek(0, os.SEEK_END)
                thumb_size = int(thumb.stream.tell())
                thumb.stream.seek(0)
            except Exception:
                thumb_size = 0

            if thumb_size <= 0:
                return jsonify(ok=False, error="Miniatura inválida"), 400

            if thumb_size > int(app.config.get("MAX_THUMB_SIZE", 8 * 1024 * 1024)):
                return jsonify(ok=False, error="Miniatura demasiado grande"), 400

            safe_thumb = _safe_thumb_filename(thumb.filename)
            thumb_unique_name = f"{me}_{uuid4().hex}_{safe_thumb}"
            thumb_path = thumbs_dir / thumb_unique_name

        try:
            f.save(str(out_path))
            if thumb and thumb_unique_name and thumb_path:
                thumb.save(str(thumb_path))
        except Exception:
            try:
                if out_path.exists():
                    out_path.unlink()
            except Exception:
                pass
            try:
                if thumb_path and thumb_path.exists():
                    thumb_path.unlink()
            except Exception:
                pass
            return jsonify(ok=False, error="No se pudo guardar"), 500

        if not _file_is_inside_dir(out_path, videos_dir):
            try:
                if out_path.exists():
                    out_path.unlink()
            except Exception:
                pass
            try:
                if thumb_path and thumb_path.exists():
                    thumb_path.unlink()
            except Exception:
                pass
            return jsonify(ok=False, error="Ruta inválida"), 400

        if thumb_path and not _file_is_inside_dir(thumb_path, thumbs_dir):
            try:
                if out_path.exists():
                    out_path.unlink()
            except Exception:
                pass
            try:
                if thumb_path.exists():
                    thumb_path.unlink()
            except Exception:
                pass
            return jsonify(ok=False, error="Ruta inválida"), 400

        try:
            vid = _create_video_record(
                user_id=me,
                filename=unique_name,
                thumbnail_filename=thumb_unique_name,
                title=title,
                description=description,
                size_bytes=size,
                visibility=visibility,
                thumbnail_pos_x=thumbnail_pos_x,
                thumbnail_pos_y=thumbnail_pos_y,
                thumbnail_scale=thumbnail_scale,
            )
        except Exception:
            try:
                if out_path.exists():
                    out_path.unlink()
            except Exception:
                pass
            try:
                if thumb_path and thumb_path.exists():
                    thumb_path.unlink()
            except Exception:
                pass
            return jsonify(ok=False, error="DB error (no se pudo registrar)"), 500

        if vid:
            _award_once(
                receiver_id=me,
                action="video_uploaded",
                points=PTS_UPLOAD,
                ref_type="video",
                ref_id=str(vid),
                meta={
                    "video_id": vid,
                    "size": size,
                    "visibility": visibility,
                    "thumbnail": bool(thumb_unique_name),
                },
            )

        try:
            log_event(
                "VIDEO_UPLOADED",
                user_id=me,
                meta={
                    "video_id": vid,
                    "file": unique_name,
                    "thumb": thumb_unique_name,
                    "size": size,
                    "thumb_x": thumbnail_pos_x,
                    "thumb_y": thumbnail_pos_y,
                    "thumb_scale": thumbnail_scale,
                },
            )
        except Exception:
            pass

        return jsonify(
            ok=True,
            video_id=vid,
            filename=unique_name,
            thumbnail_filename=thumb_unique_name or "",
            thumbnail_pos_x=thumbnail_pos_x,
            thumbnail_pos_y=thumbnail_pos_y,
            thumbnail_scale=thumbnail_scale,
        )

    # ---------------- STREAM ----------------
    @app.get("/videos/stream/<video_id>", endpoint="vm_stream_video")
    @login_required
    def stream_video(video_id: str):
        me = uid()
        if not me:
            return "Not authorized", 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return "Not found", 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return "Not authorized", 403

        filename = _sanitize_stored_filename(row.get("filename") or "")
        if not filename:
            return "Not found", 404

        if not _is_owner_filename(filename, owner_id):
            return "Not authorized", 403

        file_path = videos_dir / filename
        if not _file_is_inside_dir(file_path, videos_dir):
            return "Not found", 404
        if not file_path.exists():
            return "Not found", 404

        return send_file(
            file_path,
            as_attachment=False,
            conditional=True,
            max_age=0,
            etag=True,
            last_modified=file_path.stat().st_mtime,
        )

    # ---------------- THUMB STREAM ----------------
    @app.get("/videos/thumb/<video_id>", endpoint="vm_video_thumb")
    @login_required
    def video_thumb(video_id: str):
        me = uid()
        if not me:
            return "Not authorized", 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return "Not found", 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return "Not authorized", 403

        filename = _sanitize_stored_filename(row.get("thumbnail_filename") or "")
        if not filename:
            return "Not found", 404

        if not _is_owner_filename(filename, owner_id):
            return "Not authorized", 403

        file_path = thumbs_dir / filename
        if not _file_is_inside_dir(file_path, thumbs_dir):
            return "Not found", 404
        if not file_path.exists():
            return "Not found", 404

        return send_file(
            file_path,
            as_attachment=False,
            conditional=True,
            max_age=0,
            etag=True,
            last_modified=file_path.stat().st_mtime,
        )

    # ---------------- DELETE ----------------
    @app.post("/videos/delete/<video_id>", endpoint="vm_delete_video")
    @login_required
    def delete_video(video_id: str):
        me = uid()
        if not me:
            return redirect(url_for("vm_videos_feed"))
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return redirect(url_for("vm_videos_feed"))

        if int(row.get("user_id") or 0) != me:
            return redirect(url_for("vm_videos_feed"))

        filename = _sanitize_stored_filename(row.get("filename") or "")
        thumb_filename = _sanitize_stored_filename(row.get("thumbnail_filename") or "")

        file_path = videos_dir / filename
        thumb_path = thumbs_dir / thumb_filename if thumb_filename else None

        try:
            if filename and file_path.exists() and _file_is_inside_dir(file_path, videos_dir):
                file_path.unlink()
        except Exception:
            pass

        try:
            if thumb_filename and thumb_path and thumb_path.exists() and _file_is_inside_dir(thumb_path, thumbs_dir):
                thumb_path.unlink()
        except Exception:
            pass

        try:
            db_delete_video(video_id, user_id=me)
        except Exception:
            pass

        try:
            log_event("VIDEO_DELETED", user_id=me, meta={"video_id": video_id})
        except Exception:
            pass

        return redirect(url_for("vm_videos_feed"))

    # ---------------- VIEW API ----------------
    @app.post("/api/videos/view/<video_id>", endpoint="vm_api_video_view")
    @login_required
    def api_video_view(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return jsonify(ok=False, error="No autorizado"), 403

        if _views_ready():
            try:
                register_video_view(  # type: ignore
                    video_id,
                    user_id=me,
                    session_id=_get_session_id(),
                    ip=_client_ip(),
                )
            except Exception:
                pass

        return jsonify(ok=True, stats=_watch_counts(video_id))

    # ---------------- COMMENTS ----------------
    @app.post("/api/videos/comment/<video_id>", endpoint="vm_api_video_comment")
    @login_required
    def api_video_comment(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return jsonify(ok=False, error="No autorizado"), 403

        if not _comments_ready():
            return jsonify(ok=False, error="Comentarios no disponibles"), 400

        payload = request.get_json(silent=True) or {}
        text = (payload.get("text") or "").strip()
        parent_id = payload.get("parent_id", None)

        if not text:
            return jsonify(ok=False, error="Comentario vacío"), 400
        if len(text) > 500:
            text = text[:500]

        try:
            cid = int(add_video_comment(video_id, me, text, parent_id=parent_id) or 0)  # type: ignore
            if cid <= 0:
                return jsonify(ok=False, error="No se pudo guardar"), 500
        except Exception:
            return jsonify(ok=False, error="No se pudo guardar"), 500

        try:
            log_event("VIDEO_COMMENT_CREATED", user_id=me, meta={"video_id": video_id, "comment_id": cid})
        except Exception:
            pass

        return jsonify(ok=True, comment_id=cid, stats=_watch_counts(video_id))

    @app.get("/api/videos/comments/<video_id>", endpoint="vm_api_video_comments")
    @login_required
    def api_video_comments(video_id: str):
        me = uid()
        if not me:
            return jsonify(ok=False, error="No auth"), 401
        me = int(me)

        row = _safe_get_video(video_id)
        if not row:
            return jsonify(ok=False, error="No existe"), 404

        owner_id = int(row.get("user_id") or 0)
        vis = _norm_visibility(row.get("visibility"))
        if not _can_view_video(me, owner_id, vis):
            return jsonify(ok=False, error="No autorizado"), 403

        if not _comments_ready():
            return jsonify(ok=True, comments=[], stats=_watch_counts(video_id))

        try:
            limit = int(request.args.get("limit", 30))
        except Exception:
            limit = 30
        try:
            offset = int(request.args.get("offset", 0))
        except Exception:
            offset = 0

        limit = max(1, min(limit, 100))
        offset = max(0, offset)

        try:
            rows = list_video_comments(video_id, limit=limit, offset=offset) or []  # type: ignore
        except Exception:
            rows = []

        return jsonify(ok=True, comments=rows, stats=_watch_counts(video_id))

    # ---------------- OPTIONAL STUBS ----------------
    @app.post("/api/videos/share/<video_id>", endpoint="vm_api_video_share")
    @login_required
    def api_video_share(video_id: str):
        return jsonify(ok=False, error="Share no disponible aún", stats=_watch_counts(video_id)), 400

    @app.post("/api/videos/download/<video_id>", endpoint="vm_api_video_download")
    @login_required
    def api_video_download(video_id: str):
        return jsonify(ok=False, error="Download no disponible aún", stats=_watch_counts(video_id)), 400

    # ---------------- ALIASES ----------------
    try:
        if "videos_feed" not in app.view_functions:
            app.add_url_rule("/videos", endpoint="videos_feed", view_func=videos_feed)
        if "videos_library" not in app.view_functions:
            app.add_url_rule("/videos/library", endpoint="videos_library", view_func=videos_library)
        if "watch_video" not in app.view_functions:
            app.add_url_rule("/videos/watch/<video_id>", endpoint="watch_video", view_func=watch_video)
        if "delete_video" not in app.view_functions:
            app.add_url_rule("/videos/delete/<video_id>", endpoint="delete_video", view_func=delete_video)
        if "stream_video" not in app.view_functions:
            app.add_url_rule("/videos/stream/<video_id>", endpoint="stream_video", view_func=stream_video)
        if "video_thumb" not in app.view_functions:
            app.add_url_rule("/videos/thumb/<video_id>", endpoint="video_thumb", view_func=video_thumb)
        if "upload_video_page" not in app.view_functions:
            app.add_url_rule("/videos/upload", endpoint="upload_video_page", view_func=upload_video_page)
        if "api_video_upload" not in app.view_functions:
            app.add_url_rule(
                "/api/videos/upload",
                endpoint="api_video_upload",
                view_func=api_video_upload,
                methods=["POST"],
            )
        if "api_video_suggest" not in app.view_functions:
            app.add_url_rule(
                "/api/videos/suggest",
                endpoint="api_video_suggest",
                view_func=api_video_suggest,
                methods=["GET"],
            )
        if "api_video_thumb_transform" not in app.view_functions:
            app.add_url_rule(
                "/api/videos/thumb-transform/<video_id>",
                endpoint="api_video_thumb_transform",
                view_func=api_video_thumb_transform,
                methods=["POST"],
            )
    except Exception:
        pass

    return True