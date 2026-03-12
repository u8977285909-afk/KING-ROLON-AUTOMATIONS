# -*- coding: utf-8 -*-
"""
database.py — KING ROLON AUTOMATIONS
MODO DIOS • ESTABLE • PROFESIONAL • COMPATIBLE
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

# =========================================================
# PROJECT PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent


# =========================================================
# DB PATH (AUTO-RECOVERY)
# =========================================================
def _pick_db_path() -> Path:
    env_path = (os.getenv("KR_DB_PATH") or "").strip()
    if env_path:
        p = Path(env_path).expanduser()
        if not p.is_absolute():
            p = (PROJECT_DIR / p).resolve()
        return p

    candidates = [
        PROJECT_DIR / "data" / "kr_users.db",
        PROJECT_DIR / "kr_users.db",
        PROJECT_DIR / "data" / "users.db",
        PROJECT_DIR / "users.db",
        PROJECT_DIR / "data" / "king_rolon.db",
        PROJECT_DIR / "king_rolon.db",
    ]

    existing = [p for p in candidates if p.exists() and p.is_file()]
    if existing:
        existing.sort(key=lambda x: (x.stat().st_size, x.stat().st_mtime), reverse=True)
        return existing[0]

    return PROJECT_DIR / "data" / "kr_users.db"


DB_PATH: Path = _pick_db_path()


# =========================================================
# CONNECTION (HARDENED)
# =========================================================
def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass

    return conn


def get_db_path() -> str:
    return str(DB_PATH)


# =========================================================
# MIGRATION HELPERS
# =========================================================
def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        r = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
            (table,),
        ).fetchone()
        return bool(r)
    except Exception:
        return False


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        return [r[1] for r in conn.execute(f"PRAGMA table_info({table});").fetchall()]
    except Exception:
        return []


def _add_column_if_missing(conn: sqlite3.Connection, table: str, col: str, ddl: str) -> None:
    if not _table_exists(conn, table):
        return
    if col in _table_columns(conn, table):
        return
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl};")
    except Exception:
        pass


def _slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"[^a-z0-9\-]", "", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or "app"


def _slugify_public(text: str) -> str:
    s = _slugify(text)
    return s if s != "app" else "user"


def _to_int(v: Union[str, int, None]) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _to_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _norm_thumb_pos_x(v: Any) -> float:
    return _clamp(_to_float(v, 50.0), -5000.0, 5000.0)


def _norm_thumb_pos_y(v: Any) -> float:
    return _clamp(_to_float(v, 50.0), -5000.0, 5000.0)


def _norm_thumb_scale(v: Any) -> float:
    return _clamp(_to_float(v, 1.0), 0.1, 10.0)


def _norm_banner_pos_x(v: Any) -> float:
    return _clamp(_to_float(v, 50.0), -5000.0, 5000.0)


def _norm_banner_pos_y(v: Any) -> float:
    return _clamp(_to_float(v, 50.0), -5000.0, 5000.0)


def _norm_banner_scale(v: Any) -> float:
    return _clamp(_to_float(v, 1.0), 0.1, 10.0)


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _norm_visibility(v: Optional[str]) -> str:
    vv = (v or "").strip().lower()
    return vv if vv in ("public", "contacts", "private") else "public"


def _norm_role_for_core(role: Optional[str]) -> str:
    r = (role or "").strip().lower()
    if r == "assistant":
        return "assistant"
    if r == "system":
        return "system"
    if r in ("ai", "bot"):
        return "assistant"
    return "user"


# =========================================================
# SEARCH HELPERS
# =========================================================
def _normalize_search_text(text: Any) -> str:
    s = str(text or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_search_area(area: Optional[str]) -> str:
    a = (area or "").strip().lower()
    return a if a in ("videos", "library", "creators", "hashtags", "global") else "videos"


def _norm_search_kind(kind: Optional[str]) -> str:
    k = (kind or "").strip().lower()
    return k if k in ("general", "creator", "hashtag", "semantic") else "general"


def _extract_hashtags(text: Any) -> List[str]:
    s = str(text or "").strip().lower()
    if not s:
        return []
    tags = re.findall(
        r"(?:^|[\s\.,;:!\?\(\)\[\]\{\}/\\])#([a-zA-Z0-9_áéíóúüñÁÉÍÓÚÜÑ]+)",
        f" {s}",
    )
    out: List[str] = []
    seen = set()
    for tag in tags:
        norm = _normalize_search_text(tag)
        norm = re.sub(r"[^a-z0-9_]", "", norm)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm[:50])
    return out


def _search_terms_from_text(*values: Any) -> List[str]:
    bag = " ".join(str(v or "") for v in values).strip()
    norm = _normalize_search_text(bag)
    if not norm:
        return []
    tokens = [t for t in re.split(r"[^a-z0-9_#@]+", norm) if t]
    seen = set()
    out: List[str] = []
    for tok in tokens:
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


# =========================================================
# PROFILE HELPERS (PUBLIC PROFILE)
# =========================================================
_USERNAME_RE = re.compile(r"^[a-z0-9_]{3,20}$")


def _norm_username(username: Optional[str]) -> str:
    u = (username or "").strip().lower()
    u = u.replace("-", "_")
    u = re.sub(r"[^a-z0-9_]", "", u)
    u = re.sub(r"_{2,}", "_", u).strip("_")
    return u


def _is_valid_username(username: str) -> bool:
    return bool(_USERNAME_RE.match(username or ""))


def _suggest_username_from_email_or_name(email: str, name: str) -> str:
    base = _norm_username(name) or _norm_username((email or "").split("@")[0])
    base = base[:20]
    if len(base) < 3:
        base = f"user{uuid4().hex[:6]}"
    return base


def _generate_unique_username(conn: sqlite3.Connection, base: str) -> str:
    base = _norm_username(base)
    if not _is_valid_username(base):
        base = f"user{uuid4().hex[:6]}"

    def exists(u: str) -> bool:
        try:
            r = conn.execute(
                "SELECT 1 FROM users WHERE username = ? LIMIT 1;",
                (u,),
            ).fetchone()
            return bool(r)
        except Exception:
            return False

    if not exists(base):
        return base

    for i in range(2, 9999):
        u = f"{base[: max(3, 20 - (len(str(i)) + 1))]}_{i}"
        u = u[:20]
        if _is_valid_username(u) and not exists(u):
            return u

    return f"user{uuid4().hex[:12]}"[:20]


# =========================================================
# PUBLIC SLUG (PROFILE URL)
# =========================================================
def _generate_unique_public_slug(
    conn: sqlite3.Connection,
    base: str,
    exclude_user_id: Optional[int] = None,
) -> str:
    base_slug = _slugify_public(base)
    slug = base_slug

    def exists(s: str) -> bool:
        try:
            if exclude_user_id is None:
                r = conn.execute(
                    "SELECT 1 FROM users WHERE public_slug = ? LIMIT 1;",
                    (s,),
                ).fetchone()
            else:
                r = conn.execute(
                    "SELECT 1 FROM users WHERE public_slug = ? AND id != ? LIMIT 1;",
                    (s, int(exclude_user_id)),
                ).fetchone()
            return bool(r)
        except Exception:
            return False

    if not exists(slug):
        return slug

    for i in range(2, 9999):
        s = f"{base_slug}-{i}"
        if not exists(s):
            return s

    return f"{base_slug}-{uuid4().hex[:8]}"


# =========================================================
# VIDEO VIEWS (PRO) — HELPERS
# =========================================================
def _ip_hash(ip: Optional[str]) -> str:
    raw = (ip or "").strip()
    if not raw:
        return ""
    salt = (os.getenv("KR_IP_SALT") or "kr").strip()
    return hashlib.sha256((salt + "|" + raw).encode("utf-8")).hexdigest()


# =========================================================
# POINTS (INTERNAL) — ATÓMICO, SIN NUEVA CONEXIÓN
# =========================================================
def _award_points_conn(
    conn: sqlite3.Connection,
    user_id: int,
    action: str,
    points: int,
    ref_type: str = "",
    ref_id: str = "",
    meta: Optional[Any] = None,
) -> None:
    uid = _to_int(user_id)
    if uid is None:
        return

    action = (action or "").strip().lower()
    if not action:
        return

    pts = int(points or 0)
    if pts == 0:
        return

    meta_str = None
    if meta is not None:
        try:
            meta_str = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            meta_str = str(meta)

    conn.execute(
        """
        INSERT INTO points_ledger (user_id, action, points, ref_type, ref_id, meta)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            uid,
            action,
            pts,
            (ref_type or "").strip(),
            (ref_id or "").strip(),
            meta_str,
        ),
    )

    conn.execute(
        """
        INSERT INTO user_points (user_id, points)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
          points = user_points.points + excluded.points,
          updated_at = datetime('now');
        """,
        (uid, pts),
    )


# =========================================================
# INIT DB + MIGRATIONS
# =========================================================
def init_db() -> None:
    with get_connection() as conn:
        c = conn.cursor()

        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            name TEXT,
            role TEXT NOT NULL DEFAULT 'FREE',
            language TEXT DEFAULT 'es',
            avatar_url TEXT,

            username TEXT,
            bio TEXT,
            banner_url TEXT,
            website TEXT,
            verified INTEGER NOT NULL DEFAULT 0,

            is_public INTEGER NOT NULL DEFAULT 0,
            public_slug TEXT,

            banner_pos_x REAL NOT NULL DEFAULT 50,
            banner_pos_y REAL NOT NULL DEFAULT 50,
            banner_scale REAL NOT NULL DEFAULT 1,

            updated_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            event_type TEXT NOT NULL,
            user_id INTEGER,
            platform TEXT,
            meta TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            description TEXT,
            platform TEXT,
            level TEXT,
            price_cents INTEGER NOT NULL DEFAULT 0,
            default_frequency TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            installed_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id, product_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS apps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            description TEXT,
            owner_id INTEGER NOT NULL,
            platform TEXT,
            category TEXT,
            level TEXT,
            price_cents INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'draft',
            icon_path TEXT,
            file_path TEXT,
            downloads INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT,
            FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS app_sales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            app_id INTEGER NOT NULL,
            buyer_id INTEGER NOT NULL,
            price_cents INTEGER NOT NULL,
            kr_fee_cents INTEGER NOT NULL,
            creator_amount_cents INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (app_id) REFERENCES apps(id) ON DELETE CASCADE,
            FOREIGN KEY (buyer_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            thumbnail_filename TEXT,
            thumbnail_pos_x REAL NOT NULL DEFAULT 50,
            thumbnail_pos_y REAL NOT NULL DEFAULT 50,
            thumbnail_scale REAL NOT NULL DEFAULT 1,
            title TEXT NOT NULL DEFAULT '',
            description TEXT,
            size_bytes INTEGER NOT NULL DEFAULT 0,
            visibility TEXT NOT NULL DEFAULT 'public',
            views_count INTEGER NOT NULL DEFAULT 0,
            shares_count INTEGER NOT NULL DEFAULT 0,
            downloads_count INTEGER NOT NULL DEFAULT 0,
            comments_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_hashtags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            hashtag TEXT NOT NULL,
            normalized_hashtag TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(video_id, normalized_hashtag),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER,
            session_id TEXT,
            ip_hash TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_shares (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER,
            text TEXT NOT NULL,
            parent_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS followers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            follower_id INTEGER NOT NULL,
            followed_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(follower_id, followed_id),
            FOREIGN KEY (follower_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (followed_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_likes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(video_id, user_id),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            reaction TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(video_id, user_id),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS comment_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comment_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            reaction TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(comment_id, user_id),
            FOREIGN KEY (comment_id) REFERENCES video_comments(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS video_collections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(video_id, user_id),
            FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS user_points (
            user_id INTEGER PRIMARY KEY,
            points INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS points_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            points INTEGER NOT NULL,
            ref_type TEXT,
            ref_id TEXT,
            meta TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS search_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            area TEXT NOT NULL DEFAULT 'videos',
            kind TEXT NOT NULL DEFAULT 'general',
            query TEXT NOT NULL,
            normalized_query TEXT NOT NULL,
            hits INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_used_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id, area, kind, normalized_query),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS eye_memory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            importance INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_used_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS eye_memory_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            importance INTEGER NOT NULL DEFAULT 1,
            created_at TEXT,
            last_used_at TEXT,
            archived_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """)

        _add_column_if_missing(conn, "users", "language", "TEXT DEFAULT 'es'")
        _add_column_if_missing(conn, "users", "avatar_url", "TEXT")
        _add_column_if_missing(conn, "users", "username", "TEXT")
        _add_column_if_missing(conn, "users", "bio", "TEXT")
        _add_column_if_missing(conn, "users", "banner_url", "TEXT")
        _add_column_if_missing(conn, "users", "website", "TEXT")
        _add_column_if_missing(conn, "users", "verified", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "users", "updated_at", "TEXT")
        _add_column_if_missing(conn, "users", "is_public", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "users", "public_slug", "TEXT")
        _add_column_if_missing(conn, "users", "banner_pos_x", "REAL NOT NULL DEFAULT 50")
        _add_column_if_missing(conn, "users", "banner_pos_y", "REAL NOT NULL DEFAULT 50")
        _add_column_if_missing(conn, "users", "banner_scale", "REAL NOT NULL DEFAULT 1")

        _add_column_if_missing(conn, "apps", "icon_path", "TEXT")
        _add_column_if_missing(conn, "apps", "file_path", "TEXT")
        _add_column_if_missing(conn, "apps", "updated_at", "TEXT")
        _add_column_if_missing(conn, "apps", "downloads", "INTEGER NOT NULL DEFAULT 0")

        _add_column_if_missing(conn, "products", "default_frequency", "TEXT")
        _add_column_if_missing(conn, "products", "is_active", "INTEGER NOT NULL DEFAULT 1")

        _add_column_if_missing(conn, "videos", "title", "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(conn, "videos", "description", "TEXT")
        _add_column_if_missing(conn, "videos", "size_bytes", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "videos", "thumbnail_filename", "TEXT")
        _add_column_if_missing(conn, "videos", "thumbnail_pos_x", "REAL NOT NULL DEFAULT 50")
        _add_column_if_missing(conn, "videos", "thumbnail_pos_y", "REAL NOT NULL DEFAULT 50")
        _add_column_if_missing(conn, "videos", "thumbnail_scale", "REAL NOT NULL DEFAULT 1")
        _add_column_if_missing(conn, "videos", "visibility", "TEXT NOT NULL DEFAULT 'public'")
        _add_column_if_missing(conn, "videos", "views_count", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "videos", "shares_count", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "videos", "downloads_count", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "videos", "comments_count", "INTEGER NOT NULL DEFAULT 0")

        if _table_exists(conn, "search_queries"):
            _add_column_if_missing(conn, "search_queries", "kind", "TEXT NOT NULL DEFAULT 'general'")

        for _tbl, _ddl in (
            ("video_hashtags", """
                CREATE TABLE IF NOT EXISTS video_hashtags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    hashtag TEXT NOT NULL,
                    normalized_hashtag TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(video_id, normalized_hashtag),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE
                );
            """),
            ("video_views", """
                CREATE TABLE IF NOT EXISTS video_views (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    user_id INTEGER,
                    session_id TEXT,
                    ip_hash TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("video_shares", """
                CREATE TABLE IF NOT EXISTS video_shares (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    user_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("video_downloads", """
                CREATE TABLE IF NOT EXISTS video_downloads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    user_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("video_comments", """
                CREATE TABLE IF NOT EXISTS video_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    user_id INTEGER,
                    text TEXT NOT NULL,
                    parent_id INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("video_reactions", """
                CREATE TABLE IF NOT EXISTS video_reactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    reaction TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(video_id, user_id),
                    FOREIGN KEY (video_id) REFERENCES videos(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("comment_reactions", """
                CREATE TABLE IF NOT EXISTS comment_reactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comment_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    reaction TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(comment_id, user_id),
                    FOREIGN KEY (comment_id) REFERENCES video_comments(id) ON DELETE CASCADE,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
            ("search_queries", """
                CREATE TABLE IF NOT EXISTS search_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    area TEXT NOT NULL DEFAULT 'videos',
                    kind TEXT NOT NULL DEFAULT 'general',
                    query TEXT NOT NULL,
                    normalized_query TEXT NOT NULL,
                    hits INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_used_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(user_id, area, kind, normalized_query),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """),
        ):
            try:
                conn.execute(f"SELECT 1 FROM {_tbl} LIMIT 1;")
            except Exception:
                try:
                    conn.execute(_ddl)
                except Exception:
                    pass

        try:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_created ON events(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_products_active_created ON products(is_active, created_at);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_purchases_unique ON purchases(user_id, product_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_apps_owner_created ON apps(owner_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_apps_status_created ON apps(status, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_eye_memory_user_last_used ON eye_memory(user_id, last_used_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_user_created ON videos(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_visibility_created ON videos(visibility, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_title_created ON videos(title, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_thumb_filename ON videos(thumbnail_filename);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_views_count ON videos(views_count);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_shares_count ON videos(shares_count);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_downloads_count ON videos(downloads_count);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_comments_count ON videos(comments_count);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_hashtags_tag ON video_hashtags(normalized_hashtag);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_hashtags_video ON video_hashtags(video_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_views_video_created ON video_views(video_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_views_user_created ON video_views(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_views_session_created ON video_views(session_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_views_ip_created ON video_views(ip_hash, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_shares_video_created ON video_shares(video_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_downloads_video_created ON video_downloads(video_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_comments_video_created ON video_comments(video_id, created_at);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_follow_unique ON followers(follower_id, followed_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_followed_id ON followers(followed_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_follower_id ON followers(follower_id);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_video_like_unique ON video_likes(video_id, user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_like_video ON video_likes(video_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_like_user ON video_likes(user_id);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_video_reactions_unique ON video_reactions(video_id, user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_reactions_video_created ON video_reactions(video_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_reactions_user_created ON video_reactions(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_reactions_reaction ON video_reactions(reaction);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_comment_reactions_unique ON comment_reactions(comment_id, user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_reactions_comment_created ON comment_reactions(comment_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_reactions_user_created ON comment_reactions(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_comment_reactions_reaction ON comment_reactions(reaction);")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_video_collection_unique ON video_collections(video_id, user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_collection_video ON video_collections(video_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_video_collection_user ON video_collections(user_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_points_ledger_user_created ON points_ledger(user_id, created_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_points_ledger_action ON points_ledger(action);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_points_updated ON user_points(updated_at);")
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_unique
                ON users(username)
                WHERE username IS NOT NULL AND username != '';
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_public_name ON users(is_public, username, name);")
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_users_public_slug_unique
                ON users(public_slug)
                WHERE public_slug IS NOT NULL AND public_slug != '';
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_is_public ON users(is_public);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_banner_pos_y ON users(banner_pos_y);")
            try:
                conn.execute("DROP INDEX IF EXISTS idx_search_queries_unique;")
            except Exception:
                pass
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_search_queries_unique2 ON search_queries(user_id, area, kind, normalized_query);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search_queries_user_area_last ON search_queries(user_id, area, last_used_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search_queries_area_hits ON search_queries(area, hits);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search_queries_area_last ON search_queries(area, last_used_at);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search_queries_norm ON search_queries(normalized_query);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_search_queries_kind ON search_queries(kind);")
        except Exception:
            pass

        try:
            rows = conn.execute("""
                SELECT id, email, COALESCE(name,'') AS name
                FROM users
                WHERE username IS NULL OR username = '';
            """).fetchall()
            for r in rows:
                uid_ = int(r["id"])
                base = _suggest_username_from_email_or_name(r["email"], r["name"])
                unique = _generate_unique_username(conn, base)
                conn.execute(
                    "UPDATE users SET username=?, updated_at=datetime('now') WHERE id=?;",
                    (unique, uid_),
                )
        except Exception:
            pass

        try:
            rows = conn.execute("""
                SELECT id, COALESCE(username,'') AS username, COALESCE(name,'') AS name, COALESCE(email,'') AS email
                FROM users
                WHERE public_slug IS NULL OR public_slug = '';
            """).fetchall()

            for r in rows:
                uid_ = int(r["id"])
                username = (r["username"] or "").strip()
                name = (r["name"] or "").strip()
                email = (r["email"] or "").strip()

                base = username or (name or (email.split("@")[0] if email else "user"))
                slug = _generate_unique_public_slug(conn, base=base, exclude_user_id=uid_)
                conn.execute(
                    "UPDATE users SET public_slug=?, updated_at=datetime('now') WHERE id=?;",
                    (slug, uid_),
                )
        except Exception:
            pass

        try:
            rows = conn.execute("""
                SELECT id, COALESCE(title,'') AS title, COALESCE(description,'') AS description
                FROM videos;
            """).fetchall()

            for r in rows:
                vid = str(r["id"] or "").strip()
                if not vid:
                    continue
                tags = _extract_hashtags(f"{r['title']} {r['description']}")
                if not tags:
                    continue
                for tag in tags:
                    try:
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO video_hashtags (video_id, hashtag, normalized_hashtag)
                            VALUES (?, ?, ?);
                            """,
                            (vid, f"#{tag}", tag),
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            conn.commit()
        except Exception:
            pass

    ensure_default_products()


# =========================================================
# EVENTS
# =========================================================
def log_event_db(
    event_type: str,
    user_id: Optional[int] = None,
    platform: Optional[str] = None,
    meta: Optional[Any] = None,
) -> None:
    event_type = (event_type or "").strip()
    if not event_type:
        return

    meta_str = None
    if meta is not None:
        try:
            meta_str = json.dumps(meta, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            meta_str = str(meta)

    with get_connection() as conn:
        conn.execute(
            "INSERT INTO events (event_type, user_id, platform, meta) VALUES (?, ?, ?, ?);",
            (event_type, _to_int(user_id), platform, meta_str),
        )
        try:
            conn.commit()
        except Exception:
            pass


# =========================================================
# STORE
# =========================================================
def ensure_default_products() -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM products;")
        if int(cur.fetchone()["c"]) > 0:
            return

        cur.executemany(
            """
            INSERT INTO products
            (slug, title, description, platform, level, price_cents, default_frequency, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1);
            """,
            [
                ("tiktok-daily-clips", "TikTok: Clip diario", "Clip automático diario", "TikTok", "Básico", 0, "Cada 24h"),
                ("twitch-to-tiktok-highlights", "De Twitch a TikTok", "Highlights automáticos", "TikTok", "Intermedio", 0, "Después de cada stream"),
                ("global-auto-dms", "Auto DMs", "Respuestas automáticas", "Global", "Básico", 0, "Cada 30 min"),
            ],
        )
        try:
            conn.commit()
        except Exception:
            pass


def get_products(active_only: bool = True) -> List[Dict[str, Any]]:
    with get_connection() as conn:
        q = (
            "SELECT * FROM products WHERE is_active = 1 ORDER BY created_at DESC;"
            if active_only
            else "SELECT * FROM products ORDER BY created_at DESC;"
        )
        return [dict(r) for r in conn.execute(q).fetchall()]


def get_product_by_id(product_id: int) -> Optional[Dict[str, Any]]:
    pid = _to_int(product_id)
    if pid is None:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM products WHERE id = ?;", (pid,)).fetchone()
    return dict(r) if r else None


def get_user_purchased_product_ids(user_id: int) -> List[int]:
    uid = _to_int(user_id)
    if uid is None:
        return []
    with get_connection() as conn:
        rows = conn.execute("SELECT product_id FROM purchases WHERE user_id = ?;", (uid,)).fetchall()
    return [int(r["product_id"]) for r in rows]


def add_purchase(user_id: int, product_id: int) -> None:
    uid = _to_int(user_id)
    pid = _to_int(product_id)
    if uid is None or pid is None:
        return
    with get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO purchases (user_id, product_id) VALUES (?, ?);",
            (uid, pid),
        )
        try:
            conn.commit()
        except Exception:
            pass


# =========================================================
# MARKETPLACE
# =========================================================
def _generate_unique_slug(conn: sqlite3.Connection, base_slug: str) -> str:
    base_slug = _slugify(base_slug)
    slug = base_slug

    try:
        r = conn.execute("SELECT 1 FROM apps WHERE slug=? LIMIT 1;", (slug,)).fetchone()
        if not r:
            return slug
    except Exception:
        return slug

    for i in range(2, 9999):
        slug_try = f"{base_slug}-{i}"
        r = conn.execute("SELECT 1 FROM apps WHERE slug=? LIMIT 1;", (slug_try,)).fetchone()
        if not r:
            return slug_try

    return f"{base_slug}-{uuid4().hex[:8]}"


def create_app(
    slug: str,
    title: str,
    owner_id: int,
    description: str = "",
    platform: Optional[str] = None,
    category: Optional[str] = None,
    level: Optional[str] = None,
    price_cents: int = 0,
    status: str = "draft",
    icon_path: Optional[str] = None,
    file_path: Optional[str] = None,
) -> int:
    owner = _to_int(owner_id)
    if owner is None:
        return 0

    title = (title or "").strip() or "App"
    description = (description or "").strip()
    status = (status or "draft").strip().lower()
    if status not in ("draft", "published"):
        status = "draft"

    with get_connection() as conn:
        cur = conn.cursor()
        safe_slug = _generate_unique_slug(conn, slug or title)

        cur.execute(
            """
            INSERT INTO apps (
                slug, title, description, owner_id,
                platform, category, level,
                price_cents, status,
                icon_path, file_path, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'));
            """,
            (
                safe_slug,
                title,
                description,
                owner,
                platform,
                category,
                level,
                max(0, int(price_cents)),
                status,
                icon_path,
                file_path,
            ),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return int(cur.lastrowid)


def get_app_by_id(app_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    aid = _to_int(app_id)
    if aid is None:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM apps WHERE id = ?;", (aid,)).fetchone()
    return dict(r) if r else None


def get_app_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    s = _slugify(slug)
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM apps WHERE slug = ?;", (s,)).fetchone()
    return dict(r) if r else None


def list_apps() -> List[Dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM apps ORDER BY created_at DESC;").fetchall()
    return [dict(r) for r in rows]


def list_apps_by_owner(owner_id: int) -> List[Dict[str, Any]]:
    oid = _to_int(owner_id)
    if oid is None:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM apps WHERE owner_id = ? ORDER BY created_at DESC;",
            (oid,),
        ).fetchall()
    return [dict(r) for r in rows]


def update_app_status(app_id: Union[int, str], status: str) -> None:
    aid = _to_int(app_id)
    if aid is None:
        return
    status = (status or "").strip().lower()
    if status not in ("published", "draft"):
        status = "draft"
    with get_connection() as conn:
        conn.execute(
            "UPDATE apps SET status = ?, updated_at = datetime('now') WHERE id = ?;",
            (status, aid),
        )
        try:
            conn.commit()
        except Exception:
            pass


def record_app_sale(
    app_id: Union[int, None],
    buyer_id: Union[int, None],
    price_cents: int = 0,
    kr_fee_cents: Optional[int] = None,
    creator_amount_cents: Optional[int] = None,
) -> None:
    aid = _to_int(app_id)
    bid = _to_int(buyer_id)
    if aid is None or bid is None:
        return

    price_cents = max(0, int(price_cents))
    if kr_fee_cents is None:
        kr_fee_cents = int(round(price_cents * 0.20))
    if creator_amount_cents is None:
        creator_amount_cents = price_cents - int(kr_fee_cents)

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_sales
            (app_id, buyer_id, price_cents, kr_fee_cents, creator_amount_cents)
            VALUES (?, ?, ?, ?, ?);
            """,
            (
                aid,
                bid,
                price_cents,
                int(kr_fee_cents),
                int(creator_amount_cents),
            ),
        )

        try:
            conn.execute(
                "UPDATE apps SET downloads = downloads + 1, updated_at=datetime('now') WHERE id = ?;",
                (aid,),
            )
        except Exception:
            pass

        try:
            conn.commit()
        except Exception:
            pass

        # =========================================================
# VIDEOS
# =========================================================
def _sync_video_hashtags_conn(
    conn: sqlite3.Connection,
    video_id: str,
    title: str = "",
    description: str = "",
) -> None:
    vid = (video_id or "").strip()
    if not vid:
        return

    tags = _extract_hashtags(f"{title or ''} {description or ''}")

    try:
        conn.execute("DELETE FROM video_hashtags WHERE video_id=?;", (vid,))
    except Exception:
        pass

    for tag in tags:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO video_hashtags (video_id, hashtag, normalized_hashtag)
                VALUES (?, ?, ?);
                """,
                (vid, f"#{tag}", tag),
            )
        except Exception:
            pass


def sync_video_hashtags(video_id: str, title: str = "", description: str = "") -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False
    try:
        with get_connection() as conn:
            _sync_video_hashtags_conn(conn, vid, title=title, description=description)
            try:
                conn.commit()
            except Exception:
                pass
        return True
    except Exception:
        return False


def get_video_hashtags(video_id: str) -> List[str]:
    vid = (video_id or "").strip()
    if not vid:
        return []
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT hashtag
            FROM video_hashtags
            WHERE video_id=?
            ORDER BY normalized_hashtag ASC;
            """,
            (vid,),
        ).fetchall()
    return [str(r["hashtag"]) for r in rows if str(r["hashtag"] or "").strip()]


def get_video_thumbnail_transform(video_id: str) -> Dict[str, float]:
    vid = (video_id or "").strip()
    if not vid:
        return {"x": 50.0, "y": 50.0, "scale": 1.0}

    with get_connection() as conn:
        r = conn.execute(
            """
            SELECT
              COALESCE(thumbnail_pos_x, 50) AS thumbnail_pos_x,
              COALESCE(thumbnail_pos_y, 50) AS thumbnail_pos_y,
              COALESCE(thumbnail_scale, 1) AS thumbnail_scale
            FROM videos
            WHERE id = ?
            LIMIT 1;
            """,
            (vid,),
        ).fetchone()

    if not r:
        return {"x": 50.0, "y": 50.0, "scale": 1.0}

    return {
        "x": _norm_thumb_pos_x(r["thumbnail_pos_x"]),
        "y": _norm_thumb_pos_y(r["thumbnail_pos_y"]),
        "scale": _norm_thumb_scale(r["thumbnail_scale"]),
    }


def update_video_thumbnail_transform(
    video_id: str,
    *,
    thumbnail_pos_x: Any = 50.0,
    thumbnail_pos_y: Any = 50.0,
    thumbnail_scale: Any = 1.0,
    user_id: Optional[int] = None,
) -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False

    uid = _to_int(user_id) if user_id is not None else None
    pos_x = _norm_thumb_pos_x(thumbnail_pos_x)
    pos_y = _norm_thumb_pos_y(thumbnail_pos_y)
    scale = _norm_thumb_scale(thumbnail_scale)

    with get_connection() as conn:
        try:
            if uid is None:
                cur = conn.execute(
                    """
                    UPDATE videos
                    SET
                      thumbnail_pos_x = ?,
                      thumbnail_pos_y = ?,
                      thumbnail_scale = ?
                    WHERE id = ?;
                    """,
                    (pos_x, pos_y, scale, vid),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE videos
                    SET
                      thumbnail_pos_x = ?,
                      thumbnail_pos_y = ?,
                      thumbnail_scale = ?
                    WHERE id = ? AND user_id = ?;
                    """,
                    (pos_x, pos_y, scale, vid, uid),
                )

            try:
                conn.commit()
            except Exception:
                pass

            return bool(getattr(cur, "rowcount", 0))
        except Exception:
            return False


def add_video(
    user_id: int,
    filename: str,
    title: str = "",
    description: str = "",
    size_bytes: int = 0,
    visibility: str = "public",
    thumbnail_filename: Optional[str] = None,
    thumbnail_pos_x: Any = 50.0,
    thumbnail_pos_y: Any = 50.0,
    thumbnail_scale: Any = 1.0,
    video_id: Optional[str] = None,
) -> str:
    uid = _to_int(user_id)
    if uid is None:
        return ""

    filename = (filename or "").strip()
    if not filename:
        return ""

    thumb = (thumbnail_filename or "").strip() or None
    vid = (video_id or uuid4().hex).strip()
    vis = _norm_visibility(visibility)

    pos_x = _norm_thumb_pos_x(thumbnail_pos_x)
    pos_y = _norm_thumb_pos_y(thumbnail_pos_y)
    scale = _norm_thumb_scale(thumbnail_scale)

    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO videos (
              id, user_id, filename, thumbnail_filename,
              thumbnail_pos_x, thumbnail_pos_y, thumbnail_scale,
              title, description, size_bytes, visibility,
              views_count, shares_count, downloads_count, comments_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0);
            """,
            (
                vid,
                uid,
                filename,
                thumb,
                pos_x,
                pos_y,
                scale,
                (title or "").strip(),
                (description or "").strip() or None,
                max(0, int(size_bytes or 0)),
                vis,
            ),
        )
        _sync_video_hashtags_conn(conn, vid, title=title, description=description)
        try:
            conn.commit()
        except Exception:
            pass
    return vid


def list_videos_by_user(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    uid = _to_int(user_id)
    if uid is None:
        return []
    limit = max(1, min(int(limit), 500))
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM videos
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?;
            """,
            (uid, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def list_public_videos_by_user(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    uid = _to_int(user_id)
    if uid is None:
        return []
    limit = max(1, min(int(limit), 500))
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM videos
            WHERE user_id = ? AND visibility = 'public'
            ORDER BY created_at DESC
            LIMIT ?;
            """,
            (uid, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def list_public_videos(exclude_user_id: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    ex = _to_int(exclude_user_id)

    with get_connection() as conn:
        if ex is None:
            rows = conn.execute(
                """
                SELECT * FROM videos
                WHERE visibility = 'public'
                ORDER BY created_at DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM videos
                WHERE visibility = 'public' AND user_id != ?
                ORDER BY created_at DESC
                LIMIT ?;
                """,
                (ex, limit),
            ).fetchall()

    return [dict(r) for r in rows]


def get_video_by_id(video_id: str) -> Optional[Dict[str, Any]]:
    vid = (video_id or "").strip()
    if not vid:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM videos WHERE id = ?;", (vid,)).fetchone()
    return dict(r) if r else None


def delete_video(video_id: str, user_id: Optional[int] = None) -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False
    uid = _to_int(user_id) if user_id is not None else None

    with get_connection() as conn:
        if uid is None:
            cur = conn.execute("DELETE FROM videos WHERE id = ?;", (vid,))
        else:
            cur = conn.execute("DELETE FROM videos WHERE id = ? AND user_id = ?;", (vid, uid))
        try:
            conn.commit()
        except Exception:
            pass
    return bool(getattr(cur, "rowcount", 0))


# =========================================================
# USER BANNER TRANSFORM
# =========================================================
def get_user_banner_transform(user_id: int) -> Dict[str, float]:
    uid = _to_int(user_id)
    if uid is None:
        return {"x": 50.0, "y": 50.0, "scale": 1.0}

    with get_connection() as conn:
        r = conn.execute(
            """
            SELECT
              COALESCE(banner_pos_x, 50) AS banner_pos_x,
              COALESCE(banner_pos_y, 50) AS banner_pos_y,
              COALESCE(banner_scale, 1) AS banner_scale
            FROM users
            WHERE id = ?
            LIMIT 1;
            """,
            (uid,),
        ).fetchone()

    if not r:
        return {"x": 50.0, "y": 50.0, "scale": 1.0}

    return {
        "x": _norm_banner_pos_x(r["banner_pos_x"]),
        "y": _norm_banner_pos_y(r["banner_pos_y"]),
        "scale": _norm_banner_scale(r["banner_scale"]),
    }


def update_user_banner_transform(
    user_id: int,
    *,
    banner_pos_x: Any = 50.0,
    banner_pos_y: Any = 50.0,
    banner_scale: Any = 1.0,
) -> bool:
    uid = _to_int(user_id)
    if uid is None:
        return False

    pos_x = _norm_banner_pos_x(banner_pos_x)
    pos_y = _norm_banner_pos_y(banner_pos_y)
    scale = _norm_banner_scale(banner_scale)

    with get_connection() as conn:
        try:
            cur = conn.execute(
                """
                UPDATE users
                SET
                  banner_pos_x = ?,
                  banner_pos_y = ?,
                  banner_scale = ?,
                  updated_at = datetime('now')
                WHERE id = ?;
                """,
                (pos_x, pos_y, scale, uid),
            )
            try:
                conn.commit()
            except Exception:
                pass
            return bool(getattr(cur, "rowcount", 0))
        except Exception:
            return False


# =========================================================
# SEARCH HISTORY / TRENDS
# =========================================================
def record_search_query(user_id: int, query: str, area: str = "videos", kind: str = "general") -> bool:
    uid = _to_int(user_id)
    raw = str(query or "").strip()
    norm = _normalize_search_text(raw)
    area = _norm_search_area(area)
    kind = _norm_search_kind(kind)

    if uid is None or not raw or not norm or len(norm) < 2:
        return False

    with get_connection() as conn:
        try:
            conn.execute(
                """
                INSERT INTO search_queries (user_id, area, kind, query, normalized_query, hits, created_at, last_used_at)
                VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
                ON CONFLICT(user_id, area, kind, normalized_query) DO UPDATE SET
                  query = excluded.query,
                  hits = search_queries.hits + 1,
                  last_used_at = datetime('now');
                """,
                (uid, area, kind, raw[:120], norm[:120]),
            )
            try:
                conn.commit()
            except Exception:
                pass
            return True
        except Exception:
            return False


def list_recent_searches(user_id: int, area: str = "videos", limit: int = 8, kind: Optional[str] = None) -> List[str]:
    uid = _to_int(user_id)
    area = _norm_search_area(area)
    limit = max(1, min(int(limit), 30))
    if uid is None:
        return []

    with get_connection() as conn:
        if kind is None:
            rows = conn.execute(
                """
                SELECT query
                FROM search_queries
                WHERE user_id = ? AND area = ?
                ORDER BY last_used_at DESC, id DESC
                LIMIT ?;
                """,
                (uid, area, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT query
                FROM search_queries
                WHERE user_id = ? AND area = ? AND kind = ?
                ORDER BY last_used_at DESC, id DESC
                LIMIT ?;
                """,
                (uid, area, _norm_search_kind(kind), limit),
            ).fetchall()

    out: List[str] = []
    seen = set()
    for r in rows:
        q = str(r["query"] or "").strip()
        nq = _normalize_search_text(q)
        if q and nq not in seen:
            seen.add(nq)
            out.append(q)
    return out


def list_trending_searches(area: str = "videos", limit: int = 8, days: int = 14, kind: Optional[str] = None) -> List[str]:
    area = _norm_search_area(area)
    limit = max(1, min(int(limit), 30))
    days = max(1, min(int(days), 90))

    with get_connection() as conn:
        if kind is None:
            rows = conn.execute(
                """
                SELECT
                  normalized_query,
                  MAX(query) AS query,
                  SUM(hits) AS total_hits,
                  COUNT(DISTINCT user_id) AS users_count,
                  MAX(last_used_at) AS last_used_at
                FROM search_queries
                WHERE area = ?
                  AND datetime(last_used_at) >= datetime('now', ?)
                GROUP BY normalized_query
                HAVING LENGTH(normalized_query) >= 2
                ORDER BY total_hits DESC, users_count DESC, last_used_at DESC
                LIMIT ?;
                """,
                (area, f"-{days} days", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                  normalized_query,
                  MAX(query) AS query,
                  SUM(hits) AS total_hits,
                  COUNT(DISTINCT user_id) AS users_count,
                  MAX(last_used_at) AS last_used_at
                FROM search_queries
                WHERE area = ?
                  AND kind = ?
                  AND datetime(last_used_at) >= datetime('now', ?)
                GROUP BY normalized_query
                HAVING LENGTH(normalized_query) >= 2
                ORDER BY total_hits DESC, users_count DESC, last_used_at DESC
                LIMIT ?;
                """,
                (area, _norm_search_kind(kind), f"-{days} days", limit),
            ).fetchall()

    out: List[str] = []
    seen = set()
    for r in rows:
        q = str(r["query"] or "").strip()
        nq = str(r["normalized_query"] or "").strip()
        if q and nq and nq not in seen:
            seen.add(nq)
            out.append(q)
    return out


def clear_user_search_history(user_id: int, area: Optional[str] = None) -> int:
    uid = _to_int(user_id)
    if uid is None:
        return 0

    with get_connection() as conn:
        try:
            if area is None:
                cur = conn.execute("DELETE FROM search_queries WHERE user_id = ?;", (uid,))
            else:
                cur = conn.execute(
                    "DELETE FROM search_queries WHERE user_id = ? AND area = ?;",
                    (uid, _norm_search_area(area)),
                )
            try:
                conn.commit()
            except Exception:
                pass
            return int(getattr(cur, "rowcount", 0) or 0)
        except Exception:
            return 0


# =========================================================
# SEARCH PRO SUPREME — CREATORS / HASHTAGS / SEMANTIC
# =========================================================
def search_creators(query: str, limit: int = 12, only_public: bool = True) -> List[Dict[str, Any]]:
    raw = str(query or "").strip()
    if not raw:
        return []

    q = raw[1:] if raw.startswith("@") else raw
    q_norm = _normalize_search_text(q)
    if not q_norm:
        return []

    like = f"%{q_norm}%"
    limit = max(1, min(int(limit), 50))

    sql = """
    SELECT
      u.id,
      u.email,
      u.name,
      u.username,
      u.bio,
      u.avatar_url,
      u.banner_url,
      u.website,
      u.verified,
      u.is_public,
      u.public_slug,
      u.banner_pos_x,
      u.banner_pos_y,
      u.banner_scale,
      u.created_at
    FROM users u
    WHERE
      ({public_clause})
      AND (
        LOWER(COALESCE(u.username,'')) LIKE ?
        OR LOWER(COALESCE(u.name,'')) LIKE ?
        OR LOWER(COALESCE(u.public_slug,'')) LIKE ?
        OR LOWER(COALESCE(u.bio,'')) LIKE ?
      )
    ORDER BY
      CASE
        WHEN LOWER(COALESCE(u.username,'')) = ? THEN 100
        WHEN LOWER(COALESCE(u.public_slug,'')) = ? THEN 95
        WHEN LOWER(COALESCE(u.name,'')) = ? THEN 90
        WHEN LOWER(COALESCE(u.username,'')) LIKE ? THEN 80
        WHEN LOWER(COALESCE(u.name,'')) LIKE ? THEN 70
        WHEN LOWER(COALESCE(u.bio,'')) LIKE ? THEN 40
        ELSE 10
      END DESC,
      u.verified DESC,
      u.updated_at DESC,
      u.created_at DESC
    LIMIT ?;
    """.format(public_clause=("u.is_public = 1" if only_public else "1=1"))

    with get_connection() as conn:
        rows = conn.execute(
            sql,
            (
                like,
                like,
                like,
                like,
                q_norm,
                q_norm,
                q_norm,
                f"{q_norm}%",
                f"{q_norm}%",
                like,
                limit,
            ),
        ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        try:
            d["stats"] = get_user_social_stats(int(d["id"]))
        except Exception:
            d["stats"] = {
                "followers": 0,
                "following": 0,
                "videos_public": 0,
                "likes_received": 0,
                "collections_received": 0,
                "points": 0,
            }
        out.append(d)
    return out


def search_videos_by_hashtag(hashtag: str, viewer_user_id: Optional[int] = None, limit: int = 50) -> List[Dict[str, Any]]:
    raw = str(hashtag or "").strip()
    if not raw:
        return []

    tag = raw[1:] if raw.startswith("#") else raw
    tag_norm = _normalize_search_text(tag)
    tag_norm = re.sub(r"[^a-z0-9_]", "", tag_norm)
    if not tag_norm:
        return []

    limit = max(1, min(int(limit), 200))
    viewer_id = _to_int(viewer_user_id)

    with get_connection() as conn:
        if viewer_id is None:
            rows = conn.execute(
                """
                SELECT DISTINCT v.*
                FROM video_hashtags vh
                JOIN videos v ON v.id = vh.video_id
                WHERE vh.normalized_hashtag = ?
                  AND v.visibility = 'public'
                ORDER BY v.views_count DESC, v.created_at DESC
                LIMIT ?;
                """,
                (tag_norm, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT v.*
                FROM video_hashtags vh
                JOIN videos v ON v.id = vh.video_id
                WHERE vh.normalized_hashtag = ?
                  AND (
                    v.visibility = 'public'
                    OR v.user_id = ?
                  )
                ORDER BY v.views_count DESC, v.created_at DESC
                LIMIT ?;
                """,
                (tag_norm, viewer_id, limit),
            ).fetchall()

    return [dict(r) for r in rows]


def list_trending_hashtags(limit: int = 10, days: int = 30) -> List[str]:
    limit = max(1, min(int(limit), 30))
    days = max(1, min(int(days), 180))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              vh.normalized_hashtag,
              COUNT(*) AS uses,
              MAX(v.created_at) AS last_video_at
            FROM video_hashtags vh
            JOIN videos v ON v.id = vh.video_id
            WHERE v.visibility = 'public'
              AND datetime(v.created_at) >= datetime('now', ?)
            GROUP BY vh.normalized_hashtag
            ORDER BY uses DESC, last_video_at DESC
            LIMIT ?;
            """,
            (f"-{days} days", limit),
        ).fetchall()

    return [f"#{str(r['normalized_hashtag'])}" for r in rows if str(r["normalized_hashtag"] or "").strip()]


def _build_video_search_document(video: Dict[str, Any], creator: Optional[Dict[str, Any]] = None, hashtags: Optional[List[str]] = None) -> str:
    parts = [
        str(video.get("title") or ""),
        str(video.get("description") or ""),
    ]

    if creator:
        parts.extend(
            [
                str(creator.get("username") or ""),
                str(creator.get("name") or ""),
                str(creator.get("bio") or ""),
                str(creator.get("public_slug") or ""),
            ]
        )

    if hashtags:
        parts.extend([str(h or "") for h in hashtags])

    return _normalize_search_text(" ".join(parts))


def _semantic_score_document(query: str, document: str) -> int:
    q = _normalize_search_text(query)
    doc = _normalize_search_text(document)
    if not q or not doc:
        return 0

    q_tokens = [t for t in re.split(r"[^a-z0-9_]+", q) if t]
    if not q_tokens:
        return 0

    score = 0
    compact_doc = doc.replace(" ", "")

    for tok in q_tokens:
        if not tok:
            continue
        if tok in doc:
            score += 6
            if re.search(rf"(^|[\s]){re.escape(tok)}($|[\s])", doc):
                score += 2
        elif tok in compact_doc:
            score += 2

    if q in doc:
        score += 18

    return score


def search_videos_semantic(
    query: str,
    viewer_user_id: Optional[int] = None,
    limit: int = 50,
    include_private_owner: bool = True,
) -> List[Dict[str, Any]]:
    raw = str(query or "").strip()
    if not raw:
        return []

    q_norm = _normalize_search_text(raw)
    if not q_norm:
        return []

    limit = max(1, min(int(limit), 200))
    viewer_id = _to_int(viewer_user_id)

    with get_connection() as conn:
        if viewer_id is None:
            rows = conn.execute(
                """
                SELECT
                  v.*,
                  u.username,
                  u.name AS creator_name,
                  u.bio AS creator_bio,
                  u.public_slug
                FROM videos v
                LEFT JOIN users u ON u.id = v.user_id
                WHERE v.visibility = 'public'
                ORDER BY v.created_at DESC
                LIMIT 500;
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                  v.*,
                  u.username,
                  u.name AS creator_name,
                  u.bio AS creator_bio,
                  u.public_slug
                FROM videos v
                LEFT JOIN users u ON u.id = v.user_id
                WHERE (
                  v.visibility = 'public'
                  OR (? = 1 AND v.user_id = ?)
                )
                ORDER BY v.created_at DESC
                LIMIT 500;
                """,
                (1 if include_private_owner else 0, viewer_id),
            ).fetchall()

        ranked: List[Dict[str, Any]] = []

        for row in rows:
            d = dict(row)
            vid = str(d.get("id") or "").strip()
            tags_rows = conn.execute(
                "SELECT hashtag FROM video_hashtags WHERE video_id=? ORDER BY normalized_hashtag ASC;",
                (vid,),
            ).fetchall()
            hashtags = [str(x["hashtag"]) for x in tags_rows if str(x["hashtag"] or "").strip()]

            creator = {
                "username": d.get("username"),
                "name": d.get("creator_name"),
                "bio": d.get("creator_bio"),
                "public_slug": d.get("public_slug"),
            }

            doc = _build_video_search_document(d, creator=creator, hashtags=hashtags)
            sem_score = _semantic_score_document(q_norm, doc)

            if sem_score <= 0:
                continue

            d["_semantic_score"] = sem_score
            d["hashtags"] = hashtags
            ranked.append(d)

    ranked.sort(
        key=lambda x: (
            int(x.get("_semantic_score") or 0),
            int(x.get("views_count") or 0),
            int(x.get("comments_count") or 0),
            int(x.get("shares_count") or 0),
            str(x.get("created_at") or ""),
        ),
        reverse=True,
    )

    out = ranked[:limit]
    for item in out:
        try:
            del item["_semantic_score"]
        except Exception:
            pass
    return out


def search_videos_hybrid(
    query: str,
    viewer_user_id: Optional[int] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    raw = str(query or "").strip()
    if not raw:
        return {
            "query": "",
            "creators": [],
            "hashtags": [],
            "videos_exact": [],
            "videos_semantic": [],
            "top_hashtag": "",
            "top_creator": None,
        }

    creators = search_creators(raw, limit=min(limit, 12), only_public=True)

    hashtag_results: List[Dict[str, Any]] = []
    top_hashtag = ""
    if raw.startswith("#") or "#" in raw:
        found_tags = _extract_hashtags(raw)
        if found_tags:
            top_hashtag = f"#{found_tags[0]}"
            hashtag_results = search_videos_by_hashtag(found_tags[0], viewer_user_id=viewer_user_id, limit=limit)

    semantic = search_videos_semantic(raw, viewer_user_id=viewer_user_id, limit=limit)

    exact: List[Dict[str, Any]] = []
    q_norm = _normalize_search_text(raw)
    for item in semantic:
        title = _normalize_search_text(item.get("title") or "")
        desc = _normalize_search_text(item.get("description") or "")
        if q_norm and (q_norm in title or q_norm in desc):
            exact.append(item)

    seen_vids = set()
    exact_unique: List[Dict[str, Any]] = []
    for item in exact:
        vid = str(item.get("id") or "").strip()
        if vid and vid not in seen_vids:
            seen_vids.add(vid)
            exact_unique.append(item)

    return {
        "query": raw,
        "creators": creators[: min(limit, 12)],
        "hashtags": hashtag_results[:limit],
        "videos_exact": exact_unique[:limit],
        "videos_semantic": semantic[:limit],
        "top_hashtag": top_hashtag,
        "top_creator": creators[0] if creators else None,
    }


# =========================================================
# VIDEO VIEWS (PRO) — API
# =========================================================
def register_video_view(
    video_id: str,
    *,
    user_id: Optional[int] = None,
    session_id: Optional[str] = None,
    ip: Optional[str] = None,
    cooldown_minutes: Optional[int] = None,
) -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False

    uid = _to_int(user_id) if user_id is not None else None
    sid = (session_id or "").strip() or None
    iph = _ip_hash(ip)

    if cooldown_minutes is None:
        cooldown_minutes = _env_int("KR_VIEW_COOLDOWN_MINUTES", 30)
    cooldown_minutes = max(1, int(cooldown_minutes))

    with get_connection() as conn:
        v = conn.execute("SELECT id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return False

        try:
            if uid is not None:
                r = conn.execute(
                    """
                    SELECT 1 FROM video_views
                    WHERE video_id=? AND user_id=?
                      AND datetime(created_at) >= datetime('now', ?)
                    LIMIT 1;
                    """,
                    (vid, int(uid), f"-{cooldown_minutes} minutes"),
                ).fetchone()
                if r:
                    return False
            else:
                if sid:
                    r = conn.execute(
                        """
                        SELECT 1 FROM video_views
                        WHERE video_id=? AND session_id=?
                          AND datetime(created_at) >= datetime('now', ?)
                        LIMIT 1;
                        """,
                        (vid, sid, f"-{cooldown_minutes} minutes"),
                    ).fetchone()
                    if r:
                        return False
                elif iph:
                    r = conn.execute(
                        """
                        SELECT 1 FROM video_views
                        WHERE video_id=? AND ip_hash=?
                          AND datetime(created_at) >= datetime('now', ?)
                        LIMIT 1;
                        """,
                        (vid, iph, f"-{cooldown_minutes} minutes"),
                    ).fetchone()
                    if r:
                        return False
                else:
                    return False
        except Exception:
            return False

        try:
            conn.execute(
                """
                INSERT INTO video_views (video_id, user_id, session_id, ip_hash)
                VALUES (?, ?, ?, ?);
                """,
                (vid, uid, sid, iph or None),
            )
            conn.execute("UPDATE videos SET views_count = views_count + 1 WHERE id=?;", (vid,))
            try:
                conn.commit()
            except Exception:
                pass
            return True
        except Exception:
            return False


def get_video_views_count(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT views_count FROM videos WHERE id=?;", (vid,)).fetchone()
    return int(r["views_count"]) if r else 0


def add_video_view(video_id: str, user_id: Optional[int] = None, session_id: Optional[str] = None, ip: Optional[str] = None) -> bool:
    return register_video_view(video_id, user_id=user_id, session_id=session_id, ip=ip)


def count_video_views(video_id: str) -> int:
    return get_video_views_count(video_id)


# =========================================================
# VIDEO SHARES / DOWNLOADS / COMMENTS — API
# =========================================================
def add_video_share(video_id: str, user_id: Optional[int] = None) -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False
    uid = _to_int(user_id) if user_id is not None else None
    with get_connection() as conn:
        v = conn.execute("SELECT id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return False
        try:
            conn.execute("INSERT INTO video_shares (video_id, user_id) VALUES (?, ?);", (vid, uid))
            conn.execute("UPDATE videos SET shares_count = shares_count + 1 WHERE id=?;", (vid,))
            try:
                conn.commit()
            except Exception:
                pass
            return True
        except Exception:
            return False


def add_video_download(video_id: str, user_id: Optional[int] = None) -> bool:
    vid = (video_id or "").strip()
    if not vid:
        return False
    uid = _to_int(user_id) if user_id is not None else None
    with get_connection() as conn:
        v = conn.execute("SELECT id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return False
        try:
            conn.execute("INSERT INTO video_downloads (video_id, user_id) VALUES (?, ?);", (vid, uid))
            conn.execute("UPDATE videos SET downloads_count = downloads_count + 1 WHERE id=?;", (vid,))
            try:
                conn.commit()
            except Exception:
                pass
            return True
        except Exception:
            return False


def add_video_comment(video_id: str, user_id: Optional[int], text: str, parent_id: Optional[int] = None) -> int:
    vid = (video_id or "").strip()
    txt = (text or "").strip()
    if not vid or not txt:
        return 0
    uid = _to_int(user_id) if user_id is not None else None
    pid = _to_int(parent_id) if parent_id is not None else None

    with get_connection() as conn:
        v = conn.execute("SELECT id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return 0
        try:
            cur = conn.execute(
                "INSERT INTO video_comments (video_id, user_id, text, parent_id) VALUES (?, ?, ?, ?);",
                (vid, uid, txt, pid),
            )
            conn.execute("UPDATE videos SET comments_count = comments_count + 1 WHERE id=?;", (vid,))
            try:
                conn.commit()
            except Exception:
                pass
            return int(cur.lastrowid)
        except Exception:
            return 0


def list_video_comments(video_id: str, limit: int = 30, offset: int = 0) -> List[Dict[str, Any]]:
    vid = (video_id or "").strip()
    if not vid:
        return []
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              vc.id,
              vc.video_id,
              vc.user_id,
              COALESCE(u.username,'') AS username,
              COALESCE(NULLIF(TRIM(u.name),''), NULLIF(TRIM(u.username),''), u.email, 'Usuario') AS display_name,
              vc.text,
              vc.parent_id,
              vc.created_at
            FROM video_comments vc
            LEFT JOIN users u ON u.id = vc.user_id
            WHERE vc.video_id = ?
            ORDER BY vc.created_at DESC, vc.id DESC
            LIMIT ? OFFSET ?;
            """,
            (vid, limit, offset),
        ).fetchall()
    return [dict(r) for r in rows]


def count_video_shares(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT shares_count FROM videos WHERE id=?;", (vid,)).fetchone()
    return int(r["shares_count"]) if r else 0


def count_video_downloads(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT downloads_count FROM videos WHERE id=?;", (vid,)).fetchone()
    return int(r["downloads_count"]) if r else 0


def count_video_comments(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT comments_count FROM videos WHERE id=?;", (vid,)).fetchone()
    return int(r["comments_count"]) if r else 0


# =========================================================
# VIDEO LIKES (Me gusta) + POINTS
# =========================================================
def is_video_liked(video_id: str, user_id: int) -> bool:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return False
    with get_connection() as conn:
        r = conn.execute(
            "SELECT 1 FROM video_likes WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()
    return bool(r)


def toggle_video_like(video_id: str, user_id: int) -> bool:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return False

    with get_connection() as conn:
        v = conn.execute("SELECT id, user_id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return False

        owner_id = int(v["user_id"])

        liked = conn.execute(
            "SELECT 1 FROM video_likes WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()

        if liked:
            conn.execute("DELETE FROM video_likes WHERE video_id=? AND user_id=?;", (vid, uid))

            if owner_id != uid:
                _award_points_conn(
                    conn,
                    user_id=owner_id,
                    action="like_removed_received",
                    points=-2,
                    ref_type="video",
                    ref_id=str(vid),
                    meta={"by_user_id": uid},
                )
                _award_points_conn(
                    conn,
                    user_id=uid,
                    action="like_removed_given",
                    points=-1,
                    ref_type="video",
                    ref_id=str(vid),
                    meta={"owner_user_id": owner_id},
                )

            try:
                conn.commit()
            except Exception:
                pass
            return False

        conn.execute(
            "INSERT OR IGNORE INTO video_likes (video_id, user_id) VALUES (?, ?);",
            (vid, uid),
        )

        if owner_id != uid:
            _award_points_conn(
                conn,
                user_id=owner_id,
                action="like_received",
                points=2,
                ref_type="video",
                ref_id=str(vid),
                meta={"by_user_id": uid},
            )
            _award_points_conn(
                conn,
                user_id=uid,
                action="like_given",
                points=1,
                ref_type="video",
                ref_id=str(vid),
                meta={"owner_user_id": owner_id},
            )

        try:
            conn.commit()
        except Exception:
            pass

        return True


def count_video_likes(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM video_likes WHERE video_id=?;", (vid,)).fetchone()
    return int(r["c"]) if r else 0

# =========================================================
# VIDEO REACTIONS ✅
# =========================================================
_ALLOWED_REACTIONS = {"like", "fire", "haha", "wow", "angry", "devil", "heart", "clap", "star"}


def _norm_reaction(r: str) -> str:
    rr = (r or "").strip().lower()
    if rr in _ALLOWED_REACTIONS:
        return rr
    if rr:
        return rr[:24]
    return "like"


def get_user_video_reaction(video_id: str, user_id: int) -> Optional[str]:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return None
    with get_connection() as conn:
        r = conn.execute(
            "SELECT reaction FROM video_reactions WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()
    return str(r["reaction"]) if r and r["reaction"] is not None else None


def get_video_reactions_summary(video_id: str) -> Dict[str, Any]:
    vid = (video_id or "").strip()
    if not vid:
        return {"counts": {}, "total": 0}

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT reaction, COUNT(*) AS c
            FROM video_reactions
            WHERE video_id=?
            GROUP BY reaction
            ORDER BY c DESC;
            """,
            (vid,),
        ).fetchall()

    counts: Dict[str, int] = {}
    total = 0
    for r in rows:
        k = str(r["reaction"] or "")
        c = int(r["c"] or 0)
        if k:
            counts[k] = c
            total += c

    return {"counts": counts, "total": int(total)}


def toggle_video_reaction(video_id: str, user_id: int, reaction: str) -> Dict[str, Any]:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return {"ok": False, "error": "invalid"}

    react = _norm_reaction(reaction)

    with get_connection() as conn:
        v = conn.execute("SELECT id, user_id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return {"ok": False, "error": "video_not_found"}

        owner_id = int(v["user_id"])

        cur = conn.execute(
            "SELECT reaction FROM video_reactions WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()

        selected: Optional[str] = None

        if cur:
            prev = str(cur["reaction"] or "")
            if prev == react:
                conn.execute(
                    "DELETE FROM video_reactions WHERE video_id=? AND user_id=?;",
                    (vid, uid),
                )
                selected = None

                if owner_id != uid:
                    _award_points_conn(conn, owner_id, "reaction_removed_received", -1, "video", str(vid), {"by": uid, "reaction": react})
                    _award_points_conn(conn, uid, "reaction_removed_given", -1, "video", str(vid), {"owner": owner_id, "reaction": react})
            else:
                conn.execute(
                    """
                    UPDATE video_reactions
                    SET reaction=?, updated_at=datetime('now')
                    WHERE video_id=? AND user_id=?;
                    """,
                    (react, vid, uid),
                )
                selected = react

                if owner_id != uid:
                    _award_points_conn(conn, owner_id, "reaction_received", 1, "video", str(vid), {"by": uid, "reaction": react})
                    _award_points_conn(conn, uid, "reaction_given", 1, "video", str(vid), {"owner": owner_id, "reaction": react})
        else:
            conn.execute(
                """
                INSERT OR IGNORE INTO video_reactions (video_id, user_id, reaction)
                VALUES (?, ?, ?);
                """,
                (vid, uid, react),
            )
            selected = react

            if owner_id != uid:
                _award_points_conn(conn, owner_id, "reaction_received", 1, "video", str(vid), {"by": uid, "reaction": react})
                _award_points_conn(conn, uid, "reaction_given", 1, "video", str(vid), {"owner": owner_id, "reaction": react})

        try:
            conn.commit()
        except Exception:
            pass

    summary = get_video_reactions_summary(vid)
    return {"ok": True, "selected": selected, "counts": summary.get("counts", {}), "total": int(summary.get("total", 0))}


# =========================================================
# VIDEO COLLECTIONS + POINTS
# =========================================================
def is_video_collected(video_id: str, user_id: int) -> bool:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return False
    with get_connection() as conn:
        r = conn.execute(
            "SELECT 1 FROM video_collections WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()
    return bool(r)


def toggle_video_collect(video_id: str, user_id: int) -> bool:
    vid = (video_id or "").strip()
    uid = _to_int(user_id)
    if not vid or uid is None:
        return False

    with get_connection() as conn:
        v = conn.execute("SELECT id, user_id FROM videos WHERE id=? LIMIT 1;", (vid,)).fetchone()
        if not v:
            return False

        owner_id = int(v["user_id"])

        collected = conn.execute(
            "SELECT 1 FROM video_collections WHERE video_id=? AND user_id=? LIMIT 1;",
            (vid, uid),
        ).fetchone()

        if collected:
            conn.execute("DELETE FROM video_collections WHERE video_id=? AND user_id=?;", (vid, uid))

            if owner_id != uid:
                _award_points_conn(conn, owner_id, "collection_removed_received", -3, "video", str(vid), {"by_user_id": uid})
                _award_points_conn(conn, uid, "collection_removed_given", -1, "video", str(vid), {"owner_user_id": owner_id})

            try:
                conn.commit()
            except Exception:
                pass
            return False

        conn.execute(
            "INSERT OR IGNORE INTO video_collections (video_id, user_id) VALUES (?, ?);",
            (vid, uid),
        )

        if owner_id != uid:
            _award_points_conn(conn, owner_id, "collection_received", 3, "video", str(vid), {"by_user_id": uid})
            _award_points_conn(conn, uid, "collection_given", 1, "video", str(vid), {"owner_user_id": owner_id})

        try:
            conn.commit()
        except Exception:
            pass
        return True


def count_video_collections(video_id: str) -> int:
    vid = (video_id or "").strip()
    if not vid:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM video_collections WHERE video_id=?;", (vid,)).fetchone()
    return int(r["c"]) if r else 0


def list_my_library(user_id: int, limit: int = 300) -> List[Dict[str, Any]]:
    uid = _to_int(user_id)
    if uid is None:
        return []
    limit = max(1, min(int(limit), 500))

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT v.*
            FROM video_collections vc
            JOIN videos v ON v.id = vc.video_id
            WHERE vc.user_id = ?
            ORDER BY vc.created_at DESC
            LIMIT ?;
            """,
            (uid, limit),
        ).fetchall()

    return [dict(r) for r in rows]


# =========================================================
# FOLLOWERS + POINTS
# =========================================================
def follow_user(follower_id: int, followed_id: int) -> bool:
    fid = _to_int(follower_id)
    tid = _to_int(followed_id)
    if fid is None or tid is None or fid == tid:
        return False

    with get_connection() as conn:
        a = conn.execute("SELECT 1 FROM users WHERE id=? LIMIT 1;", (fid,)).fetchone()
        b = conn.execute("SELECT 1 FROM users WHERE id=? LIMIT 1;", (tid,)).fetchone()
        if not a or not b:
            return False

        cur = conn.execute(
            "INSERT OR IGNORE INTO followers (follower_id, followed_id) VALUES (?, ?);",
            (fid, tid),
        )
        inserted = bool(getattr(cur, "rowcount", 0))

        if inserted:
            _award_points_conn(conn, tid, "follow_received", 5, "user", str(fid), {"from_user_id": fid})

        try:
            conn.commit()
        except Exception:
            pass
        return inserted


def unfollow_user(follower_id: int, followed_id: int) -> bool:
    fid = _to_int(follower_id)
    tid = _to_int(followed_id)
    if fid is None or tid is None or fid == tid:
        return False

    with get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM followers WHERE follower_id = ? AND followed_id = ?;",
            (fid, tid),
        )
        deleted = bool(getattr(cur, "rowcount", 0))

        if deleted:
            _award_points_conn(conn, tid, "follow_removed", -5, "user", str(fid), {"from_user_id": fid})

        try:
            conn.commit()
        except Exception:
            pass
        return deleted


def is_following(follower_id: int, followed_id: int) -> bool:
    fid = _to_int(follower_id)
    tid = _to_int(followed_id)
    if fid is None or tid is None or fid == tid:
        return False

    with get_connection() as conn:
        r = conn.execute(
            "SELECT 1 FROM followers WHERE follower_id = ? AND followed_id = ? LIMIT 1;",
            (fid, tid),
        ).fetchone()
    return bool(r)


def toggle_follow(follower_id: int, followed_id: int) -> bool:
    if is_following(follower_id, followed_id):
        unfollow_user(follower_id, followed_id)
        return False
    return follow_user(follower_id, followed_id)


def count_followers(user_id: int) -> int:
    uid = _to_int(user_id)
    if uid is None:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM followers WHERE followed_id=?;", (uid,)).fetchone()
    return int(r["c"]) if r else 0


def count_following(user_id: int) -> int:
    uid = _to_int(user_id)
    if uid is None:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM followers WHERE follower_id=?;", (uid,)).fetchone()
    return int(r["c"]) if r else 0


# =========================================================
# POINTS SYSTEM — API PÚBLICA
# =========================================================
def award_points(
    user_id: int,
    action: str,
    points: int,
    ref_type: str = "",
    ref_id: str = "",
    meta: Optional[Any] = None,
) -> None:
    with get_connection() as conn:
        _award_points_conn(
            conn,
            user_id=user_id,
            action=action,
            points=points,
            ref_type=ref_type,
            ref_id=ref_id,
            meta=meta,
        )
        try:
            conn.commit()
        except Exception:
            pass


def get_points(user_id: int) -> int:
    uid = _to_int(user_id)
    if uid is None:
        return 0
    with get_connection() as conn:
        r = conn.execute("SELECT points FROM user_points WHERE user_id=?;", (uid,)).fetchone()
    return int(r["points"]) if r else 0


def top_points(limit: int = 100) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
              u.id AS user_id,
              COALESCE(u.name, u.email) AS display_name,
              COALESCE(p.points, 0) AS points
            FROM users u
            LEFT JOIN user_points p ON p.user_id = u.id
            ORDER BY points DESC, u.id ASC
            LIMIT ?;
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


# =========================================================
# PROFILE (PUBLIC) — API
# =========================================================
def get_user_by_id(user_id: Union[int, str]) -> Optional[Dict[str, Any]]:
    uid = _to_int(user_id)
    if uid is None:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM users WHERE id = ?;", (uid,)).fetchone()
    return dict(r) if r else None


def get_user_by_username(username: str) -> Optional[Dict[str, Any]]:
    u = _norm_username(username)
    if not u:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM users WHERE username = ? LIMIT 1;", (u,)).fetchone()
    return dict(r) if r else None


def get_user_by_public_slug(public_slug: str) -> Optional[Dict[str, Any]]:
    s = _slugify_public(public_slug)
    if not s:
        return None
    with get_connection() as conn:
        r = conn.execute("SELECT * FROM users WHERE public_slug = ? LIMIT 1;", (s,)).fetchone()
    return dict(r) if r else None


def update_user_profile(
    user_id: int,
    *,
    username: Optional[str] = None,
    name: Optional[str] = None,
    bio: Optional[str] = None,
    website: Optional[str] = None,
    avatar_url: Optional[str] = None,
    banner_url: Optional[str] = None,
    banner_pos_x: Optional[Any] = None,
    banner_pos_y: Optional[Any] = None,
    banner_scale: Optional[Any] = None,
) -> Dict[str, Any]:
    uid = _to_int(user_id)
    if uid is None:
        return {"ok": False, "error": "user_id inválido"}

    new_username = None
    if username is not None:
        u = _norm_username(username)
        if u and not _is_valid_username(u):
            return {"ok": False, "error": "username inválido (3-20, a-z, 0-9, _)"}
        new_username = u

    new_name = (name or "").strip() if name is not None else None
    new_bio = (bio or "").strip() if bio is not None else None
    new_website = (website or "").strip() if website is not None else None
    new_avatar = (avatar_url or "").strip() if avatar_url is not None else None
    new_banner = (banner_url or "").strip() if banner_url is not None else None

    new_banner_x = _norm_banner_pos_x(banner_pos_x) if banner_pos_x is not None else None
    new_banner_y = _norm_banner_pos_y(banner_pos_y) if banner_pos_y is not None else None
    new_banner_s = _norm_banner_scale(banner_scale) if banner_scale is not None else None

    if new_bio is not None and len(new_bio) > 280:
        new_bio = new_bio[:280]
    if new_website is not None and len(new_website) > 200:
        new_website = new_website[:200]
    if new_avatar is not None and len(new_avatar) > 500:
        new_avatar = new_avatar[:500]
    if new_banner is not None and len(new_banner) > 500:
        new_banner = new_banner[:500]

    with get_connection() as conn:
        user = conn.execute(
            "SELECT id, email, COALESCE(name,'') AS name, COALESCE(username,'') AS username, COALESCE(public_slug,'') AS public_slug FROM users WHERE id=?;",
            (uid,),
        ).fetchone()
        if not user:
            return {"ok": False, "error": "Usuario no existe"}

        if username is not None and (new_username is None or new_username == ""):
            base = _suggest_username_from_email_or_name(user["email"], user["name"])
            new_username = _generate_unique_username(conn, base)

        if username is not None and new_username:
            if str(user["username"] or "").strip().lower() != new_username:
                exists = conn.execute(
                    "SELECT 1 FROM users WHERE username=? AND id!=? LIMIT 1;",
                    (new_username, uid),
                ).fetchone()
                if exists:
                    new_username = _generate_unique_username(conn, new_username)

        fields: List[str] = []
        vals: List[Any] = []

        def set_field(col: str, val: Any) -> None:
            fields.append(f"{col}=?")
            vals.append(val)

        if username is not None:
            set_field("username", new_username)
            if not (user["public_slug"] or "").strip():
                slug = _generate_unique_public_slug(conn, base=new_username or "user", exclude_user_id=uid)
                set_field("public_slug", slug)

        if new_name is not None:
            set_field("name", new_name or None)
        if new_bio is not None:
            set_field("bio", new_bio or None)
        if new_website is not None:
            set_field("website", new_website or None)
        if new_avatar is not None:
            set_field("avatar_url", new_avatar or None)
        if new_banner is not None:
            set_field("banner_url", new_banner or None)
        if new_banner_x is not None:
            set_field("banner_pos_x", new_banner_x)
        if new_banner_y is not None:
            set_field("banner_pos_y", new_banner_y)
        if new_banner_s is not None:
            set_field("banner_scale", new_banner_s)

        if not fields:
            r = conn.execute("SELECT * FROM users WHERE id=?;", (uid,)).fetchone()
            return {"ok": True, "user": dict(r) if r else None}

        fields.append("updated_at=datetime('now')")
        q = f"UPDATE users SET {', '.join(fields)} WHERE id=?"
        vals.append(uid)

        try:
            conn.execute(q, tuple(vals))
            try:
                conn.commit()
            except Exception:
                pass
        except Exception as e:
            return {"ok": False, "error": f"DB error: {e}"}

        r = conn.execute("SELECT * FROM users WHERE id=?;", (uid,)).fetchone()
        return {"ok": True, "user": dict(r) if r else None}


def _count_likes_received(conn: sqlite3.Connection, owner_id: int) -> int:
    try:
        r = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM video_likes vl
            JOIN videos v ON v.id = vl.video_id
            WHERE v.user_id = ?;
            """,
            (int(owner_id),),
        ).fetchone()
        return int(r["c"]) if r else 0
    except Exception:
        return 0


def _count_collections_received(conn: sqlite3.Connection, owner_id: int) -> int:
    try:
        r = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM video_collections vc
            JOIN videos v ON v.id = vc.video_id
            WHERE v.user_id = ?;
            """,
            (int(owner_id),),
        ).fetchone()
        return int(r["c"]) if r else 0
    except Exception:
        return 0


def _count_public_videos(conn: sqlite3.Connection, owner_id: int) -> int:
    try:
        r = conn.execute(
            "SELECT COUNT(*) AS c FROM videos WHERE user_id=? AND visibility='public';",
            (int(owner_id),),
        ).fetchone()
        return int(r["c"]) if r else 0
    except Exception:
        return 0


def get_user_social_stats(user_id: int) -> Dict[str, Any]:
    uid = _to_int(user_id)
    if uid is None:
        return {
            "user_id": 0,
            "followers": 0,
            "following": 0,
            "videos_public": 0,
            "likes_received": 0,
            "collections_received": 0,
            "points": 0,
        }

    with get_connection() as conn:
        followers = 0
        following = 0
        try:
            r1 = conn.execute("SELECT COUNT(*) AS c FROM followers WHERE followed_id=?;", (uid,)).fetchone()
            followers = int(r1["c"]) if r1 else 0
            r2 = conn.execute("SELECT COUNT(*) AS c FROM followers WHERE follower_id=?;", (uid,)).fetchone()
            following = int(r2["c"]) if r2 else 0
        except Exception:
            followers = 0
            following = 0

        likes_received = _count_likes_received(conn, uid)
        collections_received = _count_collections_received(conn, uid)
        videos_public = _count_public_videos(conn, uid)

        points = 0
        try:
            rp = conn.execute("SELECT points FROM user_points WHERE user_id=?;", (uid,)).fetchone()
            points = int(rp["points"]) if rp else 0
        except Exception:
            points = 0

    return {
        "user_id": uid,
        "followers": int(followers),
        "following": int(following),
        "videos_public": int(videos_public),
        "likes_received": int(likes_received),
        "collections_received": int(collections_received),
        "points": int(points),
    }


# =========================================================
# EYE MEMORY (CORE IA)
# =========================================================
def save_memory(user_id: Optional[int], content: str, role: str = "user", importance: int = 1) -> int:
    content = (content or "").strip()
    if not content:
        return 0

    role = _norm_role_for_core(role)
    importance = max(1, min(int(importance), 5))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO eye_memory
            (user_id, role, content, importance, last_used_at)
            VALUES (?, ?, ?, ?, datetime('now'));
            """,
            (_to_int(user_id), role, content, importance),
        )
        try:
            conn.commit()
        except Exception:
            pass
        return int(cur.lastrowid)


def get_recent_memory(user_id: Optional[int], limit: int = 20) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 200))
    uid = _to_int(user_id)

    with get_connection() as conn:
        if uid is None:
            rows = conn.execute(
                """
                SELECT * FROM eye_memory
                ORDER BY last_used_at DESC, created_at DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM eye_memory
                WHERE user_id = ?
                ORDER BY last_used_at DESC, created_at DESC
                LIMIT ?;
                """,
                (uid, limit),
            ).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["role"] = _norm_role_for_core(d.get("role"))
        out.append(d)

    out.reverse()
    return out


def rotate_memory(user_id: Optional[int], max_active: int = 200) -> None:
    if max_active == 200:
        max_active = _env_int("CORE_MEMORY_LIMIT", 200)

    max_active = max(20, int(max_active))
    uid = _to_int(user_id)

    with get_connection() as conn:
        cur = conn.cursor()

        if uid is None:
            cur.execute("SELECT COUNT(*) AS c FROM eye_memory;")
            total = int(cur.fetchone()["c"])
        else:
            cur.execute("SELECT COUNT(*) AS c FROM eye_memory WHERE user_id = ?;", (uid,))
            total = int(cur.fetchone()["c"])

        if total <= max_active:
            return

        to_move = total - max_active

        if uid is None:
            rows = cur.execute(
                """
                SELECT id FROM eye_memory
                ORDER BY last_used_at ASC, created_at ASC
                LIMIT ?;
                """,
                (to_move,),
            ).fetchall()
        else:
            rows = cur.execute(
                """
                SELECT id FROM eye_memory
                WHERE user_id = ?
                ORDER BY last_used_at ASC, created_at ASC
                LIMIT ?;
                """,
                (uid, to_move),
            ).fetchall()

        ids = [int(r["id"]) for r in rows]
        if not ids:
            return

        q = ",".join("?" * len(ids))

        cur.execute(
            f"""
            INSERT INTO eye_memory_archive
            (user_id, role, content, importance, created_at, last_used_at)
            SELECT user_id, role, content, importance, created_at, last_used_at
            FROM eye_memory WHERE id IN ({q});
            """,
            ids,
        )

        cur.execute(f"DELETE FROM eye_memory WHERE id IN ({q});", ids)

        try:
            conn.commit()
        except Exception:
            pass


# =========================================================
# COMPAT ALIASES
# =========================================================
def toggle_like(video_id: str, user_id: int) -> bool:
    return toggle_video_like(video_id, user_id)


def toggle_collect(video_id: str, user_id: int) -> bool:
    return toggle_video_collect(video_id, user_id)

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    init_db()
    print(f"[OK] Database ready at: {DB_PATH}")