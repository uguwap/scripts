import os
import re
import sys
import math
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import psycopg


if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


NEOPR_PREFIX = "НЕ ОПРЕД"
DEFAULT_SCHEMA = "experiments"
DEFAULT_EMBED_DIM = 256


@dataclass(frozen=True)
class DbCfg:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v is not None and str(v).strip() != "" else default


def get_db_cfg() -> DbCfg:
    return DbCfg(
        host=env("PGHOST", "localhost"),
        port=int(env("PGPORT", "5432")),
        dbname=env("PGDATABASE", "ai_referent"),
        user=env("PGUSER", "ai"),
        password=env("PGPASSWORD", "ai"),
    )


def read_xlsx(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0, dtype=str, engine="openpyxl")
    df = df.fillna("")
    expected = [
        "document_number",
        "document_operation_date",
        "payer_or_recipient_name",
        "payer_or_recipient_inn",
        "counterparty_okved",
        "is_leasing_company",
        "debtor_name",
        "debtor_inn",
        "debit_amount",
        "credit_amount",
        "payment_purpose",
        "tag",
        "llm_level_1",
        "llm_level_2",
        "llm_level_3",
    ]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Unexpected XLSX format. Missing columns: {missing}. Present: {list(df.columns)}")
    return df[expected].copy()


def is_legacy_undefined(tag: str) -> bool:
    t = (tag or "").strip()
    return t == "" or t == "Иное"


def is_llm_undefined(s: str) -> bool:
    t = (s or "").strip().upper()
    return t == "" or t.startswith(NEOPR_PREFIX)


def normalize_purpose_light(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", "<date>", s)
    s = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", s)
    s = re.sub(r"\b\d{5,}\b", "<num>", s)
    return s.strip()


def normalize_purpose_strong(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"\d+", "<n>", s)
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def stable_hash(text: str) -> bytes:
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).digest()


def char_ngrams(text: str, n: int) -> Iterable[str]:
    if len(text) < n:
        return []
    return (text[i : i + n] for i in range(len(text) - n + 1))


def hashed_char_ngram_embedding(text: str, dim: int = DEFAULT_EMBED_DIM) -> list[float]:
    """
    Дешёвое приближение эмбеддинга: char 3-5 grams -> hashing bucket -> L2 normalize.
    Это не "семантический" embedding, но отлично клеит near-duplicate формулировки
    (разные номера/даты/счета) и позволяет проверить гипотезу уменьшения LLM-запросов.
    """
    text = normalize_purpose_light(text)
    if not text:
        return [0.0] * dim

    vec = [0.0] * dim
    for n in (3, 4, 5):
        for ng in char_ngrams(text, n):
            h = stable_hash(ng)
            idx = int.from_bytes(h[:4], "little") % dim
            sign = 1.0 if (h[4] & 1) else -1.0
            vec[idx] += sign

    norm = math.sqrt(sum(v * v for v in vec))
    if norm <= 0:
        return vec
    return [v / norm for v in vec]


def connect(cfg: DbCfg) -> psycopg.Connection:
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        autocommit=True,
    )


def ensure_schema_and_tables(conn: psycopg.Connection, schema: str, embed_dim: int) -> None:
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}".tx_classification_raw (
              id bigserial PRIMARY KEY,
              source_file text NOT NULL,
              row_idx int NOT NULL,
              document_number text,
              document_operation_date text,
              payer_or_recipient_name text,
              payer_or_recipient_inn text,
              counterparty_okved text,
              is_leasing_company text,
              debtor_name text,
              debtor_inn text,
              debit_amount text,
              credit_amount text,
              payment_purpose text,
              legacy_tag text,
              llm_level_1 text,
              llm_level_2 text,
              llm_level_3 text,
              purpose_norm_light text,
              purpose_norm_strong text,
              legacy_is_undefined boolean NOT NULL,
              llm_is_undefined boolean NOT NULL,
              created_at timestamptz NOT NULL DEFAULT now(),
              UNIQUE (source_file, row_idx)
            );
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}".purpose_corpus (
              purpose_norm_strong text PRIMARY KEY,
              purpose_norm_light text NOT NULL,
              example_purpose text NOT NULL,
              embedding vector({embed_dim}),
              embedding_model text NOT NULL,
              created_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}".purpose_labels (
              purpose_norm_strong text PRIMARY KEY
                REFERENCES "{schema}".purpose_corpus(purpose_norm_strong) ON DELETE CASCADE,
              level_1 text NOT NULL,
              level_2 text NOT NULL,
              level_3 text NOT NULL,
              source text NOT NULL,
              confidence double precision NOT NULL,
              decided_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )


def upsert_raw(conn: psycopg.Connection, schema: str, source_file: str, df: pd.DataFrame) -> None:
    records: list[tuple] = []
    for idx, row in enumerate(df.to_dict(orient="records")):
        purpose = row["payment_purpose"]
        legacy_tag = row["tag"]
        llm1 = row["llm_level_1"]
        llm2 = row["llm_level_2"]
        llm3 = row["llm_level_3"]

        records.append(
            (
                source_file,
                idx,
                row["document_number"],
                row["document_operation_date"],
                row["payer_or_recipient_name"],
                row["payer_or_recipient_inn"],
                row["counterparty_okved"],
                row["is_leasing_company"],
                row["debtor_name"],
                row["debtor_inn"],
                row["debit_amount"],
                row["credit_amount"],
                purpose,
                legacy_tag,
                llm1,
                llm2,
                llm3,
                normalize_purpose_light(purpose),
                normalize_purpose_strong(purpose),
                is_legacy_undefined(legacy_tag),
                is_llm_undefined(llm1) or is_llm_undefined(llm2),
            )
        )

    insert_sql = f"""
    INSERT INTO "{schema}".tx_classification_raw (
      source_file, row_idx,
      document_number, document_operation_date,
      payer_or_recipient_name, payer_or_recipient_inn,
      counterparty_okved, is_leasing_company,
      debtor_name, debtor_inn,
      debit_amount, credit_amount,
      payment_purpose,
      legacy_tag,
      llm_level_1, llm_level_2, llm_level_3,
      purpose_norm_light, purpose_norm_strong,
      legacy_is_undefined, llm_is_undefined
    )
    VALUES (
      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    ON CONFLICT (source_file, row_idx) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.executemany(insert_sql, records)


def upsert_corpus_and_embeddings(conn: psycopg.Connection, schema: str, embed_dim: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT purpose_norm_strong, purpose_norm_light, min(payment_purpose) AS example_purpose
            FROM "{schema}".tx_classification_raw
            GROUP BY purpose_norm_strong, purpose_norm_light
            """
        )
        rows = cur.fetchall()

    to_upsert: list[tuple] = []
    for strong, light, example in rows:
        emb = hashed_char_ngram_embedding(example, dim=embed_dim)
        to_upsert.append((strong, light, example, emb, f"hash_char_3_5_dim{embed_dim}"))

    sql = f"""
    INSERT INTO "{schema}".purpose_corpus (
      purpose_norm_strong, purpose_norm_light, example_purpose, embedding, embedding_model
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (purpose_norm_strong) DO UPDATE
      SET purpose_norm_light = EXCLUDED.purpose_norm_light,
          example_purpose = EXCLUDED.example_purpose,
          embedding = EXCLUDED.embedding,
          embedding_model = EXCLUDED.embedding_model;
    """

    with conn.cursor() as cur:
        cur.executemany(sql, to_upsert)


def seed_labels_from_llm(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH labeled AS (
              SELECT
                purpose_norm_strong,
                llm_level_1 AS level_1,
                llm_level_2 AS level_2,
                coalesce(llm_level_3, '') AS level_3,
                count(*) AS cnt
              FROM "{schema}".tx_classification_raw
              WHERE NOT llm_is_undefined
              GROUP BY purpose_norm_strong, llm_level_1, llm_level_2, coalesce(llm_level_3, '')
            ),
            best AS (
              SELECT
                purpose_norm_strong, level_1, level_2, level_3,
                cnt,
                row_number() OVER (PARTITION BY purpose_norm_strong ORDER BY cnt DESC) AS rn
              FROM labeled
            )
            INSERT INTO "{schema}".purpose_labels (purpose_norm_strong, level_1, level_2, level_3, source, confidence)
            SELECT purpose_norm_strong, level_1, level_2, level_3, 'llm', 0.7
            FROM best
            WHERE rn = 1
            ON CONFLICT (purpose_norm_strong) DO NOTHING;
            """
        )


def ensure_vector_index(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            DO $$
            BEGIN
              IF NOT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'purpose_corpus_embedding_hnsw'
                  AND n.nspname = '{schema}'
              ) THEN
                EXECUTE 'CREATE INDEX purpose_corpus_embedding_hnsw ON "{schema}".purpose_corpus USING hnsw (embedding vector_cosine_ops)';
              END IF;
            END $$;
            """
        )


def report(conn: psycopg.Connection, schema: str, source_file: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              count(*) AS total,
              count(*) FILTER (WHERE legacy_is_undefined) AS legacy_undef,
              count(*) FILTER (WHERE llm_is_undefined) AS llm_undef,
              count(DISTINCT purpose_norm_strong) AS uniq_purpose_strong,
              count(DISTINCT purpose_norm_strong) FILTER (WHERE llm_is_undefined) AS uniq_strong_llm_undef,
              count(*) FILTER (WHERE legacy_is_undefined AND NOT llm_is_undefined) AS uplift_from_llm_on_legacy_undef
            FROM "{schema}".tx_classification_raw
            WHERE source_file = %s
            """,
            (source_file,),
        )
        row = cur.fetchone()

        cur.execute(f'SELECT count(*) FROM "{schema}".purpose_corpus')
        corpus = cur.fetchone()[0]
        cur.execute(f'SELECT count(*) FROM "{schema}".purpose_labels')
        labels = cur.fetchone()[0]

    total, legacy_undef, llm_undef, uniq_strong, uniq_strong_llm_undef, uplift = row
    print("=== Loaded to Postgres ===")
    print("source_file:", source_file)
    print("rows_total:", total)
    print("legacy_undefined_rows:", legacy_undef, f"({legacy_undef/total:.2%})")
    print("llm_undefined_rows:", llm_undef, f"({llm_undef/total:.2%})")
    print("unique_purpose_strong:", uniq_strong)
    print("unique_strong_llm_undefined:", uniq_strong_llm_undef)
    print("uplift_rows_where_legacy_undefined_but_llm_defined:", uplift)
    print("purpose_corpus_rows:", corpus)
    print("purpose_labels_seeded:", labels)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python load_results_to_postgres.py <path_to_xlsx> [schema]")
        raise SystemExit(2)

    xlsx_path = Path(sys.argv[1]).resolve()
    schema = sys.argv[2] if len(sys.argv) >= 3 else DEFAULT_SCHEMA
    embed_dim = int(env("EMBED_DIM", str(DEFAULT_EMBED_DIM)))

    if not xlsx_path.exists():
        raise FileNotFoundError(str(xlsx_path))

    df = read_xlsx(xlsx_path)
    source_file = xlsx_path.name

    cfg = get_db_cfg()
    with connect(cfg) as conn:
        ensure_schema_and_tables(conn, schema=schema, embed_dim=embed_dim)
        upsert_raw(conn, schema=schema, source_file=source_file, df=df)
        upsert_corpus_and_embeddings(conn, schema=schema, embed_dim=embed_dim)
        seed_labels_from_llm(conn, schema=schema)
        ensure_vector_index(conn, schema=schema)
        report(conn, schema=schema, source_file=source_file)

    print("OK")


if __name__ == "__main__":
    main()

