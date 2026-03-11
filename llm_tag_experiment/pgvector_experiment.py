import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import psycopg


if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


DEFAULT_SCHEMA = "experiments"
DEFAULT_OUT_DIR = Path(__file__).parent / "output"


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


def load_env_file_if_exists(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def get_db_cfg() -> DbCfg:
    return DbCfg(
        host=env("PGHOST", "localhost"),
        port=int(env("PGPORT", "5432")),
        dbname=env("PGDATABASE", "ai_referent"),
        user=env("PGUSER", "ai"),
        password=env("PGPASSWORD", "ai"),
    )


def connect(cfg: DbCfg) -> psycopg.Connection:
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        autocommit=True,
    )


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def ensure_nn_table(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{schema}".purpose_nn (
              purpose_norm_strong text PRIMARY KEY,
              nn_purpose_norm_strong text NOT NULL,
              sim double precision NOT NULL,
              nn_level_1 text NOT NULL,
              nn_level_2 text NOT NULL,
              nn_level_3 text NOT NULL,
              nn_example_purpose text NOT NULL,
              computed_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )


def recompute_nn(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE "{schema}".purpose_nn')
        cur.execute(
            f"""
            INSERT INTO "{schema}".purpose_nn (
              purpose_norm_strong,
              nn_purpose_norm_strong,
              sim,
              nn_level_1,
              nn_level_2,
              nn_level_3,
              nn_example_purpose
            )
            SELECT
              c.purpose_norm_strong,
              nn.purpose_norm_strong AS nn_purpose_norm_strong,
              1 - (c.embedding <=> nn.embedding) AS sim,
              pl.level_1 AS nn_level_1,
              pl.level_2 AS nn_level_2,
              pl.level_3 AS nn_level_3,
              nn.example_purpose AS nn_example_purpose
            FROM "{schema}".purpose_corpus c
            JOIN LATERAL (
              SELECT pc.purpose_norm_strong, pc.embedding, pc.example_purpose
              FROM "{schema}".purpose_corpus pc
              JOIN "{schema}".purpose_labels pl2 ON pl2.purpose_norm_strong = pc.purpose_norm_strong
              WHERE pc.embedding IS NOT NULL
              ORDER BY c.embedding <=> pc.embedding
              LIMIT 1
            ) nn ON true
            JOIN "{schema}".purpose_labels pl ON pl.purpose_norm_strong = nn.purpose_norm_strong
            WHERE c.embedding IS NOT NULL;
            """
        )


def eval_loocv(conn: psycopg.Connection, schema: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH labeled AS (
              SELECT
                pc.purpose_norm_strong,
                pc.embedding,
                pl.level_1,
                pl.level_2,
                pl.level_3
              FROM "{schema}".purpose_corpus pc
              JOIN "{schema}".purpose_labels pl
                ON pl.purpose_norm_strong = pc.purpose_norm_strong
              WHERE pc.embedding IS NOT NULL
            ),
            preds AS (
              SELECT
                l.purpose_norm_strong AS src,
                l.level_1 AS true_l1,
                l.level_2 AS true_l2,
                l.level_3 AS true_l3,
                1 - (l.embedding <=> nn.embedding) AS sim,
                nn.level_1 AS pred_l1,
                nn.level_2 AS pred_l2,
                nn.level_3 AS pred_l3
              FROM labeled l
              JOIN LATERAL (
                SELECT
                  l2.embedding,
                  l2.level_1,
                  l2.level_2,
                  l2.level_3
                FROM labeled l2
                WHERE l2.purpose_norm_strong <> l.purpose_norm_strong
                ORDER BY l.embedding <=> l2.embedding
                LIMIT 1
              ) nn ON true
            )
            SELECT
              count(*) AS n,
              avg(sim) AS avg_sim,
              count(*) FILTER (WHERE true_l1 = pred_l1) AS acc_l1,
              count(*) FILTER (WHERE true_l1 = pred_l1 AND true_l2 = pred_l2) AS acc_l2,
              count(*) FILTER (WHERE true_l1 = pred_l1 AND true_l2 = pred_l2 AND true_l3 = pred_l3) AS acc_l3,
              count(*) FILTER (WHERE sim >= 0.80) AS n_ge_080,
              count(*) FILTER (WHERE sim >= 0.90) AS n_ge_090,
              count(*) FILTER (WHERE sim >= 0.92) AS n_ge_092,
              count(*) FILTER (WHERE sim >= 0.94) AS n_ge_094
            FROM preds
            """
        )
        row = cur.fetchone()

    n, avg_sim, acc_l1, acc_l2, acc_l3, n080, n090, n092, n094 = row
    n = int(n or 0)
    return {
        "n": n,
        "avg_sim": float(avg_sim or 0.0),
        "acc_l1": int(acc_l1 or 0),
        "acc_l2": int(acc_l2 or 0),
        "acc_l3": int(acc_l3 or 0),
        "n_ge_080": int(n080 or 0),
        "n_ge_090": int(n090 or 0),
        "n_ge_092": int(n092 or 0),
        "n_ge_094": int(n094 or 0),
    }


def export_enriched_excel(conn: psycopg.Connection, schema: str, source_file: str, out_path: Path) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              r.row_idx,
              r.document_number,
              r.document_operation_date,
              r.payer_or_recipient_name,
              r.payer_or_recipient_inn,
              r.counterparty_okved,
              r.is_leasing_company,
              r.debtor_name,
              r.debtor_inn,
              r.debit_amount,
              r.credit_amount,
              r.payment_purpose,
              r.legacy_tag,
              r.llm_level_1,
              r.llm_level_2,
              r.llm_level_3,
              r.legacy_is_undefined,
              r.llm_is_undefined,
              nn.sim,
              nn.nn_level_1,
              nn.nn_level_2,
              nn.nn_level_3,
              nn.nn_example_purpose
            FROM "{schema}".tx_classification_raw r
            LEFT JOIN "{schema}".purpose_nn nn
              ON nn.purpose_norm_strong = r.purpose_norm_strong
            WHERE r.source_file = %s
            ORDER BY r.row_idx
            """,
            (source_file,),
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=cols)

    def decide(row) -> tuple[str, str, str, str, float]:
        llm_undef = bool(row["llm_is_undefined"])
        legacy_undef = bool(row["legacy_is_undefined"])
        sim = float(row["sim"]) if row["sim"] is not None else 0.0

        llm1 = normalize_ws(row["llm_level_1"])
        llm2 = normalize_ws(row["llm_level_2"])
        llm3 = normalize_ws(row["llm_level_3"])

        if not llm_undef and (llm1 or llm2 or llm3):
            return ("llm", llm1, llm2, llm3, 1.0)

        n1 = normalize_ws(row["nn_level_1"])
        n2 = normalize_ws(row["nn_level_2"])
        n3 = normalize_ws(row["nn_level_3"])

        if sim >= 0.90 and (n1 or n2 or n3):
            return ("pgvector_auto_ge_0_90", n1, n2, n3, sim)
        if sim >= 0.80 and (n1 or n2 or n3):
            return ("pgvector_review_0_80_0_90", n1, n2, n3, sim)

        if not legacy_undef:
            return ("legacy_only", "", normalize_ws(row["legacy_tag"]), "", 0.0)

        return ("unresolved", "", "", "", sim)

    decisions = df.apply(decide, axis=1, result_type="expand")
    decisions.columns = ["resolved_source", "resolved_level_1", "resolved_level_2", "resolved_level_3", "resolved_confidence"]
    df = pd.concat([df, decisions], axis=1)

    df["payment_purpose"] = df["payment_purpose"].astype(str)
    df["nn_example_purpose"] = df["nn_example_purpose"].astype(str)
    df["resolved_level_1"] = df["resolved_level_1"].astype(str)
    df["resolved_level_2"] = df["resolved_level_2"].astype(str)
    df["resolved_level_3"] = df["resolved_level_3"].astype(str)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Enriched")

        stats = []
        stats.append(("rows_total", int(df.shape[0])))
        stats.append(("legacy_undefined_rows", int(df["legacy_is_undefined"].sum())))
        stats.append(("llm_undefined_rows", int(df["llm_is_undefined"].sum())))
        stats.append(("resolved_by_llm_rows", int((df["resolved_source"] == "llm").sum())))
        stats.append(("resolved_auto_ge_0_90_rows", int((df["resolved_source"] == "pgvector_auto_ge_0_90").sum())))
        stats.append(("resolved_review_0_80_0_90_rows", int((df["resolved_source"] == "pgvector_review_0_80_0_90").sum())))
        stats.append(("unresolved_rows", int((df["resolved_source"] == "unresolved").sum())))
        stats_df = pd.DataFrame(stats, columns=["metric", "value"])
        stats_df.to_excel(w, index=False, sheet_name="Stats")


def main() -> None:
    base = Path(__file__).resolve()
    for d in (base.parent, base.parents[1], base.parents[2]):
        load_env_file_if_exists(d / ".env")

    schema = env("EXPERIMENT_SCHEMA", DEFAULT_SCHEMA)
    source_file = env("EXPERIMENT_SOURCE_FILE", "classification_partial.xlsx")
    out_dir = Path(env("EXPERIMENT_OUT_DIR", str(DEFAULT_OUT_DIR))).resolve()
    out_path = out_dir / "classification_partial_pgvector.xlsx"

    cfg = get_db_cfg()
    with connect(cfg) as conn:
        ensure_nn_table(conn, schema=schema)
        recompute_nn(conn, schema=schema)
        metrics = eval_loocv(conn, schema=schema)
        export_enriched_excel(conn, schema=schema, source_file=source_file, out_path=out_path)

    n = metrics["n"]
    def pct(x: int) -> str:
        return f"{(x / n * 100):.1f}%" if n else "0%"

    print("=== pgvector experiment ===")
    print("schema:", schema)
    print("source_file:", source_file)
    print("output:", str(out_path))
    print("")
    print("LOOCV labeled purposes:", n)
    print("avg_sim:", round(metrics["avg_sim"], 4))
    print("top1 match level_1:", metrics["acc_l1"], pct(metrics["acc_l1"]))
    print("top1 match level_1+2:", metrics["acc_l2"], pct(metrics["acc_l2"]))
    print("top1 match level_1+2+3:", metrics["acc_l3"], pct(metrics["acc_l3"]))
    print("count sim>=0.80:", metrics["n_ge_080"])
    print("count sim>=0.90:", metrics["n_ge_090"])
    print("count sim>=0.92:", metrics["n_ge_092"])
    print("count sim>=0.94:", metrics["n_ge_094"])
    print("OK")


if __name__ == "__main__":
    main()

