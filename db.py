from __future__ import annotations

import sqlite3
from pathlib import Path

from config import DATA_DIR, DB_PATH


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ativos (
            site_id INTEGER PRIMARY KEY,
            nome TEXT,
            codigo TEXT,
            emissor TEXT,
            cnpj_emissor TEXT,          -- NOVO
            numero_emissao TEXT,        -- NOVO (ex: 2ª Emissão)
            numero_serie TEXT,          -- NOVO (ex: 1ª Série)
            codigo_isin TEXT,           -- NOVO (ex: BRDELGDBS023)
            tipo_ativo TEXT,
            categoria TEXT,
            indice TEXT,                -- Indexador (ex: CDI)
            remuneracao TEXT,           -- NOVO (ex: CDI + 5,00% a.a.)
            data_emissao TEXT,
            data_vencimento TEXT,
            pagamento TEXT,
            status_pu TEXT,
            classificacao TEXT,
            cod_cetip TEXT,
            cod_bovespa TEXT,
            quantidade TEXT,            -- NOVO
            forma TEXT,                 -- NOVO
            resgate_antecipado TEXT,    -- NOVO
            registro_cvm TEXT,          -- NOVO 
            volume_serie TEXT,          -- NOVO
            garantia TEXT,              -- NOVO
            rating TEXT,                -- NOVO
            banco_liquidante TEXT,      -- NOVO
            coordenador_lider TEXT,     -- NOVO
            pu_atual NUMERIC,
            data_pu_atual TEXT,
            valor_nominal_atual NUMERIC,
            juros_atual NUMERIC,
            raw_json TEXT NOT NULL,
            raw_details_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pu_historico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            data_referencia TEXT NOT NULL,
            nome TEXT,
            indice TEXT,
            valor_nominal NUMERIC,
            juros NUMERIC,
            pu NUMERIC,
            raw_json TEXT NOT NULL,
            coletado_em TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(site_id, data_referencia),
            FOREIGN KEY(site_id) REFERENCES ativos(site_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pu_historico_site_id
            ON pu_historico(site_id);

        CREATE INDEX IF NOT EXISTS idx_pu_historico_data
            ON pu_historico(data_referencia);
        """
    )
    conn.commit()