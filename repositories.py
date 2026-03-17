from __future__ import annotations

import sqlite3
from typing import Any


class AtivoRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def upsert(self, payload: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO ativos (
                site_id,
                nome,
                codigo,
                emissor,
                cnpj_emissor,
                numero_emissao,
                numero_serie,
                codigo_isin,
                tipo_ativo,
                categoria,
                indice,
                remuneracao,
                data_emissao,
                data_vencimento,
                pagamento,
                status_pu,
                classificacao,
                cod_cetip,
                cod_bovespa,
                quantidade,
                forma,
                resgate_antecipado,
                registro_cvm,
                volume_serie,
                garantia,
                rating,
                banco_liquidante,
                coordenador_lider,
                data_pu_atual,
                pu_atual,
                valor_nominal_atual,
                juros_atual,
                raw_json,
                raw_details_json,
                created_at,
                updated_at
            )
            VALUES (
                :site_id,
                :nome,
                :codigo,
                :emissor,
                :cnpj_emissor,
                :numero_emissao,
                :numero_serie,
                :codigo_isin,
                :tipo_ativo,
                :categoria,
                :indice,
                :remuneracao,
                :data_emissao,
                :data_vencimento,
                :pagamento,
                :status_pu,
                :classificacao,
                :cod_cetip,
                :cod_bovespa,
                :quantidade,
                :forma,
                :resgate_antecipado,
                :registro_cvm,
                :volume_serie,
                :garantia,
                :rating,
                :banco_liquidante,
                :coordenador_lider,
                :data_pu_atual,
                :pu_atual,
                :valor_nominal_atual,
                :juros_atual,
                :raw_json,
                :raw_details_json,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT(site_id) DO UPDATE SET
                nome = excluded.nome,
                codigo = excluded.codigo,
                emissor = excluded.emissor,
                cnpj_emissor = excluded.cnpj_emissor,
                numero_emissao = excluded.numero_emissao,
                numero_serie = excluded.numero_serie,
                codigo_isin = excluded.codigo_isin,
                tipo_ativo = excluded.tipo_ativo,
                categoria = excluded.categoria,
                indice = excluded.indice,
                remuneracao = excluded.remuneracao,
                data_emissao = excluded.data_emissao,
                data_vencimento = excluded.data_vencimento,
                pagamento = excluded.pagamento,
                status_pu = excluded.status_pu,
                classificacao = excluded.classificacao,
                cod_cetip = excluded.cod_cetip,
                cod_bovespa = excluded.cod_bovespa,
                quantidade = excluded.quantidade,
                forma = excluded.forma,
                resgate_antecipado = excluded.resgate_antecipado,
                registro_cvm = excluded.registro_cvm,
                volume_serie = excluded.volume_serie,
                garantia = excluded.garantia,
                rating = excluded.rating,
                banco_liquidante = excluded.banco_liquidante,
                coordenador_lider = excluded.coordenador_lider,
                data_pu_atual = excluded.data_pu_atual,
                pu_atual = excluded.pu_atual,
                valor_nominal_atual = excluded.valor_nominal_atual,
                juros_atual = excluded.juros_atual,
                raw_json = excluded.raw_json,
                raw_details_json = excluded.raw_details_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            payload,
        )

    def list_all_ids(self) -> list[int]:
        rows = self.conn.execute(
            "SELECT site_id FROM ativos ORDER BY site_id"
        ).fetchall()
        return [row["site_id"] for row in rows]


class PuHistoricoRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def insert_many(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        before = self.conn.total_changes

        self.conn.executemany(
            """
            INSERT OR IGNORE INTO pu_historico (
                site_id,
                data_referencia,
                nome,
                indice,
                valor_nominal,
                juros,
                pu,
                raw_json
            )
            VALUES (
                :site_id,
                :data_referencia,
                :nome,
                :indice,
                :valor_nominal,
                :juros,
                :pu,
                :raw_json
            )
            """,
            rows,
        )

        after = self.conn.total_changes
        return after - before