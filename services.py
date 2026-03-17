from __future__ import annotations

import json
import sqlite3
from typing import Any

from client import FiduciarioClient
from repositories import AtivoRepository, PuHistoricoRepository
from utils import (
    ensure_json_string,
    first_existing,
    normalize_scalar,
    parse_date,
    parse_decimal,
)


def _try_parse_json_string(value: str) -> Any:
    value = value.strip()
    if not value:
        return value

    try:
        return json.loads(value)
    except Exception:
        return value


def _extract_candidate_list(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if isinstance(data, dict):
        candidate_keys = (
            "RetornoPU",
            "Retorno",
            "data",
            "dados",
            "result",
            "results",
            "items",
            "rows",
            "lista",
            "titulos",
        )

        for key in candidate_keys:
            if key not in data:
                continue

            inner = data[key]

            if isinstance(inner, list):
                return [item for item in inner if isinstance(item, dict)]

            if isinstance(inner, dict):
                nested = _extract_candidate_list(inner)
                if nested:
                    return nested
                return [inner]

            if isinstance(inner, str):
                parsed = _try_parse_json_string(inner)
                nested = _extract_candidate_list(parsed)
                if nested:
                    return nested

        for _, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                return value

        return []

    if isinstance(data, str):
        parsed = _try_parse_json_string(data)
        return _extract_candidate_list(parsed)

    return []


def normalize_to_list(data: Any) -> list[dict[str, Any]]:
    return _extract_candidate_list(data)

def build_detail_lookup(detail_data: Any) -> dict[str, Any]:
    items = normalize_to_list(detail_data)
    if items:
        return items[0]

    if isinstance(detail_data, dict):
        return detail_data

    return {}

def build_ativo_row(
    item: dict[str, Any],
    detail_item: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    detail_item = detail_item or {}

    # O ID principal vem como 'id_titulo' no detalhe
    site_id = first_existing(detail_item, "id_titulo") or first_existing(item, "ID_TITULO", "id")
    if site_id is None:
        return None

    nome = first_existing(detail_item, "nome_emissao", "nome_fantasia_emissao") or first_existing(item, "NOME", "nome")
    
    cod_cetip = first_existing(detail_item, "codcetip_emissao") or first_existing(item, "CODCETIP")
    cod_bovespa = first_existing(detail_item, "codbovespa_emissao") or first_existing(item, "CODBOVESPA")
    
    # Se não tiver B3, usa CETIP
    codigo = cod_bovespa or cod_cetip

    emissor = first_existing(detail_item, "razao_social_emissao") or first_existing(item, "EMISSOR")
    tipo_ativo = first_existing(detail_item, "classificacao_emissao") or first_existing(item, "TIPO_ATIVO")
    
    # === NOVOS CAMPOS MAPEADOS DIRETAMENTE DO AJAX ===
    cnpj_emissor = first_existing(detail_item, "cnpj_emissao")
    numero_emissao = first_existing(detail_item, "numero_emissao_emissao")
    numero_serie = first_existing(detail_item, "numero_serie_emissao")
    codigo_isin = first_existing(detail_item, "isin_emissao")
    remuneracao = first_existing(detail_item, "remuneracao_emissao") # Ex: Prefixado 13,3181% a.a. ou CDI + 5%
    garantia = first_existing(detail_item, "garantia_emissao")
    rating = first_existing(detail_item, "rating_emissao") # Pode vir vazio
    quantidade = first_existing(detail_item, "quantidade_emissao")
    forma = first_existing(detail_item, "forma_emissao")
    resgate_antecipado = first_existing(detail_item, "resgate_antecipado_emissao")
    registro_cvm = first_existing(detail_item, "instrucao_cvm_emissao")
    volume_serie = first_existing(detail_item, "volume_emissao")
    coordenador_lider = first_existing(detail_item, "coordenador_lider_emissao")
    banco_liquidante = first_existing(detail_item, "escriturador_mandarario_emissao", "banco_liquidante_emissao")
    
    indice = first_existing(detail_item, "indexador_emissao") or first_existing(item, "INDEXADOR", "INDICE")

    # Datas (O payload retorna '2024-10-03T00:00:00-03:00')
    data_emissao = parse_date(first_existing(detail_item, "data_emissao_emissao", "DATA_EMISSAO"))
    data_vencimento = parse_date(first_existing(detail_item, "data_vencimento_emissao", "DATA_VENCIMENTO"))
    
    pagamento = first_existing(detail_item, "pagamento_emissao") or first_existing(item, "PAGAMENTO")
    status_pu = first_existing(detail_item, "status_emissao", "situacao_emissao") or first_existing(item, "STATUS_PU")
    categoria = first_existing(item, "CATEGORIA")

    # Dados do PU atual
    data_pu_atual = parse_date(first_existing(item, "DATA", "dataBase", "dataReferencia"))
    pu_atual = parse_decimal(first_existing(item, "PUPAR", "PU"))
    valor_nominal_atual = parse_decimal(first_existing(item, "VALOR_NOMINAL_ATUALIZADO", "VALOR_NOMINAL"))
    juros_atual = parse_decimal(first_existing(item, "JUROS_INDICE", "VALOR_JUROS_DI"))

    return {
        "site_id": int(site_id),
        "nome": nome,
        "codigo": codigo,
        "emissor": emissor,
        "cnpj_emissor": cnpj_emissor,
        "numero_emissao": numero_emissao,
        "numero_serie": numero_serie,
        "codigo_isin": codigo_isin,
        "tipo_ativo": tipo_ativo,
        "categoria": categoria,
        "indice": indice,
        "remuneracao": remuneracao,
        "data_emissao": data_emissao,
        "data_vencimento": data_vencimento,
        "pagamento": pagamento,
        "status_pu": status_pu,
        "classificacao": tipo_ativo, # Geralmente é a mesma coisa
        "cod_cetip": cod_cetip,
        "cod_bovespa": cod_bovespa,
        "quantidade": quantidade,
        "volume_serie": volume_serie,
        "garantia": garantia,
        "forma": forma,                               # NOVO AQUI
        "resgate_antecipado": resgate_antecipado,     # NOVO AQUI
        "registro_cvm": registro_cvm,                 # NOVO AQUI
        "rating": rating,
        "coordenador_lider": coordenador_lider,
        "banco_liquidante": banco_liquidante,
        "data_pu_atual": data_pu_atual,
        "pu_atual": str(pu_atual) if pu_atual is not None else None,
        "valor_nominal_atual": str(valor_nominal_atual) if valor_nominal_atual is not None else None,
        "juros_atual": str(juros_atual) if juros_atual is not None else None,
        "raw_json": ensure_json_string(item),
        "raw_details_json": ensure_json_string(detail_item) if detail_item else None,
    }


def build_history_rows(asset_id: int, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for item in items:
        data_ref = parse_date(
            first_existing(
                item,
                "DATA",
                "data",
                "Data",
                "dataBase",
                "dataReferencia",
            )
        )
        if not data_ref:
            continue

        nome = first_existing(item, "NOME", "Nome", "nome", "titulo", "Titulo", "ativo")
        indice = first_existing(item, "INDEXADOR", "INDICE", "indice", "Indice")

        valor_nominal = parse_decimal(
            first_existing(
                item,
                "VALOR_NOMINAL_ATUALIZADO",
                "VALOR_NOMINAL",
                "valorNominal",
                "valor_nominal",
                "ValorNominal",
            )
        )

        juros = parse_decimal(
            first_existing(
                item,
                "JUROS_INDICE",
                "VALOR_JUROS_DI",
                "juros",
                "Juros",
            )
        )

        pu = parse_decimal(
            first_existing(
                item,
                "PUPAR",
                "PU",
                "pu",
                "preco",
                "Preço",
                "Preco",
            )
        )

        rows.append(
            {
                "site_id": int(asset_id),
                "data_referencia": data_ref,
                "nome": normalize_scalar(nome),
                "indice": normalize_scalar(indice),
                "valor_nominal": str(valor_nominal) if valor_nominal is not None else None,
                "juros": str(juros) if juros is not None else None,
                "pu": str(pu) if pu is not None else None,
                "raw_json": ensure_json_string(item),
            }
        )

    return rows


class FiduciarioService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.client = FiduciarioClient()
        self.ativos_repo = AtivoRepository(conn)
        self.historico_repo = PuHistoricoRepository(conn)

    def sync_daily_snapshot(self, today: str) -> list[int]:
        raw = self.client.get_daily_pu(today)
        print(f"[DEBUG] tipo retorno daily: {type(raw).__name__}")

        if isinstance(raw, dict):
            print(f"[DEBUG] chaves topo daily: {list(raw.keys())}")

        items = normalize_to_list(raw)
        print(f"[DEBUG] itens normalizados daily: {len(items)}")

        if items:
            print(f"[DEBUG] primeira chave item daily: {list(items[0].keys())}")

        asset_ids: list[int] = []

        for item in items:
            emissao = first_existing(
                item,
                "CODCETIP",
                "CODBOVESPA",
                "NOME",
                "nome",
            )

            detail_item: dict[str, Any] = {}
            if emissao:
                try:
                    detail_raw = self.client.get_asset_details(str(emissao))
                    detail_item = build_detail_lookup(detail_raw)
                except Exception as e:
                    print(f"[WARN] falha ao buscar detalhe do ativo emissao={emissao}: {e}")

            row = build_ativo_row(item, detail_item=detail_item)
            if row is None:
                continue

            self.ativos_repo.upsert(row)
            asset_ids.append(row["site_id"])

        self.conn.commit()
        return sorted(set(asset_ids))

    def backfill_history_for_asset(
        self,
        asset_id: int,
        start_date: str,
        end_date: str,
    ) -> int:
        raw = self.client.get_pu_history(asset_id, start_date, end_date)

        if isinstance(raw, dict):
            print(f"[DEBUG][HIST {asset_id}] chaves topo: {list(raw.keys())}")

        items = normalize_to_list(raw)

        if items:
            print(f"[DEBUG][HIST {asset_id}] primeira chave item: {list(items[0].keys())}")
        else:
            print(f"[DEBUG][HIST {asset_id}] nenhum item retornado")

        rows = build_history_rows(asset_id, items)
        print(f"[DEBUG][HIST {asset_id}] linhas montadas: {len(rows)}")

        if rows:
            print(f"[DEBUG][HIST {asset_id}] amostra linha montada: {rows[0]}")

        inserted = self.historico_repo.insert_many(rows)
        self.conn.commit()
        return inserted

    def backfill_history_for_assets(
        self,
        asset_ids: list[int],
        start_date: str,
        end_date: str,
    ) -> dict[str, int]:
        total_inserted = 0
        total_assets = 0

        for asset_id in asset_ids:
            inserted = self.backfill_history_for_asset(asset_id, start_date, end_date)
            total_inserted += inserted
            total_assets += 1
            print(
                f"[HIST] ativo_id={asset_id} | inseridos={inserted} | "
                f"intervalo={start_date} -> {end_date}"
            )

        return {
            "assets_processed": total_assets,
            "history_rows_inserted": total_inserted,
        }