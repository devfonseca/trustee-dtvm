from __future__ import annotations

from pathlib import Path
from typing import Any

import requests

from config import BASE_DIR, BASE_URL, DEFAULT_HEADERS, TIMEOUT


class FiduciarioClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.debug_dir = BASE_DIR / "debug"
        self.debug_dir.mkdir(parents=True, exist_ok=True)

    def _save_debug_response(self, name: str, response: requests.Response) -> None:
        path = self.debug_dir / name
        path.write_text(response.text, encoding="utf-8")

    def _get(self, params: dict[str, Any], debug_name: str | None = None) -> Any:
        import time
        from requests.exceptions import RequestException

        max_tentativas = 3
        
        for tentativa in range(max_tentativas):
            try:
                response = self.session.get(BASE_URL, params=params, timeout=TIMEOUT)
                response.raise_for_status()

                if debug_name:
                    self._save_debug_response(debug_name, response)

                text = response.text.strip()

                # 1) tenta parsear como JSON normalmente
                try:
                    return response.json()
                except Exception:
                    pass

                # 2) tenta parsear a string manualmente
                try:
                    import json
                    return json.loads(text)
                except Exception:
                    pass

                # 3) devolve texto puro
                return text

            except RequestException as e:
                print(f"[AVISO] Falha de conexão/timeout. Tentativa {tentativa + 1}/{max_tentativas}. Aguardando 5s...")
                time.sleep(5)  # Espera 5 segundos para o servidor respirar
                
                if tentativa == max_tentativas - 1:
                    print(f"[ERRO] O servidor não respondeu. Pulando requisição.")
                    return [] # Retorna uma lista vazia para o scraper não quebrar e ir para o próximo ativo

    def get_asset_details(self, emissao: str) -> Any:
        params = {
            "action": "buscarPorTitulo",
            "emissao": emissao,
        }
        safe_name = str(emissao).replace("/", "_").replace("\\", "_").replace(" ", "_")
        return self._get(params, debug_name=f"detalhe_{safe_name}.txt")

    def get_daily_pu(self, today: str) -> Any:
        params = {
            "action": "ConsultaPuHistoricoEmIntervaloDeDadas",
            "hoje": today,
        }
        return self._get(params, debug_name=f"daily_{today}.txt")

    def get_pu_history(self, asset_id: int, start_date: str, end_date: str) -> Any:
        params = {
            "action": "ConsultaPuHistoricoPorIdTituloEmIntervaloDeDadas",
            "id": asset_id,
            "inicio": start_date,
            "fim": end_date,
        }
        return self._get(
            params,
            debug_name=f"history_{asset_id}_{start_date}_{end_date}.txt",
        )

    def get_informativos(self, year: str | int | None = None) -> Any:
        params = {
            "action": "dataInformativos",
            "ano": "" if year is None else year,
        }
        return self._get(params, debug_name=f"informativos_{year or 'all'}.txt")

    def get_eventos_por_periodo(self, today: str) -> Any:
        params = {
            "action": "ConsultaCadastroEventosPorPeriodo",
            "hoje": today,
        }
        return self._get(params, debug_name=f"eventos_{today}.txt")