from __future__ import annotations

import argparse

from config import DEFAULT_BACKFILL_START, RECENT_DAYS_WINDOW, TODAY
from db import get_connection, init_db
from services import FiduciarioService
from utils import days_ago_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scraper de PU diário e histórico do site fiduciario.com.br"
    )

    parser.add_argument(
        "--mode",
        choices=["full", "daily", "history"],
        default="full",
        help=(
            "full = snapshot diário + histórico completo; "
            "daily = só snapshot diário + janela recente; "
            "history = só histórico"
        ),
    )

    parser.add_argument(
        "--today",
        default=TODAY,
        help="Data de referência do snapshot diário no formato YYYY-MM-DD",
    )

    parser.add_argument(
        "--start-date",
        default=DEFAULT_BACKFILL_START,
        help="Data inicial do histórico no formato YYYY-MM-DD",
    )

    parser.add_argument(
        "--end-date",
        default=TODAY,
        help="Data final do histórico no formato YYYY-MM-DD",
    )

    parser.add_argument(
        "--asset-id",
        type=int,
        default=None,
        help="Se informado em --mode history, processa apenas este ativo",
    )

    parser.add_argument(
        "--recent-days",
        type=int,
        default=RECENT_DAYS_WINDOW,
        help="Janela de dias usada no modo daily para atualização recente",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    conn = get_connection()
    init_db(conn)

    service = FiduciarioService(conn)

    if args.mode == "full":
        print(f"[DAILY] coletando snapshot do dia {args.today}")
        asset_ids = service.sync_daily_snapshot(args.today)
        print(f"[DAILY] ativos encontrados: {len(asset_ids)}")

        print(
            f"[HIST] backfill completo | inicio={args.start_date} | fim={args.end_date}"
        )
        result = service.backfill_history_for_assets(
            asset_ids=asset_ids,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print(f"[FIM] resumo: {result}")

    elif args.mode == "daily":
        print(f"[DAILY] coletando snapshot do dia {args.today}")
        asset_ids = service.sync_daily_snapshot(args.today)
        print(f"[DAILY] ativos encontrados: {len(asset_ids)}")

        recent_start = days_ago_iso(args.recent_days)
        print(
            f"[HIST] atualização recente | inicio={recent_start} | fim={args.end_date}"
        )
        result = service.backfill_history_for_assets(
            asset_ids=asset_ids,
            start_date=recent_start,
            end_date=args.end_date,
        )
        print(f"[FIM] resumo: {result}")

    elif args.mode == "history":
        if args.asset_id is not None:
            print(
                f"[HIST] ativo único={args.asset_id} | "
                f"inicio={args.start_date} | fim={args.end_date}"
            )
            inserted = service.backfill_history_for_asset(
                asset_id=args.asset_id,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            print(f"[FIM] linhas inseridas: {inserted}")
        else:
            # Se quiser rodar histórico de todos os ativos já cadastrados
            asset_ids = service.ativos_repo.list_all_ids()
            print(f"[HIST] ativos cadastrados no banco: {len(asset_ids)}")
            result = service.backfill_history_for_assets(
                asset_ids=asset_ids,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            print(f"[FIM] resumo: {result}")

    conn.close()


if __name__ == "__main__":
    main()