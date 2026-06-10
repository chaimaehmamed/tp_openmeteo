"""
Module archive — sauvegarde des réponses JSON brutes sur disque.
Permet de rejouer la transformation sans rappeler l'API.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

ARCHIVE_BASE = "/tmp/weather_archive"


def get_archive_path(execution_date: str, run_id: str) -> str:
    safe_run_id = run_id.replace(":", "_").replace("+", "_")
    return os.path.join(ARCHIVE_BASE, execution_date, f"{safe_run_id}.json")


def save_archive(raw_responses: dict, execution_date: str, run_id: str) -> str:
    path = get_archive_path(execution_date, run_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(raw_responses, f, ensure_ascii=False, indent=2)
    logger.info(f"[archive] JSON brut sauvegardé : {path}")
    return path
