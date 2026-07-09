"""
Testes unitários para src/pipeline.py — sem rede.
"""

from __future__ import annotations

from src.models import Snapshot
from src.pipeline import CNPJPipeline


def _empty_snapshot() -> Snapshot:
    return Snapshot(date="2026-01", url="https://example.com/2026-01/", files=[])


class TestWorkerPropagation:
    def test_run_propaga_workers_para_cada_estagio(self):
        pipeline = CNPJPipeline()
        run = pipeline.run(
            snapshot=_empty_snapshot(),
            download_workers=11,
            extract_workers=7,
            process_workers=5,
        )

        assert pipeline.downloader.workers == 11
        assert pipeline.extractor.workers == 7
        assert pipeline.processor.workers == 5
        assert run.results == []
        assert run.finished_at is not None

    def test_snapshot_explicito_dispensa_descoberta(self):
        """Com snapshot fornecido, o run termina sem tocar a rede."""
        run = CNPJPipeline().run(snapshot=_empty_snapshot())
        assert run.snapshot_date == "2026-01"
