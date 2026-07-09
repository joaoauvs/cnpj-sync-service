"""
Testes unitários para o parser de argumentos do main.py.
"""

from __future__ import annotations

import pytest

from main import _build_arg_parser, _fmt_elapsed, _parse_snapshot_date


class TestWorkersFlags:
    def test_flags_documentados(self):
        args = _build_arg_parser().parse_args(
            ["--workers-download", "12", "--workers-extract", "4", "--workers-process", "6"]
        )
        assert args.download_workers == 12
        assert args.extract_workers == 4
        assert args.process_workers == 6

    def test_aliases_antigos_continuam_funcionando(self):
        args = _build_arg_parser().parse_args(
            ["--download-workers", "3", "--extract-workers", "2", "--process-workers", "1"]
        )
        assert args.download_workers == 3
        assert args.extract_workers == 2
        assert args.process_workers == 1

    def test_workers_zero_e_rejeitado(self):
        with pytest.raises(SystemExit):
            _build_arg_parser().parse_args(["--workers-download", "0"])

    def test_defaults_sao_none_para_flags_por_estagio(self):
        args = _build_arg_parser().parse_args([])
        assert args.download_workers is None
        assert args.extract_workers is None
        assert args.process_workers is None


class TestHelpers:
    def test_parse_snapshot_date_mes(self):
        assert _parse_snapshot_date("2026-04").isoformat() == "2026-04-01"

    def test_parse_snapshot_date_dia(self):
        assert _parse_snapshot_date("2026-04-15").isoformat() == "2026-04-15"

    def test_fmt_elapsed(self):
        assert _fmt_elapsed(45) == "45s"
        assert _fmt_elapsed(125) == "2min 05s"
        assert _fmt_elapsed(3725) == "1h 02min 05s"
