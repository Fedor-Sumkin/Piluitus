from __future__ import annotations

import logging

from monitoring.durable_file_handler import DurableFileHandler


def _make_logger(path, *, max_bytes: int) -> tuple[logging.Logger, DurableFileHandler]:
    handler = DurableFileHandler(
        path,
        mode="x",
        encoding="utf-8",
        sync_interval_seconds=0.0,
        max_bytes=max_bytes,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger = logging.Logger(f"session-log-test-{path.name}", level=logging.INFO)
    logger.propagate = False
    logger.addHandler(handler)
    return logger, handler


def _close_logger(logger: logging.Logger, handler: DurableFileHandler) -> None:
    logger.removeHandler(handler)
    handler.close()


def test_session_log_rotates_before_record_exceeds_limit(tmp_path):
    path = tmp_path / "session_25112026_10.37.log"
    logger, handler = _make_logger(path, max_bytes=9)
    try:
        logger.info("1234567")
        logger.info("abcdefg")
        logger.info("xy")
    finally:
        _close_logger(logger, handler)

    part_two = tmp_path / "session_25112026_10.37_part02.log"
    part_three = tmp_path / "session_25112026_10.37_part03.log"
    assert path.read_text(encoding="utf-8") == "1234567\n"
    assert part_two.read_text(encoding="utf-8") == "abcdefg\n"
    assert part_three.read_text(encoding="utf-8") == "xy\n"
    assert path.stat().st_size <= 9
    assert part_two.stat().st_size <= 9
    assert part_three.stat().st_size <= 9


def test_rotation_uses_utf8_byte_size_not_character_count(tmp_path):
    path = tmp_path / "session_unicode.log"
    logger, handler = _make_logger(path, max_bytes=9)
    try:
        logger.info("абвг")
        logger.info("д")
    finally:
        _close_logger(logger, handler)

    part_two = tmp_path / "session_unicode_part02.log"
    assert path.read_text(encoding="utf-8") == "абвг\n"
    assert part_two.read_text(encoding="utf-8") == "д\n"
    assert path.stat().st_size == 9


def test_zero_rotation_limit_keeps_one_file(tmp_path):
    path = tmp_path / "diagnostic.log"
    logger, handler = _make_logger(path, max_bytes=0)
    try:
        logger.info("first")
        logger.info("second")
    finally:
        _close_logger(logger, handler)

    assert path.read_text(encoding="utf-8") == "first\nsecond\n"
    assert not (tmp_path / "diagnostic_part02.log").exists()


def test_rotation_skips_an_existing_part_without_overwriting_it(tmp_path):
    path = tmp_path / "session_existing.log"
    existing_part = tmp_path / "session_existing_part02.log"
    existing_part.write_text("keep me", encoding="utf-8")
    logger, handler = _make_logger(path, max_bytes=3)
    try:
        logger.info("a")
        logger.info("b")
    finally:
        _close_logger(logger, handler)

    part_three = tmp_path / "session_existing_part03.log"
    assert existing_part.read_text(encoding="utf-8") == "keep me"
    assert part_three.read_text(encoding="utf-8") == "b\n"
