from config import resolve_paper_book_size_unit


def test_book_depth_unit_stays_unknown_without_explicit_evidence():
    assert resolve_paper_book_size_unit("shares", "") == "unknown"
    assert resolve_paper_book_size_unit("lots", None) == "unknown"


def test_book_depth_unit_accepts_only_allowlisted_evidence_sources():
    assert resolve_paper_book_size_unit("shares", "FINAM_SUPPORT") == "shares"
    assert resolve_paper_book_size_unit("lots", "BROKER_EXPORT") == "lots"
    assert resolve_paper_book_size_unit("shares", "CONTROLLED_RECONCILIATION") == "shares"


def test_book_depth_unit_rejects_unknown_units_and_arbitrary_evidence():
    assert resolve_paper_book_size_unit("contracts", "FINAM_SUPPORT") == "unknown"
    assert resolve_paper_book_size_unit("shares", "operator_guess") == "unknown"
