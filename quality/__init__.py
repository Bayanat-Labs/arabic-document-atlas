"""Quality checks for Arabic Document Atlas; no OCR correctness claims."""

from .validate import (
    QualityPolicy, audit_collection, canonical_ids, document_split,
    image_fingerprints, normalized_text_hash, text_metrics, validate_page,
)

__all__ = [
    "QualityPolicy", "audit_collection", "canonical_ids", "document_split",
    "image_fingerprints", "normalized_text_hash", "text_metrics", "validate_page",
]
