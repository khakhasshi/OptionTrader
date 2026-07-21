"""Advisory Phase 4 LLM intelligence layer.

This package deliberately has no broker or execution imports. It transforms
allowlisted, structured records into auditable explanations and research-only
hypotheses. Rust remains the only final trading authority.
"""

from app.llm.models import (
    DailyReviewDetail,
    EvidenceCitation,
    LLMReview,
    LLMReviewContent,
    LLMReviewRequest,
    ProviderMetadata,
    ReviewContext,
    RuleHypothesis,
    RuleHypothesisRecord,
    SourceReference,
)

__all__ = [
    "DailyReviewDetail",
    "EvidenceCitation",
    "LLMReview",
    "LLMReviewContent",
    "LLMReviewRequest",
    "ProviderMetadata",
    "ReviewContext",
    "RuleHypothesis",
    "RuleHypothesisRecord",
    "SourceReference",
]
