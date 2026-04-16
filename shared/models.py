from dataclasses import dataclass, field
from typing import Any
import uuid


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str] = field(default_factory=list)
    schema_version: str = ""


@dataclass
class Order:
    raw: dict[str, Any]
    schema_version: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Order":
        return cls(
            raw=data,
            schema_version=data.get("schema_version", "unknown"),
        )


@dataclass
class DLQMessage:
    dlq_id: str
    original_message: dict[str, Any]
    error_details: list[str]
    schema_version: str
    failed_at: str
    retry_count: int = 0
    resolved: bool = False

    @classmethod
    def create(cls, original: dict[str, Any], errors: list[str], schema_version: str, failed_at: str) -> "DLQMessage":
        return cls(
            dlq_id=str(uuid.uuid4()),
            original_message=original,
            error_details=errors,
            schema_version=schema_version,
            failed_at=failed_at,
            retry_count=0,
            resolved=False,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dlq_id": self.dlq_id,
            "original_message": self.original_message,
            "error_details": self.error_details,
            "schema_version": self.schema_version,
            "failed_at": self.failed_at,
            "retry_count": self.retry_count,
            "resolved": self.resolved,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DLQMessage":
        return cls(
            dlq_id=data["dlq_id"],
            original_message=data["original_message"],
            error_details=data["error_details"],
            schema_version=data["schema_version"],
            failed_at=data["failed_at"],
            retry_count=data.get("retry_count", 0),
            resolved=data.get("resolved", False),
        )
