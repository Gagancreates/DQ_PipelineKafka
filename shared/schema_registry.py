import json
import os
import re
from datetime import datetime, timezone

import jsonschema

from .models import ValidationResult


class SchemaRegistry:
    def __init__(self, schemas_dir: str):
        self._schemas_dir = schemas_dir
        # version -> {json_schema, business_rules, registered_at}
        self._registry: dict[str, dict] = {}
        self._load_from_disk()

    # Loading

    def _load_from_disk(self) -> None:
        """Load all vN.json files from the schemas directory."""
        if not os.path.isdir(self._schemas_dir):
            return
        for fname in sorted(os.listdir(self._schemas_dir)):
            if fname.endswith(".json"):
                version = fname[:-5]  # strip .json
                fpath = os.path.join(self._schemas_dir, fname)
                with open(fpath, "r") as f:
                    schema_doc = json.load(f)
                self._register(version, schema_doc, source="disk")

    def _register(self, version: str, schema_doc: dict, source: str = "api") -> None:
        business_rules = schema_doc.pop("business_rules", {})
        self._registry[version] = {
            "json_schema": schema_doc,
            "business_rules": business_rules,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
        }

    # Public API

    def list_versions(self) -> list[str]:
        return sorted(self._registry.keys())

    def get_latest_version(self) -> str | None:
        versions = self.list_versions()
        return versions[-1] if versions else None

    def get_schema_info(self, version: str) -> dict | None:
        entry = self._registry.get(version)
        if not entry:
            return None
        return {
            "version": version,
            "registered_at": entry["registered_at"],
            "source": entry["source"],
            "business_rules": entry["business_rules"],
        }

    def register_version(self, version: str, schema_doc: dict) -> None:
        """Register a new schema version at runtime (called from API)."""
        self._register(version, schema_doc, source="api")

    def validate(self, message: dict) -> ValidationResult:
        version = message.get("schema_version", "unknown")
        entry = self._registry.get(version)

        if entry is None:
            return ValidationResult(
                is_valid=False,
                errors=[f"Unknown schema version: '{version}'"],
                schema_version=version,
            )

        errors: list[str] = []

        # Phase 1 — JSON Schema structural check
        try:
            jsonschema.validate(instance=message, schema=entry["json_schema"])
        except jsonschema.ValidationError as e:
            errors.append(f"Schema error: {e.message}")
        except jsonschema.SchemaError as e:
            errors.append(f"Invalid schema definition: {e.message}")

        # Phase 2 — Business rules
        rules = entry["business_rules"]
        errors.extend(self._apply_business_rules(message, rules))

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            schema_version=version,
        )

    def validate_against_latest(self, message: dict) -> ValidationResult:
        """Revalidate a message against the latest registered schema version."""
        latest = self.get_latest_version()
        if latest is None:
            return ValidationResult(is_valid=False, errors=["No schemas registered"], schema_version="unknown")
        patched = {**message, "schema_version": latest}
        return self.validate(patched)

    # Business Rules

    def _apply_business_rules(self, message: dict, rules: dict) -> list[str]:
        errors: list[str] = []

        # order_id pattern
        pattern = rules.get("order_id_pattern")
        order_id = message.get("order_id", "")
        if pattern and order_id and not re.match(pattern, str(order_id)):
            errors.append(f"order_id '{order_id}' does not match pattern {pattern}")

        # customer_id non-empty (structural check catches missing, this catches empty string)
        customer_id = message.get("customer_id", None)
        if customer_id is not None and str(customer_id).strip() == "":
            errors.append("customer_id must not be empty")

        # amount range
        amount = message.get("amount", None)
        if amount is not None:
            amount_min = rules.get("amount_min")
            amount_max = rules.get("amount_max")
            if amount_min is not None and amount <= amount_min:
                errors.append(f"amount must be > {amount_min}, got {amount}")
            if amount_max is not None and amount > amount_max:
                errors.append(f"amount must be <= {amount_max}, got {amount}")

        # order_date not in future
        if rules.get("order_date_not_future"):
            order_date_str = message.get("order_date", "")
            if order_date_str:
                try:
                    order_date = datetime.fromisoformat(
                        order_date_str.replace("Z", "+00:00")
                    )
                    if order_date > datetime.now(timezone.utc):
                        errors.append(f"order_date '{order_date_str}' is in the future")
                except ValueError:
                    errors.append(f"order_date '{order_date_str}' is not a valid ISO 8601 datetime")

        # currency whitelist
        allowed = rules.get("allowed_currencies")
        if allowed is not None:
            currency = message.get("currency", None)
            if currency is not None and currency not in allowed:
                errors.append(f"currency '{currency}' not allowed; must be one of {allowed}")

        # discount_pct range (v3+)
        discount = message.get("discount_pct", None)
        if discount is not None:
            d_min = rules.get("discount_pct_min", 0)
            d_max = rules.get("discount_pct_max", 100)
            if not (d_min <= discount <= d_max):
                errors.append(f"discount_pct must be between {d_min} and {d_max}, got {discount}")

        return errors
