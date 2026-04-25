"""Data quality rule registry sourced from the production extension."""

from eu_energy_intelligence.extension_bridge import _EXTENSION

PRICE_DQ_RULES = _EXTENSION.PRICE_DQ_RULES
GENERATION_DQ_RULES = _EXTENSION.GENERATION_DQ_RULES
LOAD_DQ_RULES = _EXTENSION.LOAD_DQ_RULES
FLOW_DQ_RULES = _EXTENSION.FLOW_DQ_RULES

DQ_RULE_REGISTRY: dict[str, list[dict[str, str]]] = _EXTENSION.DQ_RULE_REGISTRY
