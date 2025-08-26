# app/cache/rules_cache.py

import time
from typing import List
from cachetools import TTLCache
from sqlalchemy.orm import Session

from app import crud
from app.core.config import settings
from app.schemas.rule import RuleSet as RuleSetSchema, RuleSetSummary
from app.db.models.rule import RuleSet as RuleSetORM

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = structlog.get_logger(__name__)

_RULESET_CACHE = TTLCache(maxsize=1, ttl=settings.RULES_CACHE_TTL_SECONDS)
_LAST_REFRESH_TS: float = 0

def get_active_rulesets(db: Session) -> List[RuleSetSchema]:
    global _LAST_REFRESH_TS

    if not settings.RULES_CACHE_ENABLED:
        logger.debug("[rules_cache] CACHE DISABLED — Fetching rules from DB")
        return _hydrate_rulesets(db)  # Fixed syntax error

    try:
        logger.debug("[rules_cache] CACHE HIT — Using cached rules")
        return _RULESET_CACHE["active"]
    except KeyError:
        logger.debug("[rules_cache] CACHE MISS — Fetching rules from DB")
        rulesets = _hydrate_rulesets(db)
        _RULESET_CACHE["active"] = rulesets
        _LAST_REFRESH_TS = time.time()
        return rulesets

def invalidate() -> None:
    logger.debug("[rules_cache] CACHE INVALIDATED MANUALLY")
    _RULESET_CACHE.pop("active", None)

def _hydrate_rulesets(db: Session) -> List[RuleSetSchema]:
    # This query in crud.ruleset.get_active_ordered already uses selectinload for RuleSet.rules
    rulesets_db: List[RuleSetORM] = crud.ruleset.get_active_ordered(db)
    result: List[RuleSetSchema] = []

    for rs_orm in rulesets_db:
        # The 'rs_orm' (RuleSetORM instance) now has a 'rule_count' property.
        # When RuleSetSchema.model_validate(rs_orm) is called, Pydantic will:
        # 1. Create a RuleSetSchema instance.
        # 2. For RuleSetSchema.rules (List[RuleSchema]):
        #    Iterate rs_orm.rules (List[RuleORM]). For each rule_orm:
        #    a. Create a RuleSchema instance.
        #    b. For RuleSchema.ruleset (Optional[RuleSetSummary]):
        #       It looks at rule_orm.ruleset (which is the parent rs_orm - a RuleSetORM instance).
        #       It then creates a RuleSetSummary from this rs_orm.
        #       The RuleSetSummary.rule_count field will be populated by calling rs_orm.rule_count (the new property).
        # 3. The RuleSetSchema.rule_count field will also be populated by calling rs_orm.rule_count.

        rs_schema = RuleSetSchema.model_validate(rs_orm) # from_attributes=True is implied
        result.append(rs_schema)

    return result