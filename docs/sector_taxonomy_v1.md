# Sector Taxonomy v1

- Version: `v1`
- Scope: stock-to-sector mapping for KR/US universe
- Mapping cardinality: `1 stock : N sectors (N >= 1)`

## Sector List

1. `INFORMATION_TECHNOLOGY`
2. `SEMICONDUCTORS`
3. `FINANCIALS`
4. `HEALTH_CARE`
5. `ENERGY`
6. `MATERIALS`
7. `CONSUMER_DISCRETIONARY`
8. `CONSUMER_STAPLES`
9. `INDUSTRIALS`
10. `COMMUNICATION_SERVICES`
11. `PLATFORM_IT`
12. `UTILITIES`
13. `REAL_ESTATE`
14. `UNCLASSIFIED`

## Mapping Policy

1. KR symbols: use Naver upjong mapping first.
2. If KR source is unavailable or stock code is missing, fallback to rule-based mapping.
3. US symbols: currently rule-based mapping.
4. Keep all sectors whose confidence is above threshold.
5. Limit to top `N` sectors by confidence (`max_sectors`).
6. Always return at least one sector (`UNCLASSIFIED` fallback).

## Notes

1. KR external source URL:
   `https://finance.naver.com/sise/sise_group.naver?type=upjong`
2. This is a deterministic baseline for M2-T01.
3. LLM-assisted mapping refinement is planned in later tickets.
