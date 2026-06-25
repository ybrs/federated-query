# DuckDB Reconstruction Failing Use Cases

This document records 1000 concrete SQL use cases that are not covered by the current passing reconstruction tests and fail native reconstruction through the REPL-style path.

Each case was generated and checked by executing the original SQL in DuckDB, reconstructing with `reconstruct_sql(..., root=None)`, executing the reconstructed SQL, and recording either a reconstruction error or a row/column mismatch.

## Summary

- `root-derived-auto-schema-limit` / `row-or-column-mismatch`: 1000

## Cases

### 1. `root-derived-auto-schema-limit` `row-or-column-mismatch` `64b71fd88a`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `1`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 1) T
```

### 2. `root-derived-auto-schema-limit` `row-or-column-mismatch` `657be84c96`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `2`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 2) T
```

### 3. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bb23d06cdf`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `3`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 3) T
```

### 4. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3ffbc6419d`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `4`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 4) T
```

### 5. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c6146f1d3b`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `5`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 5) T
```

### 6. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e2abb62f04`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `6`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 6) T
```

### 7. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0ac93702d9`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `7`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 7) T
```

### 8. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d24b150d95`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `8`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 8) T
```

### 9. `root-derived-auto-schema-limit` `row-or-column-mismatch` `25cb2b08ba`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `9`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 9) T
```

### 10. `root-derived-auto-schema-limit` `row-or-column-mismatch` `63d689b9b4`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `10`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 10) T
```

### 11. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6a16cb5b40`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `11`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 11) T
```

### 12. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e15e864c64`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `12`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 12) T
```

### 13. `root-derived-auto-schema-limit` `row-or-column-mismatch` `71964f1f72`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `13`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 13) T
```

### 14. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b64cb1d3e3`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `14`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 14) T
```

### 15. `root-derived-auto-schema-limit` `row-or-column-mismatch` `39042218a2`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `15`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 15) T
```

### 16. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4efb2e5f7a`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `16`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 16) T
```

### 17. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1a905de9e7`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `17`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 17) T
```

### 18. `root-derived-auto-schema-limit` `row-or-column-mismatch` `72f98e3a4a`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `18`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 18) T
```

### 19. `root-derived-auto-schema-limit` `row-or-column-mismatch` `40035dd08b`

- Auto schema: `True`
- Expected columns: `('b_1',)`
- Expected row count: `19`
- Actual columns: `('b_1',)`
- Actual row count: `20`

```sql
-- query
SELECT b_1 FROM (SELECT TRUE AS b_1 FROM dual LIMIT 19) T
```

### 20. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d2808aac1e`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `1`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 1) T
```

### 21. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f03892a9d1`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `2`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 2) T
```

### 22. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c2664d5ff8`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `3`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 3) T
```

### 23. `root-derived-auto-schema-limit` `row-or-column-mismatch` `142f2328f5`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `4`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 4) T
```

### 24. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f1d98178b5`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `5`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 5) T
```

### 25. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dd1d6f8d63`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `6`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 6) T
```

### 26. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a431c66744`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `7`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 7) T
```

### 27. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf37208d05`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `8`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 8) T
```

### 28. `root-derived-auto-schema-limit` `row-or-column-mismatch` `445dc91522`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `9`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 9) T
```

### 29. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bc6f7df44f`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `10`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 10) T
```

### 30. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1e16210cde`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `11`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 11) T
```

### 31. `root-derived-auto-schema-limit` `row-or-column-mismatch` `55b5218c8c`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `12`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 12) T
```

### 32. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dfea906514`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `13`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 13) T
```

### 33. `root-derived-auto-schema-limit` `row-or-column-mismatch` `25df13ef61`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `14`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 14) T
```

### 34. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9c527c6494`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `15`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 15) T
```

### 35. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e9e5c7bd8b`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `16`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 16) T
```

### 36. `root-derived-auto-schema-limit` `row-or-column-mismatch` `895b57b557`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `17`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 17) T
```

### 37. `root-derived-auto-schema-limit` `row-or-column-mismatch` `011dfffbac`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `18`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 18) T
```

### 38. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c2d2195f44`

- Auto schema: `True`
- Expected columns: `('b_2',)`
- Expected row count: `19`
- Actual columns: `('b_2',)`
- Actual row count: `20`

```sql
-- query
SELECT b_2 FROM (SELECT TRUE AS b_2 FROM dual LIMIT 19) T
```

### 39. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b477931b9b`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `1`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 1) T
```

### 40. `root-derived-auto-schema-limit` `row-or-column-mismatch` `70cbc1ccb6`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `2`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 2) T
```

### 41. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e1607380c0`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `3`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 3) T
```

### 42. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4928a1ff79`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `4`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 4) T
```

### 43. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e5b8529245`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `5`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 5) T
```

### 44. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8398739987`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `6`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 6) T
```

### 45. `root-derived-auto-schema-limit` `row-or-column-mismatch` `becb5df63c`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `7`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 7) T
```

### 46. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2b32e64409`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `8`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 8) T
```

### 47. `root-derived-auto-schema-limit` `row-or-column-mismatch` `81fb65b592`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `9`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 9) T
```

### 48. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a8b9d1832e`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `10`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 10) T
```

### 49. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ae729c91ef`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `11`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 11) T
```

### 50. `root-derived-auto-schema-limit` `row-or-column-mismatch` `43f2a442d9`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `12`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 12) T
```

### 51. `root-derived-auto-schema-limit` `row-or-column-mismatch` `95664c0ee7`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `13`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 13) T
```

### 52. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5e010ca005`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `14`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 14) T
```

### 53. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0bc6bbb6d9`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `15`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 15) T
```

### 54. `root-derived-auto-schema-limit` `row-or-column-mismatch` `32336bcbd5`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `16`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 16) T
```

### 55. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e7956a2724`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `17`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 17) T
```

### 56. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7e53bfa6c5`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `18`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 18) T
```

### 57. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b4ced1a0e9`

- Auto schema: `True`
- Expected columns: `('b_3',)`
- Expected row count: `19`
- Actual columns: `('b_3',)`
- Actual row count: `20`

```sql
-- query
SELECT b_3 FROM (SELECT TRUE AS b_3 FROM dual LIMIT 19) T
```

### 58. `root-derived-auto-schema-limit` `row-or-column-mismatch` `165994e996`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `1`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 1) T
```

### 59. `root-derived-auto-schema-limit` `row-or-column-mismatch` `07588cc29b`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `2`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 2) T
```

### 60. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e93ebf762b`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `3`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 3) T
```

### 61. `root-derived-auto-schema-limit` `row-or-column-mismatch` `efd39b1ca6`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `4`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 4) T
```

### 62. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c63de230d8`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `5`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 5) T
```

### 63. `root-derived-auto-schema-limit` `row-or-column-mismatch` `af1d0c267e`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `6`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 6) T
```

### 64. `root-derived-auto-schema-limit` `row-or-column-mismatch` `40cfe44461`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `7`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 7) T
```

### 65. `root-derived-auto-schema-limit` `row-or-column-mismatch` `30de9ac282`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `8`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 8) T
```

### 66. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fd786f763e`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `9`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 9) T
```

### 67. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1cb3948745`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `10`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 10) T
```

### 68. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b551ba581b`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `11`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 11) T
```

### 69. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6915e8bb6a`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `12`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 12) T
```

### 70. `root-derived-auto-schema-limit` `row-or-column-mismatch` `82ef6cc551`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `13`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 13) T
```

### 71. `root-derived-auto-schema-limit` `row-or-column-mismatch` `12dc47f673`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `14`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 14) T
```

### 72. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5bc768683e`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `15`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 15) T
```

### 73. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3c93a0be0e`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `16`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 16) T
```

### 74. `root-derived-auto-schema-limit` `row-or-column-mismatch` `36d3bab520`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `17`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 17) T
```

### 75. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2c24df033a`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `18`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 18) T
```

### 76. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9d7cc883e2`

- Auto schema: `True`
- Expected columns: `('b_4',)`
- Expected row count: `19`
- Actual columns: `('b_4',)`
- Actual row count: `20`

```sql
-- query
SELECT b_4 FROM (SELECT TRUE AS b_4 FROM dual LIMIT 19) T
```

### 77. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1f2d0c90c2`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `1`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 1) T
```

### 78. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a419c91681`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `2`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 2) T
```

### 79. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4ef799011a`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `3`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 3) T
```

### 80. `root-derived-auto-schema-limit` `row-or-column-mismatch` `61d2855b0b`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `4`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 4) T
```

### 81. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6f19f69555`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `5`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 5) T
```

### 82. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b042852720`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `6`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 6) T
```

### 83. `root-derived-auto-schema-limit` `row-or-column-mismatch` `10e994da5f`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `7`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 7) T
```

### 84. `root-derived-auto-schema-limit` `row-or-column-mismatch` `908fdccf3e`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `8`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 8) T
```

### 85. `root-derived-auto-schema-limit` `row-or-column-mismatch` `07861df9f3`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `9`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 9) T
```

### 86. `root-derived-auto-schema-limit` `row-or-column-mismatch` `78072a428a`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `10`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 10) T
```

### 87. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7ceacf41d7`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `11`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 11) T
```

### 88. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6ba323dc47`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `12`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 12) T
```

### 89. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4db12bd95f`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `13`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 13) T
```

### 90. `root-derived-auto-schema-limit` `row-or-column-mismatch` `69081f91df`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `14`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 14) T
```

### 91. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2909202a50`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `15`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 15) T
```

### 92. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f00f540da0`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `16`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 16) T
```

### 93. `root-derived-auto-schema-limit` `row-or-column-mismatch` `54edcf27a1`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `17`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 17) T
```

### 94. `root-derived-auto-schema-limit` `row-or-column-mismatch` `05cd25c6c1`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `18`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 18) T
```

### 95. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8f46d3621d`

- Auto schema: `True`
- Expected columns: `('b_5',)`
- Expected row count: `19`
- Actual columns: `('b_5',)`
- Actual row count: `20`

```sql
-- query
SELECT b_5 FROM (SELECT TRUE AS b_5 FROM dual LIMIT 19) T
```

### 96. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f1be3281e1`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `1`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 1) T
```

### 97. `root-derived-auto-schema-limit` `row-or-column-mismatch` `20943c83fa`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `2`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 2) T
```

### 98. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b19abb6b6b`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `3`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 3) T
```

### 99. `root-derived-auto-schema-limit` `row-or-column-mismatch` `10d5efdf65`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `4`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 4) T
```

### 100. `root-derived-auto-schema-limit` `row-or-column-mismatch` `62752be2e3`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `5`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 5) T
```

### 101. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9785277d3d`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `6`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 6) T
```

### 102. `root-derived-auto-schema-limit` `row-or-column-mismatch` `84c15d4cdd`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `7`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 7) T
```

### 103. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0747c6bd9c`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `8`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 8) T
```

### 104. `root-derived-auto-schema-limit` `row-or-column-mismatch` `80fa8a66da`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `9`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 9) T
```

### 105. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e3b94b12b9`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `10`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 10) T
```

### 106. `root-derived-auto-schema-limit` `row-or-column-mismatch` `61919194de`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `11`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 11) T
```

### 107. `root-derived-auto-schema-limit` `row-or-column-mismatch` `27b770c29b`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `12`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 12) T
```

### 108. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d86f41c90d`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `13`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 13) T
```

### 109. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a6c51fd5cc`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `14`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 14) T
```

### 110. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc1f0d552c`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `15`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 15) T
```

### 111. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6cfdb23ce5`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `16`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 16) T
```

### 112. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bb67b6b020`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `17`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 17) T
```

### 113. `root-derived-auto-schema-limit` `row-or-column-mismatch` `809ad7859c`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `18`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 18) T
```

### 114. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7040de26d6`

- Auto schema: `True`
- Expected columns: `('b_6',)`
- Expected row count: `19`
- Actual columns: `('b_6',)`
- Actual row count: `20`

```sql
-- query
SELECT b_6 FROM (SELECT TRUE AS b_6 FROM dual LIMIT 19) T
```

### 115. `root-derived-auto-schema-limit` `row-or-column-mismatch` `812bd4a063`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `1`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 1) T
```

### 116. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4b44675af0`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `2`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 2) T
```

### 117. `root-derived-auto-schema-limit` `row-or-column-mismatch` `88f1c5d24e`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `3`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 3) T
```

### 118. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0527924776`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `4`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 4) T
```

### 119. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e10c550005`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `5`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 5) T
```

### 120. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eb03366326`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `6`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 6) T
```

### 121. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d1cdfc35a4`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `7`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 7) T
```

### 122. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cb12c27e86`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `8`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 8) T
```

### 123. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eee5c3d663`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `9`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 9) T
```

### 124. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c4edf16fcf`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `10`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 10) T
```

### 125. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3f72a81bba`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `11`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 11) T
```

### 126. `root-derived-auto-schema-limit` `row-or-column-mismatch` `09cb0c1677`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `12`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 12) T
```

### 127. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2a72a2e3ca`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `13`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 13) T
```

### 128. `root-derived-auto-schema-limit` `row-or-column-mismatch` `99cbd554fc`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `14`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 14) T
```

### 129. `root-derived-auto-schema-limit` `row-or-column-mismatch` `547cab83b5`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `15`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 15) T
```

### 130. `root-derived-auto-schema-limit` `row-or-column-mismatch` `93c69b0e0a`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `16`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 16) T
```

### 131. `root-derived-auto-schema-limit` `row-or-column-mismatch` `61cf14bac4`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `17`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 17) T
```

### 132. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6541f8fe0f`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `18`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 18) T
```

### 133. `root-derived-auto-schema-limit` `row-or-column-mismatch` `41e635d8be`

- Auto schema: `True`
- Expected columns: `('b_7',)`
- Expected row count: `19`
- Actual columns: `('b_7',)`
- Actual row count: `20`

```sql
-- query
SELECT b_7 FROM (SELECT TRUE AS b_7 FROM dual LIMIT 19) T
```

### 134. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e276b394cc`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `1`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 1) T
```

### 135. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cc2932b2cb`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `2`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 2) T
```

### 136. `root-derived-auto-schema-limit` `row-or-column-mismatch` `92e6c1c00f`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `3`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 3) T
```

### 137. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f5d01bbe7e`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `4`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 4) T
```

### 138. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c96931dda6`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `5`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 5) T
```

### 139. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8e4786e90a`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `6`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 6) T
```

### 140. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c56fb87a8f`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `7`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 7) T
```

### 141. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1d8314ee6f`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `8`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 8) T
```

### 142. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1687c62b76`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `9`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 9) T
```

### 143. `root-derived-auto-schema-limit` `row-or-column-mismatch` `52ffc62f8d`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `10`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 10) T
```

### 144. `root-derived-auto-schema-limit` `row-or-column-mismatch` `94e3dd81ad`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `11`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 11) T
```

### 145. `root-derived-auto-schema-limit` `row-or-column-mismatch` `151efa6323`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `12`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 12) T
```

### 146. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9c34a84039`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `13`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 13) T
```

### 147. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a8999459dd`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `14`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 14) T
```

### 148. `root-derived-auto-schema-limit` `row-or-column-mismatch` `23a2a2accb`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `15`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 15) T
```

### 149. `root-derived-auto-schema-limit` `row-or-column-mismatch` `76b8a5c79a`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `16`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 16) T
```

### 150. `root-derived-auto-schema-limit` `row-or-column-mismatch` `84141b3ec7`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `17`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 17) T
```

### 151. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2da1ad5a2c`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `18`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 18) T
```

### 152. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8313acb1c3`

- Auto schema: `True`
- Expected columns: `('b_8',)`
- Expected row count: `19`
- Actual columns: `('b_8',)`
- Actual row count: `20`

```sql
-- query
SELECT b_8 FROM (SELECT TRUE AS b_8 FROM dual LIMIT 19) T
```

### 153. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d763954f70`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `1`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 1) T
```

### 154. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8b7f021fdc`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `2`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 2) T
```

### 155. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7de57db698`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `3`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 3) T
```

### 156. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0acde67679`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `4`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 4) T
```

### 157. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f4138a5fab`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `5`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 5) T
```

### 158. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4b50bcb29f`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `6`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 6) T
```

### 159. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e7b9f32f35`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `7`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 7) T
```

### 160. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f615967e62`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `8`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 8) T
```

### 161. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f552399d6e`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `9`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 9) T
```

### 162. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3520d2be8e`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `10`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 10) T
```

### 163. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f4ab64265d`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `11`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 11) T
```

### 164. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dd6cd94013`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `12`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 12) T
```

### 165. `root-derived-auto-schema-limit` `row-or-column-mismatch` `55041f332a`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `13`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 13) T
```

### 166. `root-derived-auto-schema-limit` `row-or-column-mismatch` `559400c1ff`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `14`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 14) T
```

### 167. `root-derived-auto-schema-limit` `row-or-column-mismatch` `27dc7efe0b`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `15`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 15) T
```

### 168. `root-derived-auto-schema-limit` `row-or-column-mismatch` `05966c9773`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `16`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 16) T
```

### 169. `root-derived-auto-schema-limit` `row-or-column-mismatch` `624f3b92a6`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `17`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 17) T
```

### 170. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f5b03a455c`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `18`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 18) T
```

### 171. `root-derived-auto-schema-limit` `row-or-column-mismatch` `32d86ac6d2`

- Auto schema: `True`
- Expected columns: `('b_9',)`
- Expected row count: `19`
- Actual columns: `('b_9',)`
- Actual row count: `20`

```sql
-- query
SELECT b_9 FROM (SELECT TRUE AS b_9 FROM dual LIMIT 19) T
```

### 172. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5b656d11db`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `1`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 1) T
```

### 173. `root-derived-auto-schema-limit` `row-or-column-mismatch` `52548dfb2b`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `2`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 2) T
```

### 174. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b39bf121b4`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `3`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 3) T
```

### 175. `root-derived-auto-schema-limit` `row-or-column-mismatch` `10e536ad5f`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `4`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 4) T
```

### 176. `root-derived-auto-schema-limit` `row-or-column-mismatch` `041019e5d7`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `5`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 5) T
```

### 177. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1e6b2ad692`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `6`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 6) T
```

### 178. `root-derived-auto-schema-limit` `row-or-column-mismatch` `96e025951e`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `7`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 7) T
```

### 179. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a6971d2825`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `8`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 8) T
```

### 180. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bf34f25da7`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `9`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 9) T
```

### 181. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d9202ddab1`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `10`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 10) T
```

### 182. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6c99078f7c`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `11`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 11) T
```

### 183. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ea3a2fd674`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `12`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 12) T
```

### 184. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f1a4f76467`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `13`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 13) T
```

### 185. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4a87570ef4`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `14`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 14) T
```

### 186. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d99ab92e24`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `15`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 15) T
```

### 187. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1e34d72424`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `16`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 16) T
```

### 188. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0639b0429d`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `17`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 17) T
```

### 189. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7b17bec520`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `18`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 18) T
```

### 190. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bba4fc1d52`

- Auto schema: `True`
- Expected columns: `('b_10',)`
- Expected row count: `19`
- Actual columns: `('b_10',)`
- Actual row count: `20`

```sql
-- query
SELECT b_10 FROM (SELECT TRUE AS b_10 FROM dual LIMIT 19) T
```

### 191. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3758b93f13`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `1`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 1) T
```

### 192. `root-derived-auto-schema-limit` `row-or-column-mismatch` `967d30bf02`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `2`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 2) T
```

### 193. `root-derived-auto-schema-limit` `row-or-column-mismatch` `89cf9f07d9`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `3`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 3) T
```

### 194. `root-derived-auto-schema-limit` `row-or-column-mismatch` `db91e263d9`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `4`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 4) T
```

### 195. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d3987c3fb8`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `5`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 5) T
```

### 196. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6a964e92c1`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `6`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 6) T
```

### 197. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8cfbdafb00`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `7`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 7) T
```

### 198. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b3b76fcfd2`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `8`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 8) T
```

### 199. `root-derived-auto-schema-limit` `row-or-column-mismatch` `331d52bee2`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `9`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 9) T
```

### 200. `root-derived-auto-schema-limit` `row-or-column-mismatch` `74947ac2a8`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `10`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 10) T
```

### 201. `root-derived-auto-schema-limit` `row-or-column-mismatch` `20d3662f3b`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `11`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 11) T
```

### 202. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2d8d634d44`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `12`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 12) T
```

### 203. `root-derived-auto-schema-limit` `row-or-column-mismatch` `87a1908a5f`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `13`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 13) T
```

### 204. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c76e6b73ce`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `14`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 14) T
```

### 205. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1a99106493`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `15`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 15) T
```

### 206. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5ac0c944b8`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `16`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 16) T
```

### 207. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0aa9106c13`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `17`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 17) T
```

### 208. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d95f5c888f`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `18`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 18) T
```

### 209. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ed1e7dbcfd`

- Auto schema: `True`
- Expected columns: `('b_11',)`
- Expected row count: `19`
- Actual columns: `('b_11',)`
- Actual row count: `20`

```sql
-- query
SELECT b_11 FROM (SELECT TRUE AS b_11 FROM dual LIMIT 19) T
```

### 210. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c1474da091`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `1`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 1) T
```

### 211. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f01e92fc69`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `2`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 2) T
```

### 212. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c600d263f8`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `3`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 3) T
```

### 213. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2c95d65644`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `4`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 4) T
```

### 214. `root-derived-auto-schema-limit` `row-or-column-mismatch` `76f8d4cf25`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `5`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 5) T
```

### 215. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9c6083aef1`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `6`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 6) T
```

### 216. `root-derived-auto-schema-limit` `row-or-column-mismatch` `06371a461f`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `7`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 7) T
```

### 217. `root-derived-auto-schema-limit` `row-or-column-mismatch` `21fab1b14a`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `8`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 8) T
```

### 218. `root-derived-auto-schema-limit` `row-or-column-mismatch` `45ad9ff205`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `9`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 9) T
```

### 219. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8dcc35081e`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `10`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 10) T
```

### 220. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2d7c58bafc`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `11`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 11) T
```

### 221. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b11f55eb83`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `12`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 12) T
```

### 222. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f2935d7fe8`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `13`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 13) T
```

### 223. `root-derived-auto-schema-limit` `row-or-column-mismatch` `38df45d3ed`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `14`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 14) T
```

### 224. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ce8b89e3d9`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `15`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 15) T
```

### 225. `root-derived-auto-schema-limit` `row-or-column-mismatch` `806f59cf20`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `16`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 16) T
```

### 226. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b62f39d10b`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `17`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 17) T
```

### 227. `root-derived-auto-schema-limit` `row-or-column-mismatch` `641da615be`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `18`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 18) T
```

### 228. `root-derived-auto-schema-limit` `row-or-column-mismatch` `041af80d75`

- Auto schema: `True`
- Expected columns: `('b_12',)`
- Expected row count: `19`
- Actual columns: `('b_12',)`
- Actual row count: `20`

```sql
-- query
SELECT b_12 FROM (SELECT TRUE AS b_12 FROM dual LIMIT 19) T
```

### 229. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d4908546e8`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `1`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 1) T
```

### 230. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2f04f3267b`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `2`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 2) T
```

### 231. `root-derived-auto-schema-limit` `row-or-column-mismatch` `985eb2b231`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `3`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 3) T
```

### 232. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a4f5e5f82e`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `4`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 4) T
```

### 233. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0de7ceb048`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `5`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 5) T
```

### 234. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a32bc894f8`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `6`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 6) T
```

### 235. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5c8d9ca2c3`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `7`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 7) T
```

### 236. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7787ae8b56`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `8`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 8) T
```

### 237. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7076177737`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `9`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 9) T
```

### 238. `root-derived-auto-schema-limit` `row-or-column-mismatch` `62538aaa81`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `10`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 10) T
```

### 239. `root-derived-auto-schema-limit` `row-or-column-mismatch` `90e47165df`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `11`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 11) T
```

### 240. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf95b158cb`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `12`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 12) T
```

### 241. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6719ece0bb`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `13`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 13) T
```

### 242. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6f235bbb1d`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `14`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 14) T
```

### 243. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f257e598fb`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `15`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 15) T
```

### 244. `root-derived-auto-schema-limit` `row-or-column-mismatch` `99ab9e58de`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `16`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 16) T
```

### 245. `root-derived-auto-schema-limit` `row-or-column-mismatch` `316ad07f49`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `17`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 17) T
```

### 246. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cd93a9ed53`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `18`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 18) T
```

### 247. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e5c7a1840d`

- Auto schema: `True`
- Expected columns: `('b_13',)`
- Expected row count: `19`
- Actual columns: `('b_13',)`
- Actual row count: `20`

```sql
-- query
SELECT b_13 FROM (SELECT TRUE AS b_13 FROM dual LIMIT 19) T
```

### 248. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5e97c1c25e`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `1`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 1) T
```

### 249. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4d38f81019`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `2`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 2) T
```

### 250. `root-derived-auto-schema-limit` `row-or-column-mismatch` `86506d10ba`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `3`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 3) T
```

### 251. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8846d02399`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `4`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 4) T
```

### 252. `root-derived-auto-schema-limit` `row-or-column-mismatch` `801c127d0b`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `5`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 5) T
```

### 253. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b728ed2452`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `6`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 6) T
```

### 254. `root-derived-auto-schema-limit` `row-or-column-mismatch` `78c245fd71`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `7`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 7) T
```

### 255. `root-derived-auto-schema-limit` `row-or-column-mismatch` `45f5bba1a0`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `8`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 8) T
```

### 256. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8d503d4a65`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `9`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 9) T
```

### 257. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7a14a608ef`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `10`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 10) T
```

### 258. `root-derived-auto-schema-limit` `row-or-column-mismatch` `588e70c263`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `11`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 11) T
```

### 259. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d362f2abca`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `12`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 12) T
```

### 260. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c35a49805d`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `13`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 13) T
```

### 261. `root-derived-auto-schema-limit` `row-or-column-mismatch` `492ed89208`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `14`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 14) T
```

### 262. `root-derived-auto-schema-limit` `row-or-column-mismatch` `12689f701f`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `15`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 15) T
```

### 263. `root-derived-auto-schema-limit` `row-or-column-mismatch` `01afad794e`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `16`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 16) T
```

### 264. `root-derived-auto-schema-limit` `row-or-column-mismatch` `50dc94c105`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `17`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 17) T
```

### 265. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8966cfc29a`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `18`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 18) T
```

### 266. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fc2c1bb52a`

- Auto schema: `True`
- Expected columns: `('b_14',)`
- Expected row count: `19`
- Actual columns: `('b_14',)`
- Actual row count: `20`

```sql
-- query
SELECT b_14 FROM (SELECT TRUE AS b_14 FROM dual LIMIT 19) T
```

### 267. `root-derived-auto-schema-limit` `row-or-column-mismatch` `55c6d3f5f0`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `1`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 1) T
```

### 268. `root-derived-auto-schema-limit` `row-or-column-mismatch` `51306e7423`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `2`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 2) T
```

### 269. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c806150ae6`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `3`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 3) T
```

### 270. `root-derived-auto-schema-limit` `row-or-column-mismatch` `15b63772a6`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `4`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 4) T
```

### 271. `root-derived-auto-schema-limit` `row-or-column-mismatch` `09ea155579`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `5`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 5) T
```

### 272. `root-derived-auto-schema-limit` `row-or-column-mismatch` `37f24c4cac`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `6`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 6) T
```

### 273. `root-derived-auto-schema-limit` `row-or-column-mismatch` `89ba5e9ccd`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `7`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 7) T
```

### 274. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ab64be42fb`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `8`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 8) T
```

### 275. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bbddafac6a`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `9`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 9) T
```

### 276. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ca0a986468`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `10`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 10) T
```

### 277. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9a8fafddb1`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `11`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 11) T
```

### 278. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0ab2dc1fa3`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `12`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 12) T
```

### 279. `root-derived-auto-schema-limit` `row-or-column-mismatch` `64e8de21c1`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `13`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 13) T
```

### 280. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e9f23f6c4b`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `14`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 14) T
```

### 281. `root-derived-auto-schema-limit` `row-or-column-mismatch` `66ed27b8f8`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `15`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 15) T
```

### 282. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3bca3f9ec8`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `16`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 16) T
```

### 283. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9ee400d060`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `17`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 17) T
```

### 284. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ac8f05f5e9`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `18`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 18) T
```

### 285. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5bd72a4da7`

- Auto schema: `True`
- Expected columns: `('b_15',)`
- Expected row count: `19`
- Actual columns: `('b_15',)`
- Actual row count: `20`

```sql
-- query
SELECT b_15 FROM (SELECT TRUE AS b_15 FROM dual LIMIT 19) T
```

### 286. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c7837fed8f`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `1`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 1) T
```

### 287. `root-derived-auto-schema-limit` `row-or-column-mismatch` `547db0fe71`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `2`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 2) T
```

### 288. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7e8ecbef44`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `3`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 3) T
```

### 289. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a89ba4a3b6`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `4`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 4) T
```

### 290. `root-derived-auto-schema-limit` `row-or-column-mismatch` `25bf4de3b4`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `5`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 5) T
```

### 291. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9c56ec9cb8`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `6`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 6) T
```

### 292. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cb7e0f43e1`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `7`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 7) T
```

### 293. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2a98820863`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `8`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 8) T
```

### 294. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a569729d3c`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `9`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 9) T
```

### 295. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8b5c290a47`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `10`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 10) T
```

### 296. `root-derived-auto-schema-limit` `row-or-column-mismatch` `446a486902`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `11`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 11) T
```

### 297. `root-derived-auto-schema-limit` `row-or-column-mismatch` `236145138a`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `12`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 12) T
```

### 298. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b30a4e6131`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `13`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 13) T
```

### 299. `root-derived-auto-schema-limit` `row-or-column-mismatch` `05cd67a208`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `14`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 14) T
```

### 300. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ee5012abd0`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `15`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 15) T
```

### 301. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6039b4d0a8`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `16`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 16) T
```

### 302. `root-derived-auto-schema-limit` `row-or-column-mismatch` `78e34a6799`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `17`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 17) T
```

### 303. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4dee459893`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `18`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 18) T
```

### 304. `root-derived-auto-schema-limit` `row-or-column-mismatch` `01f9b25084`

- Auto schema: `True`
- Expected columns: `('b_16',)`
- Expected row count: `19`
- Actual columns: `('b_16',)`
- Actual row count: `20`

```sql
-- query
SELECT b_16 FROM (SELECT TRUE AS b_16 FROM dual LIMIT 19) T
```

### 305. `root-derived-auto-schema-limit` `row-or-column-mismatch` `012c4270fd`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `1`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 1) T
```

### 306. `root-derived-auto-schema-limit` `row-or-column-mismatch` `05516c97ee`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `2`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 2) T
```

### 307. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6d7994bd80`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `3`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 3) T
```

### 308. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1354c0d6e6`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `4`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 4) T
```

### 309. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7d1e60914f`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `5`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 5) T
```

### 310. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0a3ae54701`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `6`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 6) T
```

### 311. `root-derived-auto-schema-limit` `row-or-column-mismatch` `537cd48470`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `7`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 7) T
```

### 312. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6793d626b3`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `8`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 8) T
```

### 313. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c21cd09a3e`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `9`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 9) T
```

### 314. `root-derived-auto-schema-limit` `row-or-column-mismatch` `79056bf12c`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `10`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 10) T
```

### 315. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d4de476edb`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `11`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 11) T
```

### 316. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3204d31572`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `12`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 12) T
```

### 317. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9830b65fab`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `13`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 13) T
```

### 318. `root-derived-auto-schema-limit` `row-or-column-mismatch` `153386cfa2`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `14`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 14) T
```

### 319. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0554b6a4e3`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `15`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 15) T
```

### 320. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d1c391b95c`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `16`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 16) T
```

### 321. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4c2fff73e2`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `17`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 17) T
```

### 322. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5a6a208d42`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `18`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 18) T
```

### 323. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cdb5d52f9c`

- Auto schema: `True`
- Expected columns: `('b_17',)`
- Expected row count: `19`
- Actual columns: `('b_17',)`
- Actual row count: `20`

```sql
-- query
SELECT b_17 FROM (SELECT TRUE AS b_17 FROM dual LIMIT 19) T
```

### 324. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e3920c128d`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `1`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 1) T
```

### 325. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8c8161dc20`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `2`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 2) T
```

### 326. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e5fd01b33a`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `3`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 3) T
```

### 327. `root-derived-auto-schema-limit` `row-or-column-mismatch` `980275399d`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `4`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 4) T
```

### 328. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bfc213edbe`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `5`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 5) T
```

### 329. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b0d8144190`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `6`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 6) T
```

### 330. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f4278e990f`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `7`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 7) T
```

### 331. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1e5c6e6ad2`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `8`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 8) T
```

### 332. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5175a5e60a`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `9`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 9) T
```

### 333. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc87fc9758`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `10`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 10) T
```

### 334. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0d995615e4`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `11`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 11) T
```

### 335. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fb65fe60ba`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `12`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 12) T
```

### 336. `root-derived-auto-schema-limit` `row-or-column-mismatch` `08a354ec07`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `13`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 13) T
```

### 337. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2b41ca3ff8`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `14`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 14) T
```

### 338. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c2bb825259`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `15`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 15) T
```

### 339. `root-derived-auto-schema-limit` `row-or-column-mismatch` `19a3009f07`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `16`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 16) T
```

### 340. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3ea2c614d5`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `17`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 17) T
```

### 341. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d8a82dd5ac`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `18`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 18) T
```

### 342. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bff89e9c45`

- Auto schema: `True`
- Expected columns: `('b_18',)`
- Expected row count: `19`
- Actual columns: `('b_18',)`
- Actual row count: `20`

```sql
-- query
SELECT b_18 FROM (SELECT TRUE AS b_18 FROM dual LIMIT 19) T
```

### 343. `root-derived-auto-schema-limit` `row-or-column-mismatch` `66d278b37c`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `1`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 1) T
```

### 344. `root-derived-auto-schema-limit` `row-or-column-mismatch` `73ea67b422`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `2`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 2) T
```

### 345. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cd7433faac`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `3`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 3) T
```

### 346. `root-derived-auto-schema-limit` `row-or-column-mismatch` `12d9a179d4`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `4`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 4) T
```

### 347. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc4b35a369`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `5`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 5) T
```

### 348. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3826400405`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `6`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 6) T
```

### 349. `root-derived-auto-schema-limit` `row-or-column-mismatch` `be124d31e8`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `7`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 7) T
```

### 350. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9478ea7862`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `8`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 8) T
```

### 351. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9d3ef13308`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `9`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 9) T
```

### 352. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ea79db2c8c`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `10`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 10) T
```

### 353. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a8ec9b4ec4`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `11`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 11) T
```

### 354. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fb6a3c0b5d`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `12`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 12) T
```

### 355. `root-derived-auto-schema-limit` `row-or-column-mismatch` `88c2f7b399`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `13`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 13) T
```

### 356. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1395e7d7f4`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `14`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 14) T
```

### 357. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0b9aee8070`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `15`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 15) T
```

### 358. `root-derived-auto-schema-limit` `row-or-column-mismatch` `58b4e66a66`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `16`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 16) T
```

### 359. `root-derived-auto-schema-limit` `row-or-column-mismatch` `42e1088d30`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `17`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 17) T
```

### 360. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8ff1c09f8f`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `18`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 18) T
```

### 361. `root-derived-auto-schema-limit` `row-or-column-mismatch` `afecc01446`

- Auto schema: `True`
- Expected columns: `('b_19',)`
- Expected row count: `19`
- Actual columns: `('b_19',)`
- Actual row count: `20`

```sql
-- query
SELECT b_19 FROM (SELECT TRUE AS b_19 FROM dual LIMIT 19) T
```

### 362. `root-derived-auto-schema-limit` `row-or-column-mismatch` `530c0547d6`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `1`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 1) T
```

### 363. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c9e3db0194`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `2`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 2) T
```

### 364. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3df5c96e4c`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `3`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 3) T
```

### 365. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da332c1490`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `4`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 4) T
```

### 366. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4b4507731e`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `5`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 5) T
```

### 367. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8c99d641b2`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `6`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 6) T
```

### 368. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bbff243edb`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `7`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 7) T
```

### 369. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2a21857de5`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `8`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 8) T
```

### 370. `root-derived-auto-schema-limit` `row-or-column-mismatch` `116927d0d4`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `9`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 9) T
```

### 371. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e2b4e03a12`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `10`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 10) T
```

### 372. `root-derived-auto-schema-limit` `row-or-column-mismatch` `516a37959a`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `11`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 11) T
```

### 373. `root-derived-auto-schema-limit` `row-or-column-mismatch` `72f57a47ac`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `12`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 12) T
```

### 374. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b60a20e71a`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `13`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 13) T
```

### 375. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e3ce572b54`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `14`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 14) T
```

### 376. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b38c78d79b`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `15`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 15) T
```

### 377. `root-derived-auto-schema-limit` `row-or-column-mismatch` `04d8f8e494`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `16`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 16) T
```

### 378. `root-derived-auto-schema-limit` `row-or-column-mismatch` `231ab15223`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `17`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 17) T
```

### 379. `root-derived-auto-schema-limit` `row-or-column-mismatch` `521242ef48`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `18`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 18) T
```

### 380. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9ed4fe349a`

- Auto schema: `True`
- Expected columns: `('b_20',)`
- Expected row count: `19`
- Actual columns: `('b_20',)`
- Actual row count: `20`

```sql
-- query
SELECT b_20 FROM (SELECT TRUE AS b_20 FROM dual LIMIT 19) T
```

### 381. `root-derived-auto-schema-limit` `row-or-column-mismatch` `af81e8ff81`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `1`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 1) T
```

### 382. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7e5c7b24cb`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `2`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 2) T
```

### 383. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bbe873c4bd`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `3`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 3) T
```

### 384. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c11ad17508`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `4`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 4) T
```

### 385. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e951ef5606`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `5`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 5) T
```

### 386. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1727c21d40`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `6`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 6) T
```

### 387. `root-derived-auto-schema-limit` `row-or-column-mismatch` `28bf35a84a`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `7`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 7) T
```

### 388. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ee92c0d437`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `8`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 8) T
```

### 389. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1dca1cc819`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `9`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 9) T
```

### 390. `root-derived-auto-schema-limit` `row-or-column-mismatch` `15c3206b0d`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `10`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 10) T
```

### 391. `root-derived-auto-schema-limit` `row-or-column-mismatch` `39dd4b0dd1`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `11`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 11) T
```

### 392. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ccf42f1517`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `12`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 12) T
```

### 393. `root-derived-auto-schema-limit` `row-or-column-mismatch` `56079933b0`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `13`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 13) T
```

### 394. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c6b7572242`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `14`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 14) T
```

### 395. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1f4799ccd8`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `15`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 15) T
```

### 396. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da34d4a159`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `16`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 16) T
```

### 397. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1ba5b2ff65`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `17`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 17) T
```

### 398. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3173c0504c`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `18`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 18) T
```

### 399. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9230eb8da2`

- Auto schema: `True`
- Expected columns: `('b_21',)`
- Expected row count: `19`
- Actual columns: `('b_21',)`
- Actual row count: `20`

```sql
-- query
SELECT b_21 FROM (SELECT TRUE AS b_21 FROM dual LIMIT 19) T
```

### 400. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b233c7c51a`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `1`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 1) T
```

### 401. `root-derived-auto-schema-limit` `row-or-column-mismatch` `19cf715a33`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `2`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 2) T
```

### 402. `root-derived-auto-schema-limit` `row-or-column-mismatch` `099530fe20`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `3`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 3) T
```

### 403. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b2e9a5ecc2`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `4`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 4) T
```

### 404. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b03fff9f40`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `5`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 5) T
```

### 405. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b2b45a2332`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `6`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 6) T
```

### 406. `root-derived-auto-schema-limit` `row-or-column-mismatch` `df78036889`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `7`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 7) T
```

### 407. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6f6a7fc90b`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `8`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 8) T
```

### 408. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8e5929f73e`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `9`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 9) T
```

### 409. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3bae5c782b`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `10`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 10) T
```

### 410. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1eb354bdcb`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `11`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 11) T
```

### 411. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1481bd543c`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `12`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 12) T
```

### 412. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b427fc21fb`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `13`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 13) T
```

### 413. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ac128196c5`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `14`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 14) T
```

### 414. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c8478d878a`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `15`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 15) T
```

### 415. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9ba99a4930`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `16`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 16) T
```

### 416. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f08b7f2ddc`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `17`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 17) T
```

### 417. `root-derived-auto-schema-limit` `row-or-column-mismatch` `93b64014a7`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `18`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 18) T
```

### 418. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d3985617d0`

- Auto schema: `True`
- Expected columns: `('b_22',)`
- Expected row count: `19`
- Actual columns: `('b_22',)`
- Actual row count: `20`

```sql
-- query
SELECT b_22 FROM (SELECT TRUE AS b_22 FROM dual LIMIT 19) T
```

### 419. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f6b3ff6a39`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `1`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 1) T
```

### 420. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2f4ad735df`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `2`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 2) T
```

### 421. `root-derived-auto-schema-limit` `row-or-column-mismatch` `93f87a0eb1`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `3`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 3) T
```

### 422. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf7b85b5ca`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `4`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 4) T
```

### 423. `root-derived-auto-schema-limit` `row-or-column-mismatch` `75b251fd7f`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `5`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 5) T
```

### 424. `root-derived-auto-schema-limit` `row-or-column-mismatch` `48ba39c572`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `6`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 6) T
```

### 425. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8b99dc0dab`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `7`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 7) T
```

### 426. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fbd617ce0b`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `8`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 8) T
```

### 427. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e3a9ee989f`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `9`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 9) T
```

### 428. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0f324701f6`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `10`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 10) T
```

### 429. `root-derived-auto-schema-limit` `row-or-column-mismatch` `516081e072`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `11`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 11) T
```

### 430. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8952594573`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `12`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 12) T
```

### 431. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f1a8f425bc`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `13`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 13) T
```

### 432. `root-derived-auto-schema-limit` `row-or-column-mismatch` `86bec3e58b`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `14`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 14) T
```

### 433. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d34bca0adf`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `15`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 15) T
```

### 434. `root-derived-auto-schema-limit` `row-or-column-mismatch` `23006af5a3`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `16`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 16) T
```

### 435. `root-derived-auto-schema-limit` `row-or-column-mismatch` `16445a1705`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `17`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 17) T
```

### 436. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3cf23a18b3`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `18`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 18) T
```

### 437. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ce52bfcafa`

- Auto schema: `True`
- Expected columns: `('b_23',)`
- Expected row count: `19`
- Actual columns: `('b_23',)`
- Actual row count: `20`

```sql
-- query
SELECT b_23 FROM (SELECT TRUE AS b_23 FROM dual LIMIT 19) T
```

### 438. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6ba924feb9`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `1`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 1) T
```

### 439. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bb68ef09d4`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `2`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 2) T
```

### 440. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a593f183c6`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `3`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 3) T
```

### 441. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1ef261d432`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `4`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 4) T
```

### 442. `root-derived-auto-schema-limit` `row-or-column-mismatch` `356cffc640`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `5`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 5) T
```

### 443. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ba01bf775d`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `6`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 6) T
```

### 444. `root-derived-auto-schema-limit` `row-or-column-mismatch` `724c867089`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `7`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 7) T
```

### 445. `root-derived-auto-schema-limit` `row-or-column-mismatch` `46dacb463f`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `8`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 8) T
```

### 446. `root-derived-auto-schema-limit` `row-or-column-mismatch` `68f08a06ca`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `9`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 9) T
```

### 447. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3b9985e236`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `10`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 10) T
```

### 448. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0767f66a25`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `11`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 11) T
```

### 449. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d50956a3e3`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `12`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 12) T
```

### 450. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f7e1b3663b`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `13`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 13) T
```

### 451. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6aa43c0f3d`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `14`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 14) T
```

### 452. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ec5e232100`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `15`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 15) T
```

### 453. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ed20b928ac`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `16`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 16) T
```

### 454. `root-derived-auto-schema-limit` `row-or-column-mismatch` `21279b9cec`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `17`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 17) T
```

### 455. `root-derived-auto-schema-limit` `row-or-column-mismatch` `acb84a3f23`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `18`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 18) T
```

### 456. `root-derived-auto-schema-limit` `row-or-column-mismatch` `28e12de206`

- Auto schema: `True`
- Expected columns: `('b_24',)`
- Expected row count: `19`
- Actual columns: `('b_24',)`
- Actual row count: `20`

```sql
-- query
SELECT b_24 FROM (SELECT TRUE AS b_24 FROM dual LIMIT 19) T
```

### 457. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e11069348c`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `1`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 1) T
```

### 458. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8fd8cb9292`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `2`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 2) T
```

### 459. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a8d5536c63`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `3`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 3) T
```

### 460. `root-derived-auto-schema-limit` `row-or-column-mismatch` `993556223b`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `4`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 4) T
```

### 461. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3ff88460ca`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `5`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 5) T
```

### 462. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0fa0484f7e`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `6`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 6) T
```

### 463. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e2334a22ea`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `7`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 7) T
```

### 464. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1f0018faa5`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `8`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 8) T
```

### 465. `root-derived-auto-schema-limit` `row-or-column-mismatch` `95920d7da4`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `9`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 9) T
```

### 466. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a5090cbd87`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `10`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 10) T
```

### 467. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3e9b22ab01`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `11`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 11) T
```

### 468. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a11658ba99`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `12`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 12) T
```

### 469. `root-derived-auto-schema-limit` `row-or-column-mismatch` `71c851ca6b`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `13`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 13) T
```

### 470. `root-derived-auto-schema-limit` `row-or-column-mismatch` `30344f3eee`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `14`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 14) T
```

### 471. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7163c1f21c`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `15`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 15) T
```

### 472. `root-derived-auto-schema-limit` `row-or-column-mismatch` `390b66da02`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `16`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 16) T
```

### 473. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a3ca22612b`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `17`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 17) T
```

### 474. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3d4b2652b7`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `18`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 18) T
```

### 475. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a7bc1eb836`

- Auto schema: `True`
- Expected columns: `('b_25',)`
- Expected row count: `19`
- Actual columns: `('b_25',)`
- Actual row count: `20`

```sql
-- query
SELECT b_25 FROM (SELECT TRUE AS b_25 FROM dual LIMIT 19) T
```

### 476. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cbbc46559a`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `1`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 1) T
```

### 477. `root-derived-auto-schema-limit` `row-or-column-mismatch` `feadb8942b`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `2`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 2) T
```

### 478. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6de335a759`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `3`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 3) T
```

### 479. `root-derived-auto-schema-limit` `row-or-column-mismatch` `460036e816`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `4`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 4) T
```

### 480. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ebb657c6fb`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `5`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 5) T
```

### 481. `root-derived-auto-schema-limit` `row-or-column-mismatch` `77536c4e98`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `6`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 6) T
```

### 482. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d37f008686`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `7`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 7) T
```

### 483. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fa47f39e86`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `8`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 8) T
```

### 484. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c929d77502`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `9`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 9) T
```

### 485. `root-derived-auto-schema-limit` `row-or-column-mismatch` `36bca13b28`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `10`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 10) T
```

### 486. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e55c103434`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `11`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 11) T
```

### 487. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a28fff36a2`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `12`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 12) T
```

### 488. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ada9725bd3`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `13`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 13) T
```

### 489. `root-derived-auto-schema-limit` `row-or-column-mismatch` `12ed488354`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `14`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 14) T
```

### 490. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3663be68f9`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `15`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 15) T
```

### 491. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4ca57808ba`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `16`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 16) T
```

### 492. `root-derived-auto-schema-limit` `row-or-column-mismatch` `71fea76588`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `17`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 17) T
```

### 493. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2fac6d4d8c`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `18`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 18) T
```

### 494. `root-derived-auto-schema-limit` `row-or-column-mismatch` `034d04bd49`

- Auto schema: `True`
- Expected columns: `('b_26',)`
- Expected row count: `19`
- Actual columns: `('b_26',)`
- Actual row count: `20`

```sql
-- query
SELECT b_26 FROM (SELECT TRUE AS b_26 FROM dual LIMIT 19) T
```

### 495. `root-derived-auto-schema-limit` `row-or-column-mismatch` `235e7b7ed9`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `1`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 1) T
```

### 496. `root-derived-auto-schema-limit` `row-or-column-mismatch` `618b991db5`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `2`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 2) T
```

### 497. `root-derived-auto-schema-limit` `row-or-column-mismatch` `897ba91d7d`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `3`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 3) T
```

### 498. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f27eeea6da`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `4`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 4) T
```

### 499. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b5ba5ef485`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `5`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 5) T
```

### 500. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ed7340a5c1`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `6`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 6) T
```

### 501. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bb390d3ee1`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `7`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 7) T
```

### 502. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d6558c6bc4`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `8`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 8) T
```

### 503. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3a4dd91d89`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `9`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 9) T
```

### 504. `root-derived-auto-schema-limit` `row-or-column-mismatch` `29257b3a97`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `10`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 10) T
```

### 505. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0a4ebe23c1`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `11`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 11) T
```

### 506. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1a87f87160`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `12`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 12) T
```

### 507. `root-derived-auto-schema-limit` `row-or-column-mismatch` `68a2b429cc`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `13`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 13) T
```

### 508. `root-derived-auto-schema-limit` `row-or-column-mismatch` `813e1e147f`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `14`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 14) T
```

### 509. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f485541c7c`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `15`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 15) T
```

### 510. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3be7b16fdd`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `16`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 16) T
```

### 511. `root-derived-auto-schema-limit` `row-or-column-mismatch` `16e828f7cf`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `17`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 17) T
```

### 512. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ff0a3fbd3f`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `18`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 18) T
```

### 513. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cdb9a1ef3d`

- Auto schema: `True`
- Expected columns: `('b_27',)`
- Expected row count: `19`
- Actual columns: `('b_27',)`
- Actual row count: `20`

```sql
-- query
SELECT b_27 FROM (SELECT TRUE AS b_27 FROM dual LIMIT 19) T
```

### 514. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5ab90274ba`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `1`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 1) T
```

### 515. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc57358dd0`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `2`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 2) T
```

### 516. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3a2a8f5354`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `3`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 3) T
```

### 517. `root-derived-auto-schema-limit` `row-or-column-mismatch` `be95c958e3`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `4`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 4) T
```

### 518. `root-derived-auto-schema-limit` `row-or-column-mismatch` `18312462fe`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `5`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 5) T
```

### 519. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dbeb4aa5eb`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `6`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 6) T
```

### 520. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e645f62587`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `7`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 7) T
```

### 521. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eec804101c`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `8`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 8) T
```

### 522. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9afce7f7c0`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `9`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 9) T
```

### 523. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f5ec8c38c6`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `10`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 10) T
```

### 524. `root-derived-auto-schema-limit` `row-or-column-mismatch` `023711b664`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `11`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 11) T
```

### 525. `root-derived-auto-schema-limit` `row-or-column-mismatch` `85bb2e3b31`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `12`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 12) T
```

### 526. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4a887cb02c`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `13`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 13) T
```

### 527. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3b01244984`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `14`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 14) T
```

### 528. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bcdabd3af2`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `15`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 15) T
```

### 529. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3bff273297`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `16`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 16) T
```

### 530. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c6a0babfea`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `17`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 17) T
```

### 531. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cff54b28df`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `18`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 18) T
```

### 532. `root-derived-auto-schema-limit` `row-or-column-mismatch` `16dc7bddfe`

- Auto schema: `True`
- Expected columns: `('b_28',)`
- Expected row count: `19`
- Actual columns: `('b_28',)`
- Actual row count: `20`

```sql
-- query
SELECT b_28 FROM (SELECT TRUE AS b_28 FROM dual LIMIT 19) T
```

### 533. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cd29c5615d`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `1`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 1) T
```

### 534. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2340b11df7`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `2`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 2) T
```

### 535. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a77e7b0727`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `3`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 3) T
```

### 536. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d2b82d204c`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `4`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 4) T
```

### 537. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b7b083f4a1`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `5`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 5) T
```

### 538. `root-derived-auto-schema-limit` `row-or-column-mismatch` `33eeb0c878`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `6`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 6) T
```

### 539. `root-derived-auto-schema-limit` `row-or-column-mismatch` `96fc23c2f7`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `7`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 7) T
```

### 540. `root-derived-auto-schema-limit` `row-or-column-mismatch` `76fab4c2c8`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `8`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 8) T
```

### 541. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c4fc1ad278`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `9`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 9) T
```

### 542. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ab5a08e9f1`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `10`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 10) T
```

### 543. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6b34aac9c5`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `11`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 11) T
```

### 544. `root-derived-auto-schema-limit` `row-or-column-mismatch` `985e08a1be`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `12`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 12) T
```

### 545. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7808e3795f`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `13`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 13) T
```

### 546. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0852323743`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `14`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 14) T
```

### 547. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3749993c0b`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `15`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 15) T
```

### 548. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e64baf3c5d`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `16`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 16) T
```

### 549. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9dee6de679`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `17`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 17) T
```

### 550. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e1bbd34eaa`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `18`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 18) T
```

### 551. `root-derived-auto-schema-limit` `row-or-column-mismatch` `99ed564311`

- Auto schema: `True`
- Expected columns: `('b_29',)`
- Expected row count: `19`
- Actual columns: `('b_29',)`
- Actual row count: `20`

```sql
-- query
SELECT b_29 FROM (SELECT TRUE AS b_29 FROM dual LIMIT 19) T
```

### 552. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ccff69fa4b`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `1`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 1) T
```

### 553. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c38d47a673`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `2`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 2) T
```

### 554. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3ae9db86ea`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `3`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 3) T
```

### 555. `root-derived-auto-schema-limit` `row-or-column-mismatch` `888d8a34cd`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `4`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 4) T
```

### 556. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a16dcc06e4`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `5`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 5) T
```

### 557. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c7c13a296b`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `6`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 6) T
```

### 558. `root-derived-auto-schema-limit` `row-or-column-mismatch` `47c46301d3`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `7`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 7) T
```

### 559. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c95b9e4bb1`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `8`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 8) T
```

### 560. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f9d66d5326`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `9`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 9) T
```

### 561. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e430410690`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `10`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 10) T
```

### 562. `root-derived-auto-schema-limit` `row-or-column-mismatch` `50314595d1`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `11`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 11) T
```

### 563. `root-derived-auto-schema-limit` `row-or-column-mismatch` `844ac41819`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `12`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 12) T
```

### 564. `root-derived-auto-schema-limit` `row-or-column-mismatch` `28f63751fb`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `13`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 13) T
```

### 565. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ae28d33b75`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `14`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 14) T
```

### 566. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c792a8bcf2`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `15`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 15) T
```

### 567. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5773233225`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `16`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 16) T
```

### 568. `root-derived-auto-schema-limit` `row-or-column-mismatch` `47f2807740`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `17`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 17) T
```

### 569. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cdaa83b786`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `18`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 18) T
```

### 570. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2aa0734434`

- Auto schema: `True`
- Expected columns: `('b_30',)`
- Expected row count: `19`
- Actual columns: `('b_30',)`
- Actual row count: `20`

```sql
-- query
SELECT b_30 FROM (SELECT TRUE AS b_30 FROM dual LIMIT 19) T
```

### 571. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d9cdecb373`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `1`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 1) T
```

### 572. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d8399e627a`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `2`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 2) T
```

### 573. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a2a3af2961`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `3`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 3) T
```

### 574. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c6d4a3c22b`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `4`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 4) T
```

### 575. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0b1f1c6f85`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `5`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 5) T
```

### 576. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2c5fa1cc9b`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `6`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 6) T
```

### 577. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2a62924026`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `7`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 7) T
```

### 578. `root-derived-auto-schema-limit` `row-or-column-mismatch` `34a74795e0`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `8`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 8) T
```

### 579. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e4a3029bde`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `9`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 9) T
```

### 580. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e6e0de344f`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `10`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 10) T
```

### 581. `root-derived-auto-schema-limit` `row-or-column-mismatch` `226189d3ce`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `11`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 11) T
```

### 582. `root-derived-auto-schema-limit` `row-or-column-mismatch` `712dc00d3f`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `12`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 12) T
```

### 583. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ecc0c238f9`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `13`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 13) T
```

### 584. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e88c92967f`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `14`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 14) T
```

### 585. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d43958def3`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `15`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 15) T
```

### 586. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f389857e59`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `16`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 16) T
```

### 587. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d2db25c1b7`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `17`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 17) T
```

### 588. `root-derived-auto-schema-limit` `row-or-column-mismatch` `21ffbdb2e6`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `18`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 18) T
```

### 589. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e38d68d3fa`

- Auto schema: `True`
- Expected columns: `('b_31',)`
- Expected row count: `19`
- Actual columns: `('b_31',)`
- Actual row count: `20`

```sql
-- query
SELECT b_31 FROM (SELECT TRUE AS b_31 FROM dual LIMIT 19) T
```

### 590. `root-derived-auto-schema-limit` `row-or-column-mismatch` `aa39358148`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `1`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 1) T
```

### 591. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5b9af331bd`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `2`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 2) T
```

### 592. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7a2967d322`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `3`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 3) T
```

### 593. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9e07418f3d`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `4`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 4) T
```

### 594. `root-derived-auto-schema-limit` `row-or-column-mismatch` `34decddd7d`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `5`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 5) T
```

### 595. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9c5c73af8c`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `6`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 6) T
```

### 596. `root-derived-auto-schema-limit` `row-or-column-mismatch` `defda16588`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `7`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 7) T
```

### 597. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f5ed35417c`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `8`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 8) T
```

### 598. `root-derived-auto-schema-limit` `row-or-column-mismatch` `59dd8282cc`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `9`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 9) T
```

### 599. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bae81b60f1`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `10`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 10) T
```

### 600. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8c65fcb599`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `11`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 11) T
```

### 601. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d11b59e67a`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `12`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 12) T
```

### 602. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5db811bd0c`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `13`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 13) T
```

### 603. `root-derived-auto-schema-limit` `row-or-column-mismatch` `51685e608f`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `14`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 14) T
```

### 604. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c50faf2746`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `15`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 15) T
```

### 605. `root-derived-auto-schema-limit` `row-or-column-mismatch` `893340c15a`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `16`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 16) T
```

### 606. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a3ec5f7a56`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `17`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 17) T
```

### 607. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7312309a36`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `18`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 18) T
```

### 608. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4d05297545`

- Auto schema: `True`
- Expected columns: `('b_32',)`
- Expected row count: `19`
- Actual columns: `('b_32',)`
- Actual row count: `20`

```sql
-- query
SELECT b_32 FROM (SELECT TRUE AS b_32 FROM dual LIMIT 19) T
```

### 609. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f8b355a720`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `1`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 1) T
```

### 610. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0b497a999a`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `2`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 2) T
```

### 611. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1547a32153`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `3`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 3) T
```

### 612. `root-derived-auto-schema-limit` `row-or-column-mismatch` `34d308e645`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `4`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 4) T
```

### 613. `root-derived-auto-schema-limit` `row-or-column-mismatch` `90998c9501`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `5`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 5) T
```

### 614. `root-derived-auto-schema-limit` `row-or-column-mismatch` `aa7e6f018e`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `6`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 6) T
```

### 615. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8fd66a864b`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `7`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 7) T
```

### 616. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3b75c2a5fe`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `8`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 8) T
```

### 617. `root-derived-auto-schema-limit` `row-or-column-mismatch` `140c0d037f`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `9`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 9) T
```

### 618. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c051df526b`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `10`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 10) T
```

### 619. `root-derived-auto-schema-limit` `row-or-column-mismatch` `26dec9540d`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `11`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 11) T
```

### 620. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bf1287f00c`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `12`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 12) T
```

### 621. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c746f473a4`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `13`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 13) T
```

### 622. `root-derived-auto-schema-limit` `row-or-column-mismatch` `013af22792`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `14`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 14) T
```

### 623. `root-derived-auto-schema-limit` `row-or-column-mismatch` `75ac09e8f4`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `15`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 15) T
```

### 624. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ebfbd8d192`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `16`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 16) T
```

### 625. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d22f1620b1`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `17`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 17) T
```

### 626. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d7c09981ec`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `18`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 18) T
```

### 627. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc2ec05eb0`

- Auto schema: `True`
- Expected columns: `('b_33',)`
- Expected row count: `19`
- Actual columns: `('b_33',)`
- Actual row count: `20`

```sql
-- query
SELECT b_33 FROM (SELECT TRUE AS b_33 FROM dual LIMIT 19) T
```

### 628. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1fb5a5c559`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `1`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 1) T
```

### 629. `root-derived-auto-schema-limit` `row-or-column-mismatch` `96d79f50e8`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `2`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 2) T
```

### 630. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c3ea6a8d03`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `3`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 3) T
```

### 631. `root-derived-auto-schema-limit` `row-or-column-mismatch` `234738cfce`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `4`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 4) T
```

### 632. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b769b040f4`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `5`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 5) T
```

### 633. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2d7408d50a`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `6`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 6) T
```

### 634. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ccb2973d5e`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `7`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 7) T
```

### 635. `root-derived-auto-schema-limit` `row-or-column-mismatch` `77e2785b4c`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `8`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 8) T
```

### 636. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4c1db2647d`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `9`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 9) T
```

### 637. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ae78a0b2ec`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `10`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 10) T
```

### 638. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ba0fa80095`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `11`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 11) T
```

### 639. `root-derived-auto-schema-limit` `row-or-column-mismatch` `809f68a0a6`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `12`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 12) T
```

### 640. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b8d748d752`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `13`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 13) T
```

### 641. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c18c48b341`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `14`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 14) T
```

### 642. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf9bb09ac7`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `15`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 15) T
```

### 643. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6c1297e44b`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `16`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 16) T
```

### 644. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da5f9f8012`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `17`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 17) T
```

### 645. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c7a6fcaa0e`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `18`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 18) T
```

### 646. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a10a9f80a6`

- Auto schema: `True`
- Expected columns: `('b_34',)`
- Expected row count: `19`
- Actual columns: `('b_34',)`
- Actual row count: `20`

```sql
-- query
SELECT b_34 FROM (SELECT TRUE AS b_34 FROM dual LIMIT 19) T
```

### 647. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8ab908da1e`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `1`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 1) T
```

### 648. `root-derived-auto-schema-limit` `row-or-column-mismatch` `59d0e01be8`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `2`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 2) T
```

### 649. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ec10dc334d`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `3`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 3) T
```

### 650. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5da2a8c06b`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `4`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 4) T
```

### 651. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0f3e250011`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `5`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 5) T
```

### 652. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0873333529`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `6`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 6) T
```

### 653. `root-derived-auto-schema-limit` `row-or-column-mismatch` `43df4707ad`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `7`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 7) T
```

### 654. `root-derived-auto-schema-limit` `row-or-column-mismatch` `085d4cc48b`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `8`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 8) T
```

### 655. `root-derived-auto-schema-limit` `row-or-column-mismatch` `679cd7fd71`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `9`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 9) T
```

### 656. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b3b45d4e65`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `10`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 10) T
```

### 657. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2d01cf5f7e`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `11`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 11) T
```

### 658. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d17d5fff9b`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `12`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 12) T
```

### 659. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dd401a5694`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `13`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 13) T
```

### 660. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2d1aeb701e`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `14`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 14) T
```

### 661. `root-derived-auto-schema-limit` `row-or-column-mismatch` `92c6bc3d79`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `15`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 15) T
```

### 662. `root-derived-auto-schema-limit` `row-or-column-mismatch` `52955f0a0f`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `16`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 16) T
```

### 663. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e03434d85e`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `17`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 17) T
```

### 664. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c5993ee0f7`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `18`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 18) T
```

### 665. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b60121a32e`

- Auto schema: `True`
- Expected columns: `('b_35',)`
- Expected row count: `19`
- Actual columns: `('b_35',)`
- Actual row count: `20`

```sql
-- query
SELECT b_35 FROM (SELECT TRUE AS b_35 FROM dual LIMIT 19) T
```

### 666. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6e79fd4a26`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `1`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 1) T
```

### 667. `root-derived-auto-schema-limit` `row-or-column-mismatch` `64eaefda0c`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `2`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 2) T
```

### 668. `root-derived-auto-schema-limit` `row-or-column-mismatch` `980a4c9aeb`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `3`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 3) T
```

### 669. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6f4139f8f9`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `4`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 4) T
```

### 670. `root-derived-auto-schema-limit` `row-or-column-mismatch` `105c864343`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `5`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 5) T
```

### 671. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1f85d98a67`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `6`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 6) T
```

### 672. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1837f660cf`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `7`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 7) T
```

### 673. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ccb04fbdf5`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `8`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 8) T
```

### 674. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7558fedd8d`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `9`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 9) T
```

### 675. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ed11518e3f`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `10`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 10) T
```

### 676. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9e78295e1d`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `11`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 11) T
```

### 677. `root-derived-auto-schema-limit` `row-or-column-mismatch` `910c3fcfea`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `12`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 12) T
```

### 678. `root-derived-auto-schema-limit` `row-or-column-mismatch` `566dc06b1c`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `13`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 13) T
```

### 679. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4268c384a6`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `14`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 14) T
```

### 680. `root-derived-auto-schema-limit` `row-or-column-mismatch` `717b77ea21`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `15`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 15) T
```

### 681. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1b4c2944ba`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `16`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 16) T
```

### 682. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3858e38f2e`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `17`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 17) T
```

### 683. `root-derived-auto-schema-limit` `row-or-column-mismatch` `730d70897e`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `18`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 18) T
```

### 684. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1275e50cb1`

- Auto schema: `True`
- Expected columns: `('b_36',)`
- Expected row count: `19`
- Actual columns: `('b_36',)`
- Actual row count: `20`

```sql
-- query
SELECT b_36 FROM (SELECT TRUE AS b_36 FROM dual LIMIT 19) T
```

### 685. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d37c5cc2ba`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `1`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 1) T
```

### 686. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2025a3aa19`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `2`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 2) T
```

### 687. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f25d4d8487`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `3`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 3) T
```

### 688. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a85e5a4d5f`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `4`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 4) T
```

### 689. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eef57e3434`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `5`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 5) T
```

### 690. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7ef315fb71`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `6`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 6) T
```

### 691. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8adcbe5c04`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `7`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 7) T
```

### 692. `root-derived-auto-schema-limit` `row-or-column-mismatch` `25f98b5cc6`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `8`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 8) T
```

### 693. `root-derived-auto-schema-limit` `row-or-column-mismatch` `093e6bc4bc`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `9`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 9) T
```

### 694. `root-derived-auto-schema-limit` `row-or-column-mismatch` `538e5e9c2a`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `10`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 10) T
```

### 695. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2a11d03d03`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `11`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 11) T
```

### 696. `root-derived-auto-schema-limit` `row-or-column-mismatch` `59cbf0fc1d`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `12`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 12) T
```

### 697. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d6e392daaa`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `13`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 13) T
```

### 698. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c27c93fb23`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `14`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 14) T
```

### 699. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eff5f24ded`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `15`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 15) T
```

### 700. `root-derived-auto-schema-limit` `row-or-column-mismatch` `19535dbad6`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `16`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 16) T
```

### 701. `root-derived-auto-schema-limit` `row-or-column-mismatch` `49456d7d36`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `17`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 17) T
```

### 702. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fff2b583f5`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `18`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 18) T
```

### 703. `root-derived-auto-schema-limit` `row-or-column-mismatch` `308e128c74`

- Auto schema: `True`
- Expected columns: `('b_37',)`
- Expected row count: `19`
- Actual columns: `('b_37',)`
- Actual row count: `20`

```sql
-- query
SELECT b_37 FROM (SELECT TRUE AS b_37 FROM dual LIMIT 19) T
```

### 704. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0ea22c3931`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `1`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 1) T
```

### 705. `root-derived-auto-schema-limit` `row-or-column-mismatch` `224bfbfb1e`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `2`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 2) T
```

### 706. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5f7dadcc9c`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `3`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 3) T
```

### 707. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7eef35bbdc`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `4`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 4) T
```

### 708. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2e0b9fcdf5`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `5`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 5) T
```

### 709. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6e0bd6d702`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `6`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 6) T
```

### 710. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1dcb61e546`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `7`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 7) T
```

### 711. `root-derived-auto-schema-limit` `row-or-column-mismatch` `63fa7d1c2a`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `8`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 8) T
```

### 712. `root-derived-auto-schema-limit` `row-or-column-mismatch` `172a082638`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `9`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 9) T
```

### 713. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ac51dbc19f`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `10`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 10) T
```

### 714. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9274e3a747`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `11`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 11) T
```

### 715. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f5a0470bac`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `12`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 12) T
```

### 716. `root-derived-auto-schema-limit` `row-or-column-mismatch` `066c142c11`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `13`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 13) T
```

### 717. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5b8bca425c`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `14`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 14) T
```

### 718. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0153521d21`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `15`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 15) T
```

### 719. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9de61edb95`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `16`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 16) T
```

### 720. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8074a688d5`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `17`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 17) T
```

### 721. `root-derived-auto-schema-limit` `row-or-column-mismatch` `46c1fb0a0f`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `18`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 18) T
```

### 722. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4d80140b37`

- Auto schema: `True`
- Expected columns: `('b_38',)`
- Expected row count: `19`
- Actual columns: `('b_38',)`
- Actual row count: `20`

```sql
-- query
SELECT b_38 FROM (SELECT TRUE AS b_38 FROM dual LIMIT 19) T
```

### 723. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f63a7a9aef`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `1`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 1) T
```

### 724. `root-derived-auto-schema-limit` `row-or-column-mismatch` `39d649243a`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `2`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 2) T
```

### 725. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6c3c36922b`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `3`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 3) T
```

### 726. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a4821a883b`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `4`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 4) T
```

### 727. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fff4efea40`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `5`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 5) T
```

### 728. `root-derived-auto-schema-limit` `row-or-column-mismatch` `684150ca4e`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `6`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 6) T
```

### 729. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cc95c42919`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `7`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 7) T
```

### 730. `root-derived-auto-schema-limit` `row-or-column-mismatch` `680783a862`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `8`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 8) T
```

### 731. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a295e3ab57`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `9`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 9) T
```

### 732. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5b5500c432`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `10`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 10) T
```

### 733. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d30d940a51`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `11`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 11) T
```

### 734. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ddd1d4d1bb`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `12`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 12) T
```

### 735. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5df050eba4`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `13`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 13) T
```

### 736. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b18ea65722`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `14`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 14) T
```

### 737. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bc1f1e670e`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `15`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 15) T
```

### 738. `root-derived-auto-schema-limit` `row-or-column-mismatch` `be732b9f4f`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `16`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 16) T
```

### 739. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cb4b75bea7`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `17`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 17) T
```

### 740. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b265352194`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `18`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 18) T
```

### 741. `root-derived-auto-schema-limit` `row-or-column-mismatch` `75efda140c`

- Auto schema: `True`
- Expected columns: `('b_39',)`
- Expected row count: `19`
- Actual columns: `('b_39',)`
- Actual row count: `20`

```sql
-- query
SELECT b_39 FROM (SELECT TRUE AS b_39 FROM dual LIMIT 19) T
```

### 742. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b57a8b12d4`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `1`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 1) T
```

### 743. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7b612e5f4b`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `2`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 2) T
```

### 744. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a64686e5af`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `3`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 3) T
```

### 745. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bd6182cbd1`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `4`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 4) T
```

### 746. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2136784ae7`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `5`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 5) T
```

### 747. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cc722edf03`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `6`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 6) T
```

### 748. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eaa3958a8f`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `7`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 7) T
```

### 749. `root-derived-auto-schema-limit` `row-or-column-mismatch` `33caecc063`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `8`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 8) T
```

### 750. `root-derived-auto-schema-limit` `row-or-column-mismatch` `add8702bae`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `9`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 9) T
```

### 751. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0e1261a1c6`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `10`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 10) T
```

### 752. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8cf5c09a45`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `11`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 11) T
```

### 753. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f80668bb28`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `12`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 12) T
```

### 754. `root-derived-auto-schema-limit` `row-or-column-mismatch` `178e7d448f`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `13`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 13) T
```

### 755. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5d804569bb`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `14`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 14) T
```

### 756. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e7ef5081a0`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `15`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 15) T
```

### 757. `root-derived-auto-schema-limit` `row-or-column-mismatch` `497f695ae2`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `16`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 16) T
```

### 758. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a2bb77a15d`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `17`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 17) T
```

### 759. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e7e1bfade9`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `18`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 18) T
```

### 760. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f0fe7fc6ce`

- Auto schema: `True`
- Expected columns: `('b_40',)`
- Expected row count: `19`
- Actual columns: `('b_40',)`
- Actual row count: `20`

```sql
-- query
SELECT b_40 FROM (SELECT TRUE AS b_40 FROM dual LIMIT 19) T
```

### 761. `root-derived-auto-schema-limit` `row-or-column-mismatch` `154325e8c4`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `1`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 1) T
```

### 762. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7e7e20fbec`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `2`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 2) T
```

### 763. `root-derived-auto-schema-limit` `row-or-column-mismatch` `193d2a0b3b`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `3`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 3) T
```

### 764. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c1e9414578`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `4`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 4) T
```

### 765. `root-derived-auto-schema-limit` `row-or-column-mismatch` `012f39996c`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `5`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 5) T
```

### 766. `root-derived-auto-schema-limit` `row-or-column-mismatch` `760a74979f`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `6`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 6) T
```

### 767. `root-derived-auto-schema-limit` `row-or-column-mismatch` `be2d61f974`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `7`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 7) T
```

### 768. `root-derived-auto-schema-limit` `row-or-column-mismatch` `23761a2b86`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `8`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 8) T
```

### 769. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3dc0892d15`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `9`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 9) T
```

### 770. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c1e70d52df`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `10`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 10) T
```

### 771. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d21a4ee4a0`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `11`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 11) T
```

### 772. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9ed3af9751`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `12`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 12) T
```

### 773. `root-derived-auto-schema-limit` `row-or-column-mismatch` `42bf4addcc`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `13`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 13) T
```

### 774. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1c80c2a955`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `14`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 14) T
```

### 775. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9b83643c53`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `15`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 15) T
```

### 776. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a470480f5b`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `16`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 16) T
```

### 777. `root-derived-auto-schema-limit` `row-or-column-mismatch` `acce94fb70`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `17`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 17) T
```

### 778. `root-derived-auto-schema-limit` `row-or-column-mismatch` `364b06beb3`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `18`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 18) T
```

### 779. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b14aeddd03`

- Auto schema: `True`
- Expected columns: `('b_41',)`
- Expected row count: `19`
- Actual columns: `('b_41',)`
- Actual row count: `20`

```sql
-- query
SELECT b_41 FROM (SELECT TRUE AS b_41 FROM dual LIMIT 19) T
```

### 780. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc9ab3f7e9`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `1`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 1) T
```

### 781. `root-derived-auto-schema-limit` `row-or-column-mismatch` `42d45e2bc2`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `2`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 2) T
```

### 782. `root-derived-auto-schema-limit` `row-or-column-mismatch` `38bfa753eb`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `3`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 3) T
```

### 783. `root-derived-auto-schema-limit` `row-or-column-mismatch` `40fb9b0d4b`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `4`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 4) T
```

### 784. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3206c017e7`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `5`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 5) T
```

### 785. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bcf3f110da`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `6`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 6) T
```

### 786. `root-derived-auto-schema-limit` `row-or-column-mismatch` `df7bc4e29c`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `7`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 7) T
```

### 787. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9e179c8d6e`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `8`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 8) T
```

### 788. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2f51ec9c19`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `9`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 9) T
```

### 789. `root-derived-auto-schema-limit` `row-or-column-mismatch` `73abb825b6`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `10`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 10) T
```

### 790. `root-derived-auto-schema-limit` `row-or-column-mismatch` `369a1bc427`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `11`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 11) T
```

### 791. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e007ec512e`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `12`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 12) T
```

### 792. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5c9be571be`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `13`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 13) T
```

### 793. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b38bfded57`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `14`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 14) T
```

### 794. `root-derived-auto-schema-limit` `row-or-column-mismatch` `71ec065a6b`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `15`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 15) T
```

### 795. `root-derived-auto-schema-limit` `row-or-column-mismatch` `293057173a`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `16`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 16) T
```

### 796. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a3b10a32e4`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `17`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 17) T
```

### 797. `root-derived-auto-schema-limit` `row-or-column-mismatch` `aaeb12b431`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `18`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 18) T
```

### 798. `root-derived-auto-schema-limit` `row-or-column-mismatch` `55f1d70b96`

- Auto schema: `True`
- Expected columns: `('b_42',)`
- Expected row count: `19`
- Actual columns: `('b_42',)`
- Actual row count: `20`

```sql
-- query
SELECT b_42 FROM (SELECT TRUE AS b_42 FROM dual LIMIT 19) T
```

### 799. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da5c6f7a52`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `1`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 1) T
```

### 800. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b85441459a`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `2`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 2) T
```

### 801. `root-derived-auto-schema-limit` `row-or-column-mismatch` `11b0c42a0b`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `3`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 3) T
```

### 802. `root-derived-auto-schema-limit` `row-or-column-mismatch` `09b4929334`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `4`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 4) T
```

### 803. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e92da6c4ed`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `5`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 5) T
```

### 804. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6995ed6a2d`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `6`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 6) T
```

### 805. `root-derived-auto-schema-limit` `row-or-column-mismatch` `048a51a325`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `7`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 7) T
```

### 806. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7f94a91125`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `8`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 8) T
```

### 807. `root-derived-auto-schema-limit` `row-or-column-mismatch` `670de8aa4c`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `9`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 9) T
```

### 808. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f4789b1dcc`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `10`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 10) T
```

### 809. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7d5c37312e`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `11`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 11) T
```

### 810. `root-derived-auto-schema-limit` `row-or-column-mismatch` `46ce9d34dc`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `12`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 12) T
```

### 811. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8be1f4abe1`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `13`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 13) T
```

### 812. `root-derived-auto-schema-limit` `row-or-column-mismatch` `38c198dd9b`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `14`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 14) T
```

### 813. `root-derived-auto-schema-limit` `row-or-column-mismatch` `048cdc2da8`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `15`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 15) T
```

### 814. `root-derived-auto-schema-limit` `row-or-column-mismatch` `94d13004e0`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `16`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 16) T
```

### 815. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d577696745`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `17`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 17) T
```

### 816. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6cc5e751ba`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `18`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 18) T
```

### 817. `root-derived-auto-schema-limit` `row-or-column-mismatch` `593ba41e93`

- Auto schema: `True`
- Expected columns: `('b_43',)`
- Expected row count: `19`
- Actual columns: `('b_43',)`
- Actual row count: `20`

```sql
-- query
SELECT b_43 FROM (SELECT TRUE AS b_43 FROM dual LIMIT 19) T
```

### 818. `root-derived-auto-schema-limit` `row-or-column-mismatch` `170d8b1c6d`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `1`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 1) T
```

### 819. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ff67e25dbe`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `2`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 2) T
```

### 820. `root-derived-auto-schema-limit` `row-or-column-mismatch` `31efebf20c`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `3`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 3) T
```

### 821. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e9bce90d2f`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `4`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 4) T
```

### 822. `root-derived-auto-schema-limit` `row-or-column-mismatch` `76cbd44262`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `5`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 5) T
```

### 823. `root-derived-auto-schema-limit` `row-or-column-mismatch` `93f39ddb84`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `6`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 6) T
```

### 824. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4c4bc61caa`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `7`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 7) T
```

### 825. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a49b2d808b`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `8`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 8) T
```

### 826. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e10246df28`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `9`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 9) T
```

### 827. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f73b727a36`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `10`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 10) T
```

### 828. `root-derived-auto-schema-limit` `row-or-column-mismatch` `28c2fc07db`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `11`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 11) T
```

### 829. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf2b2e972f`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `12`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 12) T
```

### 830. `root-derived-auto-schema-limit` `row-or-column-mismatch` `435dc71988`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `13`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 13) T
```

### 831. `root-derived-auto-schema-limit` `row-or-column-mismatch` `788e0b0ae0`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `14`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 14) T
```

### 832. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5c580ec092`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `15`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 15) T
```

### 833. `root-derived-auto-schema-limit` `row-or-column-mismatch` `867f50967a`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `16`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 16) T
```

### 834. `root-derived-auto-schema-limit` `row-or-column-mismatch` `10f7d8b7ca`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `17`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 17) T
```

### 835. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a196598b44`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `18`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 18) T
```

### 836. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ed3dc83b8d`

- Auto schema: `True`
- Expected columns: `('b_44',)`
- Expected row count: `19`
- Actual columns: `('b_44',)`
- Actual row count: `20`

```sql
-- query
SELECT b_44 FROM (SELECT TRUE AS b_44 FROM dual LIMIT 19) T
```

### 837. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a3d798ae45`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `1`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 1) T
```

### 838. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6cf48d489d`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `2`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 2) T
```

### 839. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c892515e6f`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `3`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 3) T
```

### 840. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ec33f3adf6`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `4`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 4) T
```

### 841. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9fd13582b9`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `5`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 5) T
```

### 842. `root-derived-auto-schema-limit` `row-or-column-mismatch` `df49bc8b22`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `6`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 6) T
```

### 843. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7b8d6ad83f`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `7`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 7) T
```

### 844. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c621cb1797`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `8`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 8) T
```

### 845. `root-derived-auto-schema-limit` `row-or-column-mismatch` `56295234cb`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `9`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 9) T
```

### 846. `root-derived-auto-schema-limit` `row-or-column-mismatch` `813d545deb`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `10`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 10) T
```

### 847. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d4272c17ea`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `11`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 11) T
```

### 848. `root-derived-auto-schema-limit` `row-or-column-mismatch` `839e979e3b`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `12`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 12) T
```

### 849. `root-derived-auto-schema-limit` `row-or-column-mismatch` `41d9bf35ad`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `13`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 13) T
```

### 850. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d333e597d2`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `14`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 14) T
```

### 851. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0ca16a3df3`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `15`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 15) T
```

### 852. `root-derived-auto-schema-limit` `row-or-column-mismatch` `475d679b87`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `16`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 16) T
```

### 853. `root-derived-auto-schema-limit` `row-or-column-mismatch` `11a66a8e9c`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `17`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 17) T
```

### 854. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1a4f22a8e4`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `18`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 18) T
```

### 855. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e7c4ca96bc`

- Auto schema: `True`
- Expected columns: `('b_45',)`
- Expected row count: `19`
- Actual columns: `('b_45',)`
- Actual row count: `20`

```sql
-- query
SELECT b_45 FROM (SELECT TRUE AS b_45 FROM dual LIMIT 19) T
```

### 856. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4bdcf8ddfc`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `1`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 1) T
```

### 857. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ae0669e0d0`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `2`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 2) T
```

### 858. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7bee90a177`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `3`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 3) T
```

### 859. `root-derived-auto-schema-limit` `row-or-column-mismatch` `30a2b8d751`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `4`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 4) T
```

### 860. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ce6888008f`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `5`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 5) T
```

### 861. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ca2a777794`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `6`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 6) T
```

### 862. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f69561a479`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `7`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 7) T
```

### 863. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6c4fa38e55`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `8`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 8) T
```

### 864. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9a96fd1cba`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `9`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 9) T
```

### 865. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d4ab6e98b8`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `10`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 10) T
```

### 866. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7f10e5dbef`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `11`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 11) T
```

### 867. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ea13604d95`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `12`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 12) T
```

### 868. `root-derived-auto-schema-limit` `row-or-column-mismatch` `07a3f4b95e`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `13`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 13) T
```

### 869. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8eb2dbe3f1`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `14`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 14) T
```

### 870. `root-derived-auto-schema-limit` `row-or-column-mismatch` `47cfb6ce1d`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `15`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 15) T
```

### 871. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ad98889020`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `16`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 16) T
```

### 872. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1d9db3f1af`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `17`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 17) T
```

### 873. `root-derived-auto-schema-limit` `row-or-column-mismatch` `551a382f51`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `18`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 18) T
```

### 874. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8270a1916f`

- Auto schema: `True`
- Expected columns: `('b_46',)`
- Expected row count: `19`
- Actual columns: `('b_46',)`
- Actual row count: `20`

```sql
-- query
SELECT b_46 FROM (SELECT TRUE AS b_46 FROM dual LIMIT 19) T
```

### 875. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3327e2da38`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `1`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 1) T
```

### 876. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c8c4d0eab8`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `2`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 2) T
```

### 877. `root-derived-auto-schema-limit` `row-or-column-mismatch` `27cbb889d4`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `3`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 3) T
```

### 878. `root-derived-auto-schema-limit` `row-or-column-mismatch` `deb9ec87e9`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `4`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 4) T
```

### 879. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4cf2dbd728`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `5`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 5) T
```

### 880. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9e0db9d446`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `6`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 6) T
```

### 881. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a779c1bb2b`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `7`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 7) T
```

### 882. `root-derived-auto-schema-limit` `row-or-column-mismatch` `31c84a2177`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `8`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 8) T
```

### 883. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e5bf022f40`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `9`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 9) T
```

### 884. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cc7fb99cf6`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `10`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 10) T
```

### 885. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a9e1ec3d9d`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `11`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 11) T
```

### 886. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8e8b202e52`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `12`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 12) T
```

### 887. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e07a176c06`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `13`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 13) T
```

### 888. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7819eeffff`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `14`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 14) T
```

### 889. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e353074210`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `15`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 15) T
```

### 890. `root-derived-auto-schema-limit` `row-or-column-mismatch` `42dc4b7389`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `16`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 16) T
```

### 891. `root-derived-auto-schema-limit` `row-or-column-mismatch` `e890c3d008`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `17`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 17) T
```

### 892. `root-derived-auto-schema-limit` `row-or-column-mismatch` `32a2e0bd81`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `18`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 18) T
```

### 893. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9e50ff8646`

- Auto schema: `True`
- Expected columns: `('b_47',)`
- Expected row count: `19`
- Actual columns: `('b_47',)`
- Actual row count: `20`

```sql
-- query
SELECT b_47 FROM (SELECT TRUE AS b_47 FROM dual LIMIT 19) T
```

### 894. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b17ad52216`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `1`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 1) T
```

### 895. `root-derived-auto-schema-limit` `row-or-column-mismatch` `83848f8319`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `2`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 2) T
```

### 896. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7323284b70`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `3`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 3) T
```

### 897. `root-derived-auto-schema-limit` `row-or-column-mismatch` `feebeecbc1`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `4`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 4) T
```

### 898. `root-derived-auto-schema-limit` `row-or-column-mismatch` `708d4f87cb`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `5`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 5) T
```

### 899. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8ed6a72b97`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `6`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 6) T
```

### 900. `root-derived-auto-schema-limit` `row-or-column-mismatch` `02e63d50ad`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `7`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 7) T
```

### 901. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a3b48a3c7a`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `8`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 8) T
```

### 902. `root-derived-auto-schema-limit` `row-or-column-mismatch` `90241d0b30`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `9`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 9) T
```

### 903. `root-derived-auto-schema-limit` `row-or-column-mismatch` `db75f804be`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `10`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 10) T
```

### 904. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1b19a8f919`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `11`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 11) T
```

### 905. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a52fa1827a`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `12`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 12) T
```

### 906. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ede6b46c35`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `13`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 13) T
```

### 907. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bea54824fe`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `14`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 14) T
```

### 908. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1b78877d71`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `15`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 15) T
```

### 909. `root-derived-auto-schema-limit` `row-or-column-mismatch` `365787dfbd`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `16`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 16) T
```

### 910. `root-derived-auto-schema-limit` `row-or-column-mismatch` `04af5a62c2`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `17`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 17) T
```

### 911. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a40b6bd52d`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `18`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 18) T
```

### 912. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8a51d282e9`

- Auto schema: `True`
- Expected columns: `('b_48',)`
- Expected row count: `19`
- Actual columns: `('b_48',)`
- Actual row count: `20`

```sql
-- query
SELECT b_48 FROM (SELECT TRUE AS b_48 FROM dual LIMIT 19) T
```

### 913. `root-derived-auto-schema-limit` `row-or-column-mismatch` `eaa2c55d1b`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `1`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 1) T
```

### 914. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1c5b44999c`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `2`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 2) T
```

### 915. `root-derived-auto-schema-limit` `row-or-column-mismatch` `459cf7f454`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `3`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 3) T
```

### 916. `root-derived-auto-schema-limit` `row-or-column-mismatch` `00b1c97838`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `4`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 4) T
```

### 917. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1a3f477ab9`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `5`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 5) T
```

### 918. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c5e53313ca`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `6`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 6) T
```

### 919. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b4bb86a346`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `7`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 7) T
```

### 920. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7f1137b2b1`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `8`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 8) T
```

### 921. `root-derived-auto-schema-limit` `row-or-column-mismatch` `22347ee071`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `9`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 9) T
```

### 922. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5c36ae0c3f`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `10`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 10) T
```

### 923. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a70309a79c`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `11`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 11) T
```

### 924. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7066297462`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `12`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 12) T
```

### 925. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c9d0aa763d`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `13`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 13) T
```

### 926. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a5a24fa2b3`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `14`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 14) T
```

### 927. `root-derived-auto-schema-limit` `row-or-column-mismatch` `622ef6f037`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `15`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 15) T
```

### 928. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7933477e55`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `16`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 16) T
```

### 929. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ba55c8570e`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `17`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 17) T
```

### 930. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da27c9acea`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `18`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 18) T
```

### 931. `root-derived-auto-schema-limit` `row-or-column-mismatch` `48a28a0cc2`

- Auto schema: `True`
- Expected columns: `('b_49',)`
- Expected row count: `19`
- Actual columns: `('b_49',)`
- Actual row count: `20`

```sql
-- query
SELECT b_49 FROM (SELECT TRUE AS b_49 FROM dual LIMIT 19) T
```

### 932. `root-derived-auto-schema-limit` `row-or-column-mismatch` `36373c0eff`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `1`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 1) T
```

### 933. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d35144f4ce`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `2`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 2) T
```

### 934. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4356343285`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `3`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 3) T
```

### 935. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2b3ed8a7b1`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `4`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 4) T
```

### 936. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4d2270bd5a`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `5`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 5) T
```

### 937. `root-derived-auto-schema-limit` `row-or-column-mismatch` `8b849765d0`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `6`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 6) T
```

### 938. `root-derived-auto-schema-limit` `row-or-column-mismatch` `155a86a5ab`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `7`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 7) T
```

### 939. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c2aad69d2a`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `8`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 8) T
```

### 940. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9107ebe4c5`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `9`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 9) T
```

### 941. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3cc775ec6e`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `10`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 10) T
```

### 942. `root-derived-auto-schema-limit` `row-or-column-mismatch` `28932d6928`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `11`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 11) T
```

### 943. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf00953bcd`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `12`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 12) T
```

### 944. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0b5fc4067f`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `13`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 13) T
```

### 945. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da09792fcd`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `14`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 14) T
```

### 946. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d647b4f18c`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `15`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 15) T
```

### 947. `root-derived-auto-schema-limit` `row-or-column-mismatch` `2cc6d2231e`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `16`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 16) T
```

### 948. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1264975e43`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `17`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 17) T
```

### 949. `root-derived-auto-schema-limit` `row-or-column-mismatch` `bb5a852d6e`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `18`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 18) T
```

### 950. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c11ec005c6`

- Auto schema: `True`
- Expected columns: `('b_50',)`
- Expected row count: `19`
- Actual columns: `('b_50',)`
- Actual row count: `20`

```sql
-- query
SELECT b_50 FROM (SELECT TRUE AS b_50 FROM dual LIMIT 19) T
```

### 951. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c0f03a3c5c`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `1`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 1) T
```

### 952. `root-derived-auto-schema-limit` `row-or-column-mismatch` `264dada473`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `2`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 2) T
```

### 953. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9daf8032ee`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `3`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 3) T
```

### 954. `root-derived-auto-schema-limit` `row-or-column-mismatch` `45d4eb134c`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `4`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 4) T
```

### 955. `root-derived-auto-schema-limit` `row-or-column-mismatch` `34390382c6`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `5`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 5) T
```

### 956. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d21c5a6c16`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `6`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 6) T
```

### 957. `root-derived-auto-schema-limit` `row-or-column-mismatch` `b57becb2a5`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `7`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 7) T
```

### 958. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9403ec9df1`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `8`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 8) T
```

### 959. `root-derived-auto-schema-limit` `row-or-column-mismatch` `538c9f7a42`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `9`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 9) T
```

### 960. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7c7e5532c1`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `10`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 10) T
```

### 961. `root-derived-auto-schema-limit` `row-or-column-mismatch` `921170be35`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `11`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 11) T
```

### 962. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6fca0476ce`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `12`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 12) T
```

### 963. `root-derived-auto-schema-limit` `row-or-column-mismatch` `a0f6ce3cf0`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `13`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 13) T
```

### 964. `root-derived-auto-schema-limit` `row-or-column-mismatch` `41bb54a5db`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `14`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 14) T
```

### 965. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d8fbdbe424`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `15`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 15) T
```

### 966. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dfb4ccab1f`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `16`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 16) T
```

### 967. `root-derived-auto-schema-limit` `row-or-column-mismatch` `79ce6f8369`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `17`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 17) T
```

### 968. `root-derived-auto-schema-limit` `row-or-column-mismatch` `69e0016dae`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `18`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 18) T
```

### 969. `root-derived-auto-schema-limit` `row-or-column-mismatch` `d97bf757f9`

- Auto schema: `True`
- Expected columns: `('b_51',)`
- Expected row count: `19`
- Actual columns: `('b_51',)`
- Actual row count: `20`

```sql
-- query
SELECT b_51 FROM (SELECT TRUE AS b_51 FROM dual LIMIT 19) T
```

### 970. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f2784da8e6`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `1`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 1) T
```

### 971. `root-derived-auto-schema-limit` `row-or-column-mismatch` `fc0a48cac6`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `2`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 2) T
```

### 972. `root-derived-auto-schema-limit` `row-or-column-mismatch` `736286f50c`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `3`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 3) T
```

### 973. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cf165ec326`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `4`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 4) T
```

### 974. `root-derived-auto-schema-limit` `row-or-column-mismatch` `17aae936af`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `5`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 5) T
```

### 975. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c856e85628`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `6`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 6) T
```

### 976. `root-derived-auto-schema-limit` `row-or-column-mismatch` `47f99d27e9`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `7`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 7) T
```

### 977. `root-derived-auto-schema-limit` `row-or-column-mismatch` `da8a58e276`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `8`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 8) T
```

### 978. `root-derived-auto-schema-limit` `row-or-column-mismatch` `3fded409e7`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `9`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 9) T
```

### 979. `root-derived-auto-schema-limit` `row-or-column-mismatch` `042fec8200`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `10`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 10) T
```

### 980. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4e7c31253b`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `11`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 11) T
```

### 981. `root-derived-auto-schema-limit` `row-or-column-mismatch` `0514dedb30`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `12`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 12) T
```

### 982. `root-derived-auto-schema-limit` `row-or-column-mismatch` `ecaf4fc73e`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `13`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 13) T
```

### 983. `root-derived-auto-schema-limit` `row-or-column-mismatch` `1d13bd029c`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `14`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 14) T
```

### 984. `root-derived-auto-schema-limit` `row-or-column-mismatch` `c89b372332`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `15`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 15) T
```

### 985. `root-derived-auto-schema-limit` `row-or-column-mismatch` `66fe80c59d`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `16`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 16) T
```

### 986. `root-derived-auto-schema-limit` `row-or-column-mismatch` `87fa1c7b35`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `17`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 17) T
```

### 987. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4a6e3e55c1`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `18`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 18) T
```

### 988. `root-derived-auto-schema-limit` `row-or-column-mismatch` `f894b61781`

- Auto schema: `True`
- Expected columns: `('b_52',)`
- Expected row count: `19`
- Actual columns: `('b_52',)`
- Actual row count: `20`

```sql
-- query
SELECT b_52 FROM (SELECT TRUE AS b_52 FROM dual LIMIT 19) T
```

### 989. `root-derived-auto-schema-limit` `row-or-column-mismatch` `6fe7711463`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `1`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 1) T
```

### 990. `root-derived-auto-schema-limit` `row-or-column-mismatch` `892de7aff0`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `2`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 2) T
```

### 991. `root-derived-auto-schema-limit` `row-or-column-mismatch` `cd994cc063`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `3`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 3) T
```

### 992. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dca1bbe914`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `4`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 4) T
```

### 993. `root-derived-auto-schema-limit` `row-or-column-mismatch` `4ba10e524e`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `5`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 5) T
```

### 994. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5f5e50eb6a`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `6`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 6) T
```

### 995. `root-derived-auto-schema-limit` `row-or-column-mismatch` `dc082a22b9`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `7`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 7) T
```

### 996. `root-derived-auto-schema-limit` `row-or-column-mismatch` `5faadaf4c3`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `8`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 8) T
```

### 997. `root-derived-auto-schema-limit` `row-or-column-mismatch` `94d51b4bec`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `9`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 9) T
```

### 998. `root-derived-auto-schema-limit` `row-or-column-mismatch` `7d57cab412`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `10`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 10) T
```

### 999. `root-derived-auto-schema-limit` `row-or-column-mismatch` `89cdbcafec`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `11`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 11) T
```

### 1000. `root-derived-auto-schema-limit` `row-or-column-mismatch` `9029e0da11`

- Auto schema: `True`
- Expected columns: `('b_53',)`
- Expected row count: `12`
- Actual columns: `('b_53',)`
- Actual row count: `20`

```sql
-- query
SELECT b_53 FROM (SELECT TRUE AS b_53 FROM dual LIMIT 12) T
```

