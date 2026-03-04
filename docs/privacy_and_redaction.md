# Privacy And Redaction

This project is designed for personal portfolio analysis. Real HL exports and generated
artifacts can contain highly sensitive financial information.

## Never Share Publicly

- Raw CSV exports from HL
- Investment report PDFs
- Generated DuckDB files
- Screenshots showing balances, account names, account numbers, or holdings
- Browser logs captured during authenticated sessions

## Safe Reproduction Checklist

Before sharing data privately or using it in a bug report:

1. Remove names, account numbers, and addresses.
2. Replace exact balances and contribution amounts unless they are required to reproduce the issue.
3. Remove rows unrelated to the failing parser or metric.
4. Prefer synthetic data shaped like the real export instead of a redacted real export.
5. Verify the file still reproduces the issue after redaction.

## Recommended Default

Use the sanitized demo dataset in `examples/demo_data/` for examples, screenshots, and public bug reports.
