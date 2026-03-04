# Security Policy

## Reporting

Do not open a public issue with credentials, session cookies, account exports, investment
reports, screenshots, or any other personal financial data.

If you find a security issue:

1. Report it to the maintainer through a private channel if one is available.
2. Include the smallest possible reproduction and prefer sanitized data.
3. Wait for confirmation before publishing details publicly.

## Sensitive Material

Treat the following as sensitive by default:

- HL CSV exports
- Investment report PDFs
- DuckDB files generated from real account data
- Browser automation logs or screenshots captured during login flows

Use the synthetic demo dataset in `examples/demo_data/` for public discussion whenever possible.
