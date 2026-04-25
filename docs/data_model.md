# Data Model

The platform uses a medallion-style model:

- Bronze stores landed source-aligned records.
- Silver standardizes, validates, and quarantines records.
- Gold provides domain marts and analytical outputs.
- Platinum provides serving-friendly summaries.
