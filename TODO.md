# TODO

## Bidirectional Visual Overrides
Partially implemented:
- highlight_elements tool: sends color/ID state to viewer
- viewer.html polls highlight://{urn} resource every 2s
- Remaining: LLM-driven isolation commands (hide all except
  selected elements) — not yet implemented

## Known Limitations
- replicate_folders is sequential (one HTTP call per folder).
  Parallelization deferred — typical folder structures are small.
- preview_model only works in Claude Desktop, not Copilot Studio
  or Teams (platform limitation, not a bug).
