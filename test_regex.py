import re
text = """
  /// @refine(confidence >= 0.0)
  record dbi0-app-state-value {
    body: bytes,
    revision: s64,
    updated-at: timestamp,
    confidence: score,
  }
"""
RECORD_BLOCK_RE = re.compile(
    r"(?:^[ \t]*///\s*@refine\(([^)]+)\)\s*\n)?[ \t]*record\s+([a-z0-9][a-z0-9-]*)\s*\{([^}]*)\}",
    re.MULTILINE
)
for m in RECORD_BLOCK_RE.finditer(text):
    print("Rule:", repr(m.group(1)))
    print("Name:", repr(m.group(2)))
