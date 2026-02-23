import re
from dataclasses import dataclass

wit_text = """
  /// @refine(namespace != "")
  record dbi0-app-state-key {
    namespace: utf8,
    key: utf8,
  }

  /// @refine(confidence >= 0.0)
  record dbi0-app-state-value {
    body: bytes,
    revision: s64,
    updated-at: timestamp,
    confidence: score,
  }
"""

@dataclass
class WitField:
    name: str
    wit_type: str

@dataclass
class WitRecord:
    name: str
    refine_rule: str | None
    fields: list[WitField]

# Regex to match:
# 1. Optional /// @refine(...) comment
# 2. record name { ... } block
RECORD_BLOCK_RE = re.compile(
    r"(?:///\s*@refine\(([^)]+)\)\s*)?^\s*record\s+([a-z0-9][a-z0-9-]*)\s*\{([^}]*)\}",
    re.MULTILINE
)
FIELD_RE = re.compile(r"^\s*([a-z0-9-]+)\s*:\s*([a-z0-9-]+(?:<[^>]+>)?)\s*,", re.MULTILINE)

records = []
for match in RECORD_BLOCK_RE.finditer(wit_text):
    refine_rule = match.group(1)
    rec_name = match.group(2)
    body = match.group(3)
    
    fields = []
    for fmatch in FIELD_RE.finditer(body):
        fields.append(WitField(name=fmatch.group(1), wit_type=fmatch.group(2)))
        
    records.append(WitRecord(name=rec_name, refine_rule=refine_rule, fields=fields))

for r in records:
    print(r)
