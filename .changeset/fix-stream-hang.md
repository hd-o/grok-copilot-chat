---
"grok-copilot-chat": patch
---

Fix chat hangs stuck on Working…/Reasoning… by keeping request abort and timeout active for the full response stream, and by closing thinking sequences when reasoning ends.
