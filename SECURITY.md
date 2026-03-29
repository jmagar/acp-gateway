# Security Considerations

## OpenAI Shim: Auto-Approved Permissions

The `/v1/chat/completions` endpoint creates ephemeral ACP sessions and is intended for trusted callers only.

**Risk:** Any caller with network access to the gateway can instruct the underlying agent to take host actions within the permissions supported by that agent.

**Mitigation planned for v2:**
- Tower middleware with API key or JWT authentication
- Permission request forwarding in the native `/api/sessions` API

**For v1:** Only expose this gateway on trusted networks. Do NOT expose it publicly without authentication in front of it.
