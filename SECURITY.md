# Security Considerations

## OpenAI Shim: Auto-Approved Permissions

The `/v1/chat/completions` endpoint creates ephemeral ACP sessions and is intended for trusted callers only.

**Risk:** Any caller with network access to the gateway can instruct the underlying agent to take host actions within the permissions supported by that agent.

**Mitigation (shipped):** `/v1/chat/completions`, along with the other long-lived routes (`/api/sessions/{id}/stream`, `/api/sessions/{id}/prompt`) and the short-lived management routes, requires a bearer `API_KEY` via `require_api_key` middleware (`src/auth.rs`, wired in `src/app.rs`). Set the `API_KEY` environment variable before exposing the gateway beyond a trusted network — if it is unset, the gateway logs a startup warning and runs with no authentication at all.

**Still recommended for v1:** Only expose this gateway on trusted networks even with `API_KEY` set. There is currently no rate limiting, and a single shared bearer token has no per-caller revocation or scoping.

## MCP Server URL Validation: SSRF and DNS Rebinding

`validate_mcp_url` (`src/handlers/sessions.rs`) rejects `http`/`https` MCP server URLs that target `localhost`, `.local` hostnames, or a private/internal/reserved IP address (RFC-1918, loopback, link-local/cloud-metadata, CGNAT/Tailscale `100.64.0.0/10`, multicast, and other special-use ranges — see `is_private_ipv4`/`is_private_ipv6`).

**What this check actually covers:** for domain-name MCP server URLs, the gateway resolves the hostname and checks every returned address *at the time the session/config request is validated* (bead .106). This blocks a directly-supplied private IP or a hostname that already resolves to one.

**Residual gap (TOCTOU / DNS rebinding):** the resolved address from that check is not pinned or reused — it is discarded. The actual MCP connection is made later, by a separate agent subprocess (see `src/agent.rs`), which independently re-resolves the same hostname when it connects. If an attacker controls DNS for the target hostname and changes the record between validation and connection (a low-TTL "DNS rebinding" record, for example resolving to a public IP at validation time and to `127.0.0.1`/`169.254.169.254`/an internal address at connect time), the subprocess can end up connecting somewhere this check never saw. In short: **validation happens at request time, not at connect time**, and nothing currently guarantees those are the same address.

A full fix requires either the gateway itself proxying/terminating the MCP connection (so the address it validated is the address it connects to), or pinning the validated IP and forcing the subprocess's connection to use it (e.g. via a resolver override or `--resolve`-style pinning) instead of re-resolving. Neither is implemented yet.

**Recommended mitigations until that lands:**
- Prefer running MCP servers as `stdio` subprocesses (subject to the command allowlist in `validate_stdio_command`) over remote `http`/`sse` URLs when the server doesn't need to be remote — this sidesteps DNS entirely.
- For remote MCP servers, prefer literal IP addresses or short-TTL-free, operator-controlled hostnames over third-party/attacker-influenced domains.
- Run the gateway and its agent subprocesses behind egress filtering (firewall/network policy) that blocks outbound connections to private/internal/cloud-metadata ranges regardless of what the application-level check decided — this closes the window even if DNS changes between validation and connect.
- Where feasible, route MCP `http`/`sse` traffic through an in-gateway forward proxy that re-validates (or better, reuses) the resolved address at actual connection time, rather than relying solely on the pre-connect check.

**For v1:** Only expose this gateway on trusted networks. Do NOT expose it publicly without both `API_KEY` authentication and egress filtering in front of it.
