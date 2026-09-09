# DX-S06 Scalar CSP Amendment

## Status

Approved amendment to `docs/superpowers/specs/2026-09-09-dx-s06-generated-openapi-scalar-design.md`.

## Decision

Scalar integration must not weaken Erabi's global Content Security Policy.

The existing global policy remains authoritative for all ordinary API and browser responses, including:

```text
script-src 'self'
style-src 'self'
```

`/api/docs` may use a route-scoped CSP replacement only to the minimum extent required for Scalar rendering.

## Required order of preference

1. Keep `script-src 'self'` and use self-hosted Scalar JavaScript.
2. Avoid inline initialization JavaScript when possible; if Scalar initialization cannot be performed without inline script, use one nonce-scoped initialization script and a docs-only CSP nonce. Never use `script-src 'unsafe-inline'` or `unsafe-eval`.
3. For Scalar style attributes, first prefer:

```text
style-src 'self'; style-src-attr 'unsafe-inline'
```

with nonce handling for generated `<style>` elements if required.
4. If verified Scalar rendering requires the broader documented fallback, `/api/docs` alone may use:

```text
style-src 'self' 'unsafe-inline'
```

5. Global `'unsafe-inline'` remains prohibited.

## Asset and network constraints

- Scalar JavaScript must be served locally from the binary, preferably at `/assets/scalar.js`.
- No CDN, external proxy, Scalar Registry, Scalar Agent, or runtime network dependency is allowed.
- No bearer token or other credential may be prefilled into Scalar configuration.
- Scalar must consume the same generated OpenAPI document as `/api/v1/openapi.json`; inline document content is preferred over a second protected fetch.

## Security boundary

`openapi_enabled` and existing bearer authentication remain authoritative for `/api/docs` exactly as for `/api/v1/openapi.json`:

```text
Loopback default:
  docs enabled

Remote default:
  bearer authentication applies
  authenticated request receives 404 OPENAPI_DISABLED

Remote explicit opt-in:
  bearer authentication applies
  authenticated request may access docs
```

The docs CSP override must replace the response CSP for `/api/docs`; it must not add a second conflicting CSP header. All other global security headers remain present.
