# Third-party reference material

The MIT license in this repository covers SDK code and original project material.
It does not relicense the upstream Exness API specification at
`contracts/trading.public.yaml`. That file is an unmodified public reference
snapshot, attributed to Exness; provenance and its source URL are recorded in
`contracts/README.md`. No ownership or endorsement by Exness is claimed.

The synthetic Ed25519 seed and public key used in tests are the published RFC 8032,
section 7.1, TEST 1 values: https://www.rfc-editor.org/rfc/rfc8032#section-7.1.
The EXN protocol signing fixture was independently computed with OpenSSL using
those public test values. It is not a live account credential.
