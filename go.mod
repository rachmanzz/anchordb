module github.com/rachmanzz/anchordb

// Requires a Go toolchain with the net/http, crypto/x509, HTTP/2, crypto/tls and
// mime/multipart security fixes that landed through 1.27.4
// (see GO-2026-6603..GO-2026-6617, GO-2026-6809, GO-2026-6859). Go 1.21+ auto-downloads this toolchain.
go 1.26.0

toolchain go1.27.4


require github.com/google/uuid v1.6.0
