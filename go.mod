module github.com/rachmanzz/anchordb

// Requires a Go toolchain with the HTTP/2, net/http, crypto/tls and
// mime/multipart security fixes that landed in 1.27.2
// (see GO-2026-6603..GO-2026-6617). Go 1.21+ auto-downloads this toolchain.
go 1.26.0

toolchain go1.27.2


require github.com/google/uuid v1.6.0
