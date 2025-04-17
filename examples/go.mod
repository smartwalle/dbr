module github.com/smartwalle/dbr/examples

go 1.23.0

replace github.com/smartwalle/dbr v0.0.0 => ../

require (
	github.com/redis/go-redis/v9 v9.7.0
	github.com/smartwalle/dbr v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/google/uuid v1.6.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
)
