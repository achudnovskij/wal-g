package storage

import (
	"context"
	"errors"
	"io"
)

// ErrObjectExists is returned by ConditionalPutObjectFolder.PutObjectIfAbsent
// when a live object already exists at the target path, so the conditional
// (create-only) write was rejected by the backend. Callers racing several
// uploaders for the same key treat this as success ("someone already wrote it").
var ErrObjectExists = errors.New("object already exists")

// ErrConditionalPutUnsupported is returned when a folder advertises the
// ConditionalPutObjectFolder interface but the active backend cannot perform a
// conditional write (e.g. a multistorage folder whose underlying storage has no
// If-None-Match support). Callers should fall back to a plain PutObject.
var ErrConditionalPutUnsupported = errors.New("conditional put not supported by backend")

// ConditionalPutObjectFolder is an OPTIONAL capability implemented by backends
// that support an atomic "create if absent" write (S3 If-None-Match: *, GCS
// ifGenerationMatch=0, Azure If-None-Match: *). Callers type-assert a Folder to
// this interface and fall back to PutObject when it is absent.
type ConditionalPutObjectFolder interface {
	// PutObjectIfAbsent uploads content only if no object exists at name. It
	// returns ErrObjectExists if the backend rejected the write because the
	// object already exists, or ErrConditionalPutUnsupported if the active
	// backend cannot perform a conditional write.
	PutObjectIfAbsent(ctx context.Context, name string, content io.Reader) error
}
