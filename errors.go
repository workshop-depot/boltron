package boltron

import (
	bolt "github.com/coreos/bbolt"
)

// These errors can be returned when opening or calling methods on a DB.
var (
	// ErrDatabaseNotOpen is returned when a DB instance is accessed before it
	// is opened or after it is closed.
	ErrDatabaseNotOpen = bolt.ErrDatabaseNotOpen

	// ErrDatabaseOpen is returned when opening a database that is
	// already open.
	ErrDatabaseOpen = bolt.ErrDatabaseOpen

	// ErrInvalid is returned when both meta pages on a database are invalid.
	// This typically occurs when a file is not a bolt database.
	ErrInvalid = bolt.ErrInvalid

	// ErrVersionMismatch is returned when the data file was created with a
	// different version of Bolt.
	ErrVersionMismatch = bolt.ErrVersionMismatch

	// ErrChecksum is returned when either meta page checksum does not match.
	ErrChecksum = bolt.ErrChecksum

	// ErrTimeout is returned when a database cannot obtain an exclusive lock
	// on the data file after the timeout passed to Open().
	ErrTimeout = bolt.ErrTimeout
)

// These errors can occur when beginning or committing a Tx.
var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = bolt.ErrTxNotWritable

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = bolt.ErrTxClosed

	// ErrDatabaseReadOnly is returned when a mutating transaction is started on a
	// read-only database.
	ErrDatabaseReadOnly = bolt.ErrDatabaseReadOnly
)

// These errors can occur when putting or deleting a value or a bucket.
var (
	// ErrBucketNotFound is returned when trying to access a bucket that has
	// not been created yet.
	ErrBucketNotFound = bolt.ErrBucketNotFound

	// ErrBucketExists is returned when creating a bucket that already exists.
	ErrBucketExists = bolt.ErrBucketExists

	// ErrBucketNameRequired is returned when creating a bucket with a blank name.
	ErrBucketNameRequired = bolt.ErrBucketNameRequired

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = bolt.ErrKeyRequired

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = bolt.ErrKeyTooLarge

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = bolt.ErrValueTooLarge

	// ErrIncompatibleValue is returned when trying create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	ErrIncompatibleValue = bolt.ErrIncompatibleValue
)
