//go:build windows
// +build windows

package utils

// SyncDir Windows doesn't support syncing directories to the file system. See
// https://github.com/dgraph-io/badger/issues/699#issuecomment-504133587 for more details.
func SyncDir(dir string) error {
	return nil
}
