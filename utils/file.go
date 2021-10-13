package utils

import (
	"io/fs"
	"os"
	"path/filepath"
)

const executablePerm = 0111

func isExecutable(perm fs.FileMode) bool {
	return perm&executablePerm == executablePerm
}

// ListAllExecutableFiles returns all the executable files in the given path
func ListAllExecutableFiles(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	files := []string{}

	// scan all executable files
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		perm := info.Mode().Perm()
		if isExecutable(perm) {
			files = append(files, filepath.Join(path, entry.Name()))
		}
	}
	return files, nil
}
