package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListAllExecutableFiles(t *testing.T) {
	// TODO
	files, err := ListAllExecutableFiles("/usr/bin")
	assert.Nil(t, err)
	for _, file := range files {
		fmt.Println(file)
	}
}
