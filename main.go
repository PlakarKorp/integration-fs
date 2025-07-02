package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/PlakarKorp/go-kloset-sdk/sdk"
	"github.com/PlakarKorp/integration-fs/fs"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <scan-dir>\n", os.Args[0])
		os.Exit(1)
	}

	argStr := os.Args[1]
	argStr = strings.TrimPrefix(argStr, "map[")
	argStr = strings.TrimSuffix(argStr, "]")
	scanMap := make(map[string]string)
	for _, pair := range strings.Fields(argStr) {
		kv := strings.SplitN(pair, ":", 2)
		if len(kv) == 2 {
			scanMap[kv[0]] = kv[1]
		}
	}

	fsStorage, err := fs.NewStore(context.Background(), "fis", scanMap)
	if err != nil {
		panic(err)
	}

	if err := sdk.RunStorage(fsStorage); err != nil {
		panic(err)
	}
}
