/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package importer

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/pkg/xattr"
)

// Worker pool to handle file scanning in parallel
func (f *FSImporter) walkDir_worker(ctx context.Context, jobs <-chan string, results chan<- *importer.ScanResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		var (
			path string
			ok   bool
		)

		select {
		case path, ok = <-jobs:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		info, err := os.Lstat(path)
		if err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		isSymlink := info.Mode()&os.ModeSymlink != 0

		// fixup the rootdir if it happened to be a file
		if !info.IsDir() && path == f.rootDir {
			f.rootDir = filepath.Dir(f.rootDir)
		}

		var extendedAttributes []string
		if isSymlink {
			extendedAttributes, err = xattr.LList(path)
		} else {
			extendedAttributes, err = xattr.List(path)
		}
		if err != nil {
			results <- importer.NewScanError(path, err)
			continue
		}

		fileinfo := objects.FileInfoFromStat(info)
		fileinfo.Lusername, fileinfo.Lgroupname = f.lookupIDs(fileinfo.Uid(), fileinfo.Gid())

		var originFile string
		if isSymlink {
			originFile, err = os.Readlink(path)
			if err != nil {
				results <- importer.NewScanError(path, err)
				continue
			}
		}

		entrypath := toslash(path)

		results <- importer.NewScanRecord(entrypath, originFile, fileinfo, extendedAttributes,
			func() (io.ReadCloser, error) {
				return os.Open(path)
			})
		for _, attr := range extendedAttributes {
			results <- importer.NewScanXattr(entrypath, attr, objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					var data []byte
					var err error
					if isSymlink {
						data, err = xattr.LGet(path, attr)
					} else {
						data, err = xattr.Get(path, attr)
					}
					if err != nil {
						return nil, err
					}
					return io.NopCloser(bytes.NewReader(data)), nil
				})
		}
	}
}

func walkDir_addPrefixDirectories(root string, results chan<- *importer.ScanResult) {
	root = filepath.Dir(root)
	for {
		var finfo objects.FileInfo

		sb, err := os.Lstat(root)
		if err != nil {
			results <- importer.NewScanError(root, err)
			finfo = objects.FileInfo{
				Lname: filepath.Base(root),
				Lmode: os.ModeDir | 0755,
			}
		} else {
			finfo = objects.FileInfoFromStat(sb)
		}

		results <- importer.NewScanRecord(toslash(root), "", finfo, nil, nil)

		newroot := filepath.Dir(root)
		if newroot == root { // base case for "/" or "C:\"
			break
		}
		root = newroot
	}

	if runtime.GOOS == "windows" {
		finfo := objects.FileInfo{
			Lname: "/",
			Lmode: os.ModeDir | 0755,
		}
		results <- importer.NewScanRecord("/", "", finfo, nil, nil)
	}
}
