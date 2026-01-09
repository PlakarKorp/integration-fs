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

package exporter

import (
	"context"
	"fmt"

	"golang.org/x/sync/singleflight"

	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
)

type FSExporter struct {
	opts    *connectors.Options
	rootDir string

	hlCreate singleflight.Group // key -> ensures canonical exists, returns canonical abs path
	hlCanon  sync.Map           // key -> canonical abs path string
	hlMu     sync.Map           // key -> *sync.Mutex (serialize os.Link per key)
}

func init() {
	exporter.Register("fs", location.FLAG_LOCALFS, NewFSExporter)
}

func NewFSExporter(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (exporter.Exporter, error) {
	location := config["location"]
	rootDir := strings.TrimPrefix(location, name+"://")

	return &FSExporter{
		opts:    opts,
		rootDir: rootDir,
	}, nil
}

func (p *FSExporter) Root() string {
	return p.rootDir
}

func (p *FSExporter) Origin() string {
	return p.opts.Hostname
}

func (p *FSExporter) Type() string {
	return "fs"
}

func (p *FSExporter) Ping(ctx context.Context) error {
	return nil
}

func (p *FSExporter) Close(ctx context.Context) error {
	return nil
}

type dirPerm struct {
	Pathname string
	Fileinfo objects.FileInfo
}

func (p *FSExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(p.opts.MaxConcurrency)

	var mu sync.Mutex
	var dirReady map[string]chan struct{} = make(map[string]chan struct{})
	var dirPerms []dirPerm = make([]dirPerm, 1024)

	markInflight := func(pathname string) {
		mu.Lock()
		dirReady[pathname] = make(chan struct{})
		mu.Unlock()
	}
	markReady := func(pathname string) {
		mu.Lock()
		ch := dirReady[pathname]
		delete(dirReady, pathname)
		mu.Unlock()
		close(ch)
	}
	waitParent := func(pathname string) {
		if pathname == "/" {
			return
		}
		mu.Lock()
		ch, exists := dirReady[filepath.Dir(pathname)]
		mu.Unlock()
		if !exists {
			return
		}
		<-ch
	}

	i := 1
	for record := range records {
		if i%1000 == 0 && ctx.Err() != nil {
			break
		}
		if record.Err != nil {
			results <- record.Ok()
			continue
		}

		if record.FileInfo.IsDir() {
			markInflight(record.Pathname)
			g.Go(func() error {
				waitParent(record.Pathname)
				if err := p.directory(record.Pathname); err != nil {
					return err
				}

				// later patching
				mu.Lock()
				dirPerms = append(dirPerms, dirPerm{
					Pathname: record.Pathname,
					Fileinfo: record.FileInfo,
				})
				mu.Unlock()

				markReady(record.Pathname)
				results <- record.Ok()
				return nil
			})
			continue
		}

		g.Go(func() error {
			waitParent(record.Pathname)

			var err error
			if record.Target != "" {
				err = p.symlink(record.Target, record.Pathname)
			} else {
				err = p.file(record.Pathname, record.Reader, record.FileInfo)
			}
			if err == nil {
				err = p.permissions(record.Pathname, record.FileInfo)
			}

			if err != nil {
				results <- record.Error(err)
			} else {
				results <- record.Ok()
			}
			record.Close()
			return nil
		})
	}
	err := g.Wait()

	for i := len(dirPerms); i > 0; i-- {
		if err := p.permissions(dirPerms[i-1].Pathname, dirPerms[i-1].Fileinfo); err != nil {
			return err
		}
	}

	return err
}

func (p *FSExporter) directory(pathname string) error {
	return os.Mkdir(filepath.Join(p.rootDir, pathname), 0700)
}

func (p *FSExporter) symlink(pathname string, target string) error {
	return os.Symlink(target, filepath.Join(p.rootDir, pathname))
}

func (p *FSExporter) hardlink(pathname string, fp io.Reader, fileinfo objects.FileInfo) error {
	key := fmt.Sprintf("%d:%d", fileinfo.Dev(), fileinfo.Ino())

	v, err, _ := p.hlCreate.Do(key, func() (any, error) {
		if v, ok := p.hlCanon.Load(key); ok {
			return v.(string), nil
		}
		if err := p.writeAtomic(pathname, fp); err != nil {
			return "", err
		}
		p.hlCanon.Store(key, filepath.Join(p.rootDir, pathname))
		return pathname, nil
	})
	if err != nil {
		return err
	}
	canonPath := v.(string)

	// If we are not the canonical path, create a hardlink
	pathname = filepath.Join(p.rootDir, pathname)
	if canonPath != pathname {
		if err := os.Link(canonPath, pathname); err != nil {
			return err
		}
	}

	return nil
}

func (p *FSExporter) file(pathname string, fp io.Reader, fileinfo objects.FileInfo) error {
	if fileinfo.Nlink() > 1 {
		return p.hardlink(pathname, fp, fileinfo)
	}
	return p.writeAtomic(pathname, fp)
}

func (p *FSExporter) writeAtomic(pathname string, fp io.Reader) error {
	pathname = filepath.Join(p.rootDir, pathname)
	parent := filepath.Dir(pathname)

	tmp, err := os.CreateTemp(parent, ".plakar-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := io.Copy(tmp, fp); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpName, pathname); err != nil {
		return err
	}

	ok = true
	return nil
}

func (p *FSExporter) permissions(pathname string, fileinfo objects.FileInfo) error {
	pathname = filepath.Join(p.rootDir, pathname)

	if fileinfo.Mode()&os.ModeSymlink == 0 {
		// Preserve all permission bits including setuid (04000), setgid (02000), and sticky bit (01000)
		// Use the full mode which includes these special bits, not just Mode().Perm()
		if err := os.Chmod(pathname, fileinfo.Mode().Perm()|fileinfo.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky)); err != nil {
			return err
		}
	}
	if os.Geteuid() == 0 {
		if fileinfo.Mode()&os.ModeSymlink != 0 {
			if err := os.Lchown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
				return err
			}
		} else {
			if err := os.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
				return err
			}
		}
	}
	if err := Lutimes(pathname, fileinfo.ModTime(), fileinfo.ModTime()); err != nil {
		return err
	}
	return nil
}
