package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	// indexの作成
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// fileの現在サイズを取得・保存
	idx.size = uint64(fi.Size())

	// 一度メモリにマップされたファイルはサイズを変更できないため、
	// 先にファイルを最大インデックまで拡大
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	// fileをメモリへマップする
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// 永続化されたファイルへ同期
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}

	// 永続化されたファイル内容を安定したストレージへ同期
	if err := i.file.Sync(); err != nil {
		return err
	}

	// マッピング前にファイルサイズまで空領域を設定しているので、
	// 永続化されたファイルを実際のデータ量まで切り詰める
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	// エントリを書き込む領域があるかチェック
	if i.isMaxend() {
		return io.EOF
	}

	// オフセットと位置をエンコード、書き込み
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// 次の位置を書き込み
	i.size += uint64(entWidth)
	return nil
}

func (i *index) isMaxend() bool {
	return uint64(len(i.mmap)) < i.size+entWidth
}

func (i *index) Name() string {
	return i.file.Name()
}
