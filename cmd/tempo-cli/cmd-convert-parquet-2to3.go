package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet3"
	"github.com/parquet-go/parquet-go"
)

type convertParquet2to3 struct {
	In               string   `arg:"" help:"The input parquet file to read from."`
	Out              string   `arg:"" help:"The output folder to write to."`
	DedicatedColumns []string `arg:"" help:"List of dedicated columns to convert"`
}

func (cmd *convertParquet2to3) Run() error {
	// open the in file
	ctx := context.Background()

	blockIsHere := "<some path>"

	cmd.In = filepath.Join(blockIsHere, "data.parquet")

	in, err := os.Open(cmd.In)
	if err != nil {
		return err
	}
	defer in.Close()

	inStat, err := in.Stat()
	if err != nil {
		return err
	}

	pf, err := parquet.OpenFile(in, inStat.Size())
	if err != nil {
		return err
	}

	// create out block
	cmd.Out = "./out2"
	outR, outW, _, err := local.New(&local.Config{
		Path: cmd.Out,
	})
	if err != nil {
		return err
	}

	meta := &backend.BlockMeta{}
	b, err := os.ReadFile(filepath.Join(blockIsHere, "meta.json"))
	if err != nil {
		return err
	}

	err = json.Unmarshal(b, meta)
	if err != nil {
		return err
	}

	blockCfg := &common.BlockConfig{
		BloomFP:             0.99,
		BloomShardSizeBytes: 100 * 1024,
		Version:             vparquet3.VersionString,
		RowGroupSizeBytes:   10 * 1024 * 1024,
		DedicatedColumns:    meta.DedicatedColumns,
	}

	// create iterator over in file
	iter := &parquetIterator{
		meta: meta,
		r:    parquet.NewGenericReader[*vparquet3.Trace](pf),
	}

	_, err = vparquet3.CreateBlock(ctx, blockCfg, meta, iter, backend.NewReader(outR), backend.NewWriter(outW))
	if err != nil {
		return err
	}

	return nil
}

type parquetIterator struct {
	r    *parquet.GenericReader[*vparquet3.Trace]
	meta *backend.BlockMeta
	i    int
}

func (i *parquetIterator) Next(_ context.Context) (common.ID, *tempopb.Trace, error) {
	traces := make([]*vparquet3.Trace, 1)

	i.i++
	if i.i%1000 == 0 {
		fmt.Println(i.i)
	}

	_, err := i.r.Read(traces)
	if errors.Is(err, io.EOF) {
		return nil, nil, io.EOF
	}
	if err != nil {
		return nil, nil, err
	}

	pqTrace := traces[0]
	pbTrace := vparquet3.ParquetTraceToTempopbTrace(i.meta, pqTrace)
	return pqTrace.TraceID, pbTrace, nil
}

func (i *parquetIterator) Close() {
	_ = i.r.Close()
}
