package v2

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFullFilename(t *testing.T) {
	tests := []struct {
		name     string
		b        *v2AppendBlock
		expected string
	}{
		{
			name: "legacy",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v0", backend.EncNone, ""),
				filepath: "/blerg",
			},
			expected: "/blerg/123e4567-e89b-12d3-a456-426614174000:foo",
		},
		{
			name: "ez-mode",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v1", backend.EncNone, ""),
				filepath: "/blerg",
			},
			expected: "/blerg/123e4567-e89b-12d3-a456-426614174000:foo:v1:none",
		},
		{
			name: "nopath",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v1", backend.EncNone, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v1:none",
		},
		{
			name: "gzip",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncGZIP, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:gzip",
		},
		{
			name: "lz41M",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncLZ4_1M, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:lz4-1M",
		},
		{
			name: "lz4256k",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncLZ4_256k, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:lz4-256k",
		},
		{
			name: "lz4M",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncLZ4_4M, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:lz4",
		},
		{
			name: "lz64k",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncLZ4_64k, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:lz4-64k",
		},
		{
			name: "snappy",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncSnappy, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:snappy",
		},
		{
			name: "zstd",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v2", backend.EncZstd, ""),
				filepath: "",
			},
			expected: "123e4567-e89b-12d3-a456-426614174000:foo:v2:zstd",
		},
		{
			name: "data encoding",
			b: &v2AppendBlock{
				meta:     backend.NewBlockMeta("foo", uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"), "v1", backend.EncNone, "dataencoding"),
				filepath: "/blerg",
			},
			expected: "/blerg/123e4567-e89b-12d3-a456-426614174000:foo:v1:none:dataencoding",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := tc.b.fullFilename()
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestAdjustTimeRangeForSlack(t *testing.T) {
	a := &v2AppendBlock{
		meta: &backend.BlockMeta{
			TenantID: "test",
		},
		ingestionSlack: 2 * time.Minute,
	}

	// test happy path
	start := uint32(time.Now().Unix())
	end := uint32(time.Now().Unix())
	actualStart, actualEnd := a.adjustTimeRangeForSlack(start, end, 0)
	assert.Equal(t, start, actualStart)
	assert.Equal(t, end, actualEnd)

	// test start out of range
	now := uint32(time.Now().Unix())
	start = uint32(time.Now().Add(-time.Hour).Unix())
	end = uint32(time.Now().Unix())
	actualStart, actualEnd = a.adjustTimeRangeForSlack(start, end, 0)
	assert.Equal(t, now, actualStart)
	assert.Equal(t, end, actualEnd)

	// test end out of range
	now = uint32(time.Now().Unix())
	start = uint32(time.Now().Unix())
	end = uint32(time.Now().Add(time.Hour).Unix())
	actualStart, actualEnd = a.adjustTimeRangeForSlack(start, end, 0)
	assert.Equal(t, start, actualStart)
	assert.Equal(t, now, actualEnd)

	// test additional start slack honored
	start = uint32(time.Now().Add(-time.Hour).Unix())
	end = uint32(time.Now().Unix())
	actualStart, actualEnd = a.adjustTimeRangeForSlack(start, end, time.Hour)
	assert.Equal(t, start, actualStart)
	assert.Equal(t, end, actualEnd)
}

func TestAppend(t *testing.T) {
	blockID := uuid.New()
	block, err := newAppendBlock(blockID, testTenantID, t.TempDir(), backend.EncSnappy, "v2", 0)
	require.NoError(t, err, "unexpected error creating block")

	//v2block := block.(*v2AppendBlock)

	numMsgs := 100
	reqs := make([]*tempopb.Trace, 0, numMsgs)
	for i := 0; i < numMsgs; i++ {
		req := test.MakeTrace(rand.Int()%1000, []byte{0x01})
		reqs = append(reqs, req)
		bReq, err := proto.Marshal(req)
		require.NoError(t, err)
		err = block.Append([]byte{0x01}, bReq, 0, 0)
		require.NoError(t, err, "unexpected error writing req")
	}

	// jpe - bad test - use iterator from block
	// records := v2block.appender.Records()
	// file, err := v2block.file()
	// require.NoError(t, err)

	// dataReader, err := NewDataReader(backend.NewContextReaderWithAllReader(file), backend.EncNone) // jpe internal?
	// require.NoError(t, err)
	// iterator := NewRecordIterator(records, dataReader, NewObjectReaderWriter()) // jpe internal?
	// defer iterator.Close()
	// i := 0

	// for {
	// 	_, bytesObject, err := iterator.Next(context.Background())
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	require.NoError(t, err)

	// 	req := &tempopb.Trace{}
	// 	err = proto.Unmarshal(bytesObject, req)
	// 	require.NoError(t, err)

	// 	require.True(t, proto.Equal(req, reqs[i]))
	// 	i++
	// }
	// require.Equal(t, numMsgs, i)
}

func TestPartialBlock(t *testing.T) {
	// jpe combine with above test
	blockID := uuid.New()

	// create partially corrupt block
	block, err := newAppendBlock(blockID, testTenantID, t.TempDir(), backend.EncSnappy, "v2", 0)
	require.NoError(t, err, "unexpected error creating block")

	objects := 10
	for i := 0; i < objects; i++ {
		id := make([]byte, 16)
		rand.Read(id)
		obj := test.MakeTrace(rand.Int()%10, id)
		bObj, err := proto.Marshal(obj)
		require.NoError(t, err)

		err = block.Append(id, bObj, 0, 0)
		require.NoError(t, err, "unexpected error writing req")
	}
	v2Block := block.(*v2AppendBlock)

	appendFile, err := os.OpenFile(v2Block.fullFilename(), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	require.NoError(t, err)
	_, err = appendFile.Write([]byte{0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01})
	require.NoError(t, err)
	err = appendFile.Close()
	require.NoError(t, err)
}

func TestParseFilename(t *testing.T) {
	tests := []struct {
		name                 string
		filename             string
		expectUUID           uuid.UUID
		expectTenant         string
		expectedVersion      string
		expectedEncoding     backend.Encoding
		expectedDataEncoding string
		expectError          bool
	}{
		{
			name:                 "version, enc snappy and dataencoding",
			filename:             "123e4567-e89b-12d3-a456-426614174000:foo:v2:snappy:dataencoding",
			expectUUID:           uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectTenant:         "foo",
			expectedVersion:      "v2",
			expectedEncoding:     backend.EncSnappy,
			expectedDataEncoding: "dataencoding",
		},
		{
			name:                 "version, enc none and dataencoding",
			filename:             "123e4567-e89b-12d3-a456-426614174000:foo:v2:none:dataencoding",
			expectUUID:           uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectTenant:         "foo",
			expectedVersion:      "v2",
			expectedEncoding:     backend.EncNone,
			expectedDataEncoding: "dataencoding",
		},
		{
			name:                 "empty dataencoding",
			filename:             "123e4567-e89b-12d3-a456-426614174000:foo:v2:snappy",
			expectUUID:           uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectTenant:         "foo",
			expectedVersion:      "v2",
			expectedEncoding:     backend.EncSnappy,
			expectedDataEncoding: "",
		},
		{
			name:                 "empty dataencoding with semicolon",
			filename:             "123e4567-e89b-12d3-a456-426614174000:foo:v2:snappy:",
			expectUUID:           uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			expectTenant:         "foo",
			expectedVersion:      "v2",
			expectedEncoding:     backend.EncSnappy,
			expectedDataEncoding: "",
		},
		{
			name:        "path fails",
			filename:    "/blerg/123e4567-e89b-12d3-a456-426614174000:foo",
			expectError: true,
		},
		{
			name:        "no :",
			filename:    "123e4567-e89b-12d3-a456-426614174000",
			expectError: true,
		},
		{
			name:        "empty string",
			filename:    "",
			expectError: true,
		},
		{
			name:        "bad uuid",
			filename:    "123e4:foo",
			expectError: true,
		},
		{
			name:        "no tenant",
			filename:    "123e4567-e89b-12d3-a456-426614174000:",
			expectError: true,
		},
		{
			name:        "no version",
			filename:    "123e4567-e89b-12d3-a456-426614174000:test::none",
			expectError: true,
		},
		{
			name:        "wrong splits - 6",
			filename:    "123e4567-e89b-12d3-a456-426614174000:test:test:test:test:test",
			expectError: true,
		},
		{
			name:        "wrong splits - 3",
			filename:    "123e4567-e89b-12d3-a456-426614174000:test:test",
			expectError: true,
		},
		{
			name:        "wrong splits - 1",
			filename:    "123e4567-e89b-12d3-a456-426614174000",
			expectError: true,
		},
		{
			name:        "bad encoding",
			filename:    "123e4567-e89b-12d3-a456-426614174000:test:v1:asdf",
			expectError: true,
		},
		{
			name:        "ez-mode old format",
			filename:    "123e4567-e89b-12d3-a456-426614174000:foo",
			expectError: true,
		},
		{
			name:        "deprecated version",
			filename:    "123e4567-e89b-12d3-a456-426614174000:foo:v1:snappy",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actualUUID, actualTenant, actualVersion, actualEncoding, actualDataEncoding, err := ParseFilename(tc.filename)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectUUID, actualUUID)
			require.Equal(t, tc.expectTenant, actualTenant)
			require.Equal(t, tc.expectedEncoding, actualEncoding)
			require.Equal(t, tc.expectedVersion, actualVersion)
			require.Equal(t, tc.expectedDataEncoding, actualDataEncoding)
		})
	}
}