package v2

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/grafana/tempo/pkg/model/trace"
	"github.com/grafana/tempo/pkg/tempopb"
)

// SegmentDecoder maintains the relationship between distributor -> ingester
// Segment format:
// | uint32 | uint32 | variable length          |
// | start  | end    | marshalled tempopb.Trace |
// start and end are unix epoch seconds
type SegmentDecoder struct {
}

var segmentDecoder = &SegmentDecoder{}

func NewSegmentDecoder() *SegmentDecoder {
	return segmentDecoder
}

func (d *SegmentDecoder) PrepareForWrite(trace *tempopb.Trace, start uint32, end uint32) ([]byte, error) {
	return marshalWithStartEnd(trace, start, end)
}

func (d *SegmentDecoder) PrepareForRead(segments [][]byte) (*tempopb.Trace, error) {
	combiner := trace.NewCombiner()
	for i, obj := range segments {
		obj, _, _, err := stripStartEnd(obj)
		if err != nil {
			return nil, fmt.Errorf("error stripping start/end: %w", err)
		}

		// jpe ownership is weird here. who puts back?
		t := tempopb.TraceFromVTPool()
		err = t.UnmarshalVT(obj)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling trace: %w", err)
		}

		combiner.ConsumeWithFinal(t, i == len(segments)-1)
	}

	combinedTrace, _ := combiner.Result()

	return combinedTrace, nil
}

// ToObject creates a byte slice that can be interpreted by ObjectDecoder in this package
// see object_decoder.go for details on the format.
func (d *SegmentDecoder) ToObject(segments [][]byte) ([]byte, error) {
	// strip start/end from individual segments and place it in a TraceBytesWrapper
	var err error
	var minStart, maxEnd uint32
	minStart = math.MaxUint32

	for i, b := range segments {
		var start, end uint32

		segments[i], start, end, err = stripStartEnd(b)
		if err != nil {
			return nil, err
		}
		if start < minStart {
			minStart = start
		}
		if end > maxEnd {
			maxEnd = end
		}
	}

	return marshalWithStartEnd(&tempopb.TraceBytes{
		Traces: segments,
	}, minStart, maxEnd)
}

func (d *SegmentDecoder) FastRange(buff []byte) (uint32, uint32, error) {
	_, start, end, err := stripStartEnd(buff)
	return start, end, err
}

type vtProto interface {
	SizeVT() int
	MarshalToSizedBufferVT(dAtA []byte) (int, error)
}

const uint32Size = 4

func marshalWithStartEnd(obj vtProto, start uint32, end uint32) ([]byte, error) {
	sz := obj.SizeVT()
	buff := make([]byte, sz+uint32Size*2) // proto buff size + start/end uint32s

	// jpe is this ok? - get rid of all gogo?
	slidingBuff := buff
	encodeUint32(start, slidingBuff)
	slidingBuff = slidingBuff[uint32Size:]
	encodeUint32(end, slidingBuff)
	slidingBuff = slidingBuff[uint32Size:]

	_, err := obj.MarshalToSizedBufferVT(slidingBuff)
	if err != nil {
		return nil, err
	}

	return buff, nil
}

func stripStartEnd(buff []byte) ([]byte, uint32, uint32, error) {
	if len(buff) < uint32Size*2 {
		return nil, 0, 0, errors.New("buffer too short to have start/end")
	}

	start, err := decodeUint32(buff)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to read start from buffer %w", err)
	}
	buff = buff[uint32Size:]

	end, err := decodeUint32(buff)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to read end from buffer %w", err)
	}
	buff = buff[uint32Size:]

	return buff, start, end, nil
}

// jpe use buffer functions? previously uint64. should i follow suit?
func encodeUint32(x uint32, buf []byte) {
	buf[0] = uint8(x)
	buf[1] = uint8(x >> 8)
	buf[2] = uint8(x >> 16)
	buf[3] = uint8(x >> 24)
}

func decodeUint32(buf []byte) (x uint32, err error) {
	if len(buf) < uint32Size {
		return 0, io.ErrUnexpectedEOF
	}

	x = uint32(buf[0])
	x |= uint32(buf[1]) << 8
	x |= uint32(buf[2]) << 16
	x |= uint32(buf[3]) << 24
	return
}
