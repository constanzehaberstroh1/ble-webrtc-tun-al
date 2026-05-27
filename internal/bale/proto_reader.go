package bale

// proto_reader.go — Proper protobuf field reader.
// Replaces fragile byte-scanning (e.g. searching for 0x08/0x10 bytes)
// with correct tag+varint parsing that survives field reordering.

import (
	"encoding/binary"
)

// ProtoField represents a parsed protobuf field.
type ProtoField struct {
	FieldNum uint64
	WireType uint64
	Varint   uint64   // wire type 0
	Bytes    []byte   // wire type 2
	Fixed64  uint64   // wire type 1
	Fixed32  uint32   // wire type 5
}

// ParseProtoFields walks a protobuf message and extracts all fields.
// Unlike raw byte-scanning, this correctly handles multi-byte tags,
// variable-length varints, and nested messages.
func ParseProtoFields(data []byte) []ProtoField {
	var fields []ProtoField
	offset := 0

	for offset < len(data) {
		// Read tag (field_number << 3 | wire_type)
		tag, n := decodeVarintSafe(data, offset)
		if n <= 0 {
			break
		}
		offset += n

		wireType := tag & 0x07
		fieldNum := tag >> 3

		f := ProtoField{
			FieldNum: fieldNum,
			WireType: wireType,
		}

		switch wireType {
		case 0: // Varint
			val, n := decodeVarintSafe(data, offset)
			if n <= 0 {
				return fields
			}
			f.Varint = val
			offset += n

		case 1: // Fixed 64-bit
			if offset+8 > len(data) {
				return fields
			}
			f.Fixed64 = binary.LittleEndian.Uint64(data[offset : offset+8])
			offset += 8

		case 2: // Length-delimited
			length, n := decodeVarintSafe(data, offset)
			if n <= 0 || offset+n+int(length) > len(data) {
				return fields
			}
			offset += n
			f.Bytes = data[offset : offset+int(length)]
			offset += int(length)

		case 5: // Fixed 32-bit
			if offset+4 > len(data) {
				return fields
			}
			f.Fixed32 = binary.LittleEndian.Uint32(data[offset : offset+4])
			offset += 4

		default:
			// Unknown wire type — can't continue safely
			return fields
		}

		fields = append(fields, f)
	}

	return fields
}

// FindVarintField finds the first varint field with the given field number.
// Returns (value, true) if found, (0, false) if not.
func FindVarintField(fields []ProtoField, fieldNum uint64) (uint64, bool) {
	for _, f := range fields {
		if f.FieldNum == fieldNum && f.WireType == 0 {
			return f.Varint, true
		}
	}
	return 0, false
}

// FindBytesField finds the first length-delimited field with the given number.
func FindBytesField(fields []ProtoField, fieldNum uint64) ([]byte, bool) {
	for _, f := range fields {
		if f.FieldNum == fieldNum && f.WireType == 2 {
			return f.Bytes, true
		}
	}
	return nil, false
}

// FindAllVarintFields returns all varint values for a given field number.
func FindAllVarintFields(fields []ProtoField, fieldNum uint64) []uint64 {
	var vals []uint64
	for _, f := range fields {
		if f.FieldNum == fieldNum && f.WireType == 0 {
			vals = append(vals, f.Varint)
		}
	}
	return vals
}

// FindAllBytesFields returns all length-delimited values for a given field number.
func FindAllBytesFields(fields []ProtoField, fieldNum uint64) [][]byte {
	var vals [][]byte
	for _, f := range fields {
		if f.FieldNum == fieldNum && f.WireType == 2 {
			vals = append(vals, f.Bytes)
		}
	}
	return vals
}

// RecursiveFindVarint searches for a varint field recursively through
// all nested length-delimited fields. Returns the first match.
func RecursiveFindVarint(data []byte, fieldNum uint64, minValue uint64) (uint64, bool) {
	fields := ParseProtoFields(data)
	// Check top-level first
	if val, ok := FindVarintField(fields, fieldNum); ok && val >= minValue {
		return val, true
	}
	// Recurse into nested messages
	for _, f := range fields {
		if f.WireType == 2 && len(f.Bytes) > 2 {
			if val, ok := RecursiveFindVarint(f.Bytes, fieldNum, minValue); ok {
				return val, true
			}
		}
	}
	return 0, false
}

// RecursiveFindString searches for a string field recursively.
// If fieldNum is 0, matches any field number (wildcard).
func RecursiveFindString(data []byte, fieldNum uint64, predicate func(string) bool) (string, bool) {
	fields := ParseProtoFields(data)
	for _, f := range fields {
		if f.WireType == 2 && (fieldNum == 0 || f.FieldNum == fieldNum) {
			if isPrintableString(f.Bytes) {
				s := string(f.Bytes)
				if predicate(s) {
					return s, true
				}
			}
		}
	}
	// Recurse into nested messages
	for _, f := range fields {
		if f.WireType == 2 && len(f.Bytes) > 2 {
			if s, ok := RecursiveFindString(f.Bytes, fieldNum, predicate); ok {
				return s, true
			}
		}
	}
	return "", false
}

// decodeVarintSafe reads a varint from data[offset:]. Returns (value, bytesRead).
// Returns (0, 0) on error (unlike binary.Uvarint which can return negative n).
func decodeVarintSafe(data []byte, offset int) (uint64, int) {
	if offset >= len(data) {
		return 0, 0
	}
	var result uint64
	var shift uint
	start := offset
	for offset < len(data) {
		b := data[offset]
		result |= uint64(b&0x7F) << shift
		offset++
		if b&0x80 == 0 {
			return result, offset - start
		}
		shift += 7
		if shift >= 64 {
			return 0, 0 // overflow
		}
	}
	return 0, 0 // truncated
}
