package main

import (
	"encoding/binary"
	"strings"
	"testing"
)

// TestBuildNetflowV5Packet checks that a built datagram conforms to the
// NetFlow v5 spec (RFC 3954-ish layout) so the subject's listener actually
// recognizes our synthetic data instead of dropping it as garbage.
//
// Validates the header and one record. Catches the "off-by-one offset"
// and "wrong endianness" classes of bug — the fields most likely to
// silently drift if someone reorders the writes in buildNetflowV5Packet.
func TestBuildNetflowV5Packet(t *testing.T) {
	const recordBase = 12345
	const uptimeMs = 999_000
	const packetsSent = 7

	buf := make([]byte, netflowV5PacketSize)
	buildNetflowV5Packet(buf, uptimeMs, recordBase, packetsSent)

	// --- Header ---
	if got := binary.BigEndian.Uint16(buf[0:]); got != 5 {
		t.Errorf("Version: got %d, want 5", got)
	}
	if got := binary.BigEndian.Uint16(buf[2:]); got != netflowV5RecordsPer {
		t.Errorf("Count: got %d, want %d", got, netflowV5RecordsPer)
	}
	if got := binary.BigEndian.Uint32(buf[4:]); got != uptimeMs {
		t.Errorf("SysUptime: got %d, want %d", got, uptimeMs)
	}
	wantSeq := uint32(packetsSent) * netflowV5RecordsPer
	if got := binary.BigEndian.Uint32(buf[16:]); got != wantSeq {
		t.Errorf("FlowSequence: got %d, want %d", got, wantSeq)
	}

	// --- First record ---
	rec := buf[netflowV5HeaderSize : netflowV5HeaderSize+netflowV5RecordSize]

	// Source IP must be 10.99.<hi>.<lo> with the counter encoded into the
	// last two octets. This is the marker the receiver scans for; if it
	// drifts here, the correctness test would silently pass.
	if rec[0] != 10 || rec[1] != 99 {
		t.Errorf("SrcAddr prefix: got %d.%d, want 10.99", rec[0], rec[1])
	}
	wantHi := byte((recordBase >> 8) & 0xff)
	wantLo := byte(recordBase & 0xff)
	if rec[2] != wantHi || rec[3] != wantLo {
		t.Errorf("SrcAddr counter octets: got %d.%d, want %d.%d", rec[2], rec[3], wantHi, wantLo)
	}

	// Dst IP prefix
	if rec[4] != 192 || rec[5] != 168 {
		t.Errorf("DstAddr prefix: got %d.%d, want 192.168", rec[4], rec[5])
	}

	// Proto must be TCP — anything else and the downstream pipeline
	// labels won't match what we documented.
	if rec[38] != 6 {
		t.Errorf("Proto: got %d, want 6 (TCP)", rec[38])
	}
	if got := binary.BigEndian.Uint16(rec[34:]); got != 443 {
		t.Errorf("DstPort: got %d, want 443", got)
	}

	// Each record's port is offset by its index — record 0 uses
	// 1024+(recordBase%60000). Confirm the per-record uniqueness
	// stamping actually wrote different bytes for record 1 vs record 0.
	rec1 := buf[netflowV5HeaderSize+netflowV5RecordSize : netflowV5HeaderSize+2*netflowV5RecordSize]
	if got0, got1 := binary.BigEndian.Uint16(rec[32:]), binary.BigEndian.Uint16(rec1[32:]); got0 == got1 {
		t.Errorf("SrcPort identical across records 0 and 1 (got %d) — uniqueness check broken", got0)
	}
}

// TestPacketSizeMatchesSpec catches the layout drifting away from the
// v5-spec byte counts. If anyone reorganizes the struct of header/record
// helpers and forgets to update the constants, the wire format silently
// breaks; this test pins both numbers.
func TestPacketSizeMatchesSpec(t *testing.T) {
	if netflowV5HeaderSize != 24 {
		t.Errorf("header size: got %d, want 24 (NetFlow v5 spec)", netflowV5HeaderSize)
	}
	if netflowV5RecordSize != 48 {
		t.Errorf("record size: got %d, want 48 (NetFlow v5 spec)", netflowV5RecordSize)
	}
	want := netflowV5HeaderSize + netflowV5RecordsPer*netflowV5RecordSize
	if netflowV5PacketSize != want {
		t.Errorf("packet size: got %d, want %d", netflowV5PacketSize, want)
	}
}

// TestRequiredSubstringPresent is a tripwire: if anyone changes the
// 10.99 prefix in writeNetflowV5Record without updating any consumer
// that asserts the substring, the test would silently start failing
// in CI. Belt-and-suspenders by repeating the expected literal here
// so the diff is loud.
func TestRequiredSubstringPresent(t *testing.T) {
	buf := make([]byte, netflowV5PacketSize)
	buildNetflowV5Packet(buf, 0, 0, 0)
	rec := buf[netflowV5HeaderSize : netflowV5HeaderSize+netflowV5RecordSize]
	// IPv4 octets land at rec[0..3]. Render to a string and look for the
	// exact prefix the case YAML expects.
	addr := dottedV4(rec[0:4])
	if !strings.HasPrefix(addr, "10.99.") {
		t.Errorf("SrcAddr=%q does not start with the required substring %q — case yaml would fail", addr, "10.99.")
	}
}

func dottedV4(b []byte) string {
	if len(b) != 4 {
		return ""
	}
	var sb strings.Builder
	for i, x := range b {
		if i > 0 {
			sb.WriteByte('.')
		}
		// Tiny inline itoa to avoid pulling strconv just for tests.
		switch {
		case x >= 100:
			sb.WriteByte('0' + x/100)
			sb.WriteByte('0' + (x/10)%10)
			sb.WriteByte('0' + x%10)
		case x >= 10:
			sb.WriteByte('0' + x/10)
			sb.WriteByte('0' + x%10)
		default:
			sb.WriteByte('0' + x)
		}
	}
	return sb.String()
}
