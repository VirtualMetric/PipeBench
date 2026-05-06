package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// NetFlow v5 over UDP: synthesizes valid v5 datagrams and ships them at the
// configured rate. Each datagram carries 5 records and every record is
// uniquely identifiable so a downstream pipeline-correctness check can
// verify (a) decode happened and (b) no records were dropped.
//
// Why v5 (not v9 or IPFIX): v5 is fixed-format with no template state, so
// the receiver-side library doesn't need warm-up packets and the test stays
// deterministic from message #1.
//
// Identification scheme — every record's Src IP is 10.99.<hi>.<lo> where
// (hi, lo) encode a global counter. The 10.99. prefix is the marker the
// receiver looks for to confirm the listener actually decoded fields out
// of the binary; the unique counter feeds the receiver's dedup check so a
// listener that emits "5 copies of the same record" instead of "5 distinct
// records" fails loudly.
//
// Counter semantics — one generator "line" maps to one FLOW RECORD, not
// one UDP datagram. We send 5 records per datagram, so a TotalLines=500
// case sends 100 datagrams. Reporting record-count keeps the harness's
// lines-sent vs lines-received comparison meaningful: the receiver sees
// one TCP line per decoded record, so a clean run has lines_sent ==
// lines_received and the runner's over-delivery check stays usable.
//
// Rate and Duration still operate at the record granularity for the
// same reason — "5000 flows/sec" is what NetFlow operators reason about,
// not "1000 datagrams/sec".

const (
	netflowV5HeaderSize = 24
	netflowV5RecordSize = 48
	netflowV5RecordsPer = 5
	netflowV5PacketSize = netflowV5HeaderSize + netflowV5RecordsPer*netflowV5RecordSize
)

// runNetflowV5 sends synthetic NetFlow v5 datagrams. With
// cfg.Connections > 1, fans out N independent senders — each
// dialing its own UDP socket so the source 4-tuple is unique per
// worker. This matters under SO_REUSEPORT on the receiver side:
// the kernel's load-balancer hashes by 4-tuple, so a single-source
// generator pins every datagram to one receiver worker even when
// the listener is fanned out across N cores. With multiple
// senders the receiver actually sees parallelism.
//
// Returns (records_sent, bytes_sent, err). Records — not datagrams
// — is the reported unit; see the package-level comment above for
// why.
func runNetflowV5(cfg config, clock *sendClock) (int64, int64, error) {
	if cfg.Connections <= 1 {
		return runNetflowV5Single(cfg, clock)
	}
	return runNetflowV5Parallel(cfg, clock)
}

// runNetflowV5Parallel fans out cfg.Connections independent NetFlow
// senders. Each goroutine dials its own UDP socket, runs the same
// per-record/duration budget the single-sender path uses, and
// reports records sent. The dispatcher does NOT partition rate or
// duration across workers — under SO_REUSEPORT the receiver scales
// with parallel senders, so the right shape is "N senders each
// pushing as fast as they can" (matching the OTLP generator's
// parallel mode).
func runNetflowV5Parallel(cfg config, clock *sendClock) (int64, int64, error) {
	var totalRecords, totalBytes atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for i := 0; i < cfg.Connections; i++ {
		wg.Add(1)
		workerCfg := cfg
		workerCfg.Connections = 1 // each worker drives a single socket
		go func(id int) {
			defer wg.Done()
			records, bytes, err := runNetflowV5Single(workerCfg, clock)
			totalRecords.Add(records)
			totalBytes.Add(bytes)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
			}
			fmt.Fprintf(os.Stderr, "generator: netflow worker %d done: records=%d bytes=%d\n",
				id, records, bytes)
		}(i)
	}

	wg.Wait()
	return totalRecords.Load(), totalBytes.Load(), firstErr
}

// runNetflowV5Single is the original single-sender path. Used
// directly when Connections <= 1, and as the per-worker body in
// the parallel dispatcher above.
func runNetflowV5Single(cfg config, clock *sendClock) (int64, int64, error) {
	addr, err := net.ResolveUDPAddr("udp", cfg.Target)
	if err != nil {
		return 0, 0, fmt.Errorf("resolve udp %s: %w", cfg.Target, err)
	}

	// Retry the dial briefly so the generator can come up before the
	// subject's UDP listener is bound. UDP is connectionless so the dial
	// itself doesn't fail when the peer isn't listening — but we still
	// want to be tolerant of slow subject startup, mirroring dialTCP's
	// behavior.
	var conn *net.UDPConn
	timeout := time.Duration(getEnvInt("GENERATOR_CONNECT_TIMEOUT", 120)) * time.Second
	deadline := time.Now().Add(timeout)
	for {
		conn, err = net.DialUDP("udp", nil, addr)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return 0, 0, fmt.Errorf("udp dial %s after %s: %w", cfg.Target, timeout, err)
		}
		fmt.Fprintf(os.Stderr, "generator: udp dial %s: %v (retrying…)\n", cfg.Target, err)
		time.Sleep(2 * time.Second)
	}
	defer conn.Close()

	// Bigger send buffer — the kernel's default (208KB on Linux) drops
	// packets under burst load, which would skew correctness counts.
	// 4MB is plenty for our packet sizes; SetWriteBuffer is best-effort
	// and silently caps to net.core.wmem_max.
	_ = conn.SetWriteBuffer(4 << 20)

	var datagramsSent, recordsSent, bytesSent int64
	var recordCounter uint32 // global record counter, drives per-record uniqueness

	startUptimeMs := uint32(time.Now().UnixMilli() & 0xFFFFFFFF)
	bootTime := time.Now()

	var deadlineSend time.Time
	if cfg.Duration > 0 {
		deadlineSend = time.Now().Add(cfg.Duration)
	}

	// Rate is interpreted at record-granularity (see package comment).
	// Convert to a datagram-tick by dividing by records-per-datagram so
	// the resulting per-record output rate matches what the user asked
	// for. cfg.Rate=0 means "no limit" same as the other modes.
	var rateLimiter <-chan time.Time
	if cfg.Rate > 0 {
		recordsPerSec := cfg.Rate
		datagramsPerSec := recordsPerSec / netflowV5RecordsPer
		if datagramsPerSec < 1 {
			datagramsPerSec = 1
		}
		ticker := time.NewTicker(time.Second / time.Duration(datagramsPerSec))
		defer ticker.Stop()
		rateLimiter = ticker.C
	}

	// Reused per-packet buffer — same capacity as one v5 datagram, no
	// realloc during the send loop. Big endian throughout because that's
	// what the spec requires.
	buf := make([]byte, netflowV5PacketSize)

	const clockSampleInterval = 10000
	for {
		// TotalLines is checked at record granularity. We emit a whole
		// datagram on each iteration; if the next datagram would push us
		// past the requested record total, we stop here. (Sending a
		// partial datagram isn't legal — the v5 header's Count field
		// must match the actual record count, and most receivers reject
		// truncated datagrams.)
		if cfg.TotalLines > 0 && recordsSent+netflowV5RecordsPer > cfg.TotalLines {
			break
		}
		if !deadlineSend.IsZero() && time.Now().After(deadlineSend) {
			break
		}
		if rateLimiter != nil {
			<-rateLimiter
		}

		uptimeMs := startUptimeMs + uint32(time.Since(bootTime).Milliseconds())
		buildNetflowV5Packet(buf, uptimeMs, atomic.LoadUint32(&recordCounter), datagramsSent)

		n, werr := conn.Write(buf)
		if werr != nil {
			return recordsSent, bytesSent, fmt.Errorf("udp write: %w", werr)
		}
		datagramsSent++
		recordsSent += netflowV5RecordsPer
		bytesSent += int64(n)
		atomic.AddUint32(&recordCounter, netflowV5RecordsPer)

		if datagramsSent == 1 || datagramsSent%clockSampleInterval == 0 {
			clock.RecordSend()
		}
	}

	if datagramsSent > 0 {
		clock.RecordSend()
	}
	return recordsSent, bytesSent, nil
}

// buildNetflowV5Packet writes a v5 datagram into buf in place. buf must be
// at least netflowV5PacketSize bytes; we always write exactly that many.
//
// recordBase is the global counter for the FIRST record in this datagram;
// the records that follow each get base, base+1, … base+4. flowSequence
// is the cumulative count of all flow records seen so far (NetFlow v5
// header semantics) — equals packetsSent * recordsPerPacket.
func buildNetflowV5Packet(buf []byte, uptimeMs uint32, recordBase uint32, packetsSent int64) {
	// --- Header (24 bytes) ---
	binary.BigEndian.PutUint16(buf[0:], 5)                    // Version
	binary.BigEndian.PutUint16(buf[2:], netflowV5RecordsPer)  // Count
	binary.BigEndian.PutUint32(buf[4:], uptimeMs)             // SysUptime (ms)
	now := time.Now()
	binary.BigEndian.PutUint32(buf[8:], uint32(now.Unix()))                       // UnixSecs
	binary.BigEndian.PutUint32(buf[12:], uint32(now.Nanosecond()))                // UnixNsecs
	binary.BigEndian.PutUint32(buf[16:], uint32(packetsSent)*netflowV5RecordsPer) // FlowSequence
	buf[20] = 0                                  // EngineType
	buf[21] = 0                                  // EngineID
	binary.BigEndian.PutUint16(buf[22:], 0)      // SamplingInterval

	// --- Records ---
	for i := 0; i < netflowV5RecordsPer; i++ {
		off := netflowV5HeaderSize + i*netflowV5RecordSize
		writeNetflowV5Record(buf[off:off+netflowV5RecordSize], recordBase+uint32(i), uptimeMs)
	}
}

// writeNetflowV5Record fills one 48-byte v5 record. n is the unique global
// record counter — encoded into the SrcAddr's last two octets and into
// SrcPort/DstPort so a downstream uniqueness check (dedup) sees one
// distinct emitted line per generated record.
func writeNetflowV5Record(rec []byte, n uint32, uptimeMs uint32) {
	// SrcAddr: 10.99.<hi>.<lo>. The 10.99 prefix is what the receiver
	// scans for to confirm the listener actually decoded the binary —
	// it's not a value any of the other generator modes (raw/syslog/json)
	// could accidentally produce.
	hi := byte((n >> 8) & 0xff)
	lo := byte(n & 0xff)
	rec[0], rec[1], rec[2], rec[3] = 10, 99, hi, lo

	// DstAddr: 192.168.<hi>.<lo>. Different prefix from src so checks can
	// distinguish source-side from destination-side decoding bugs.
	rec[4], rec[5], rec[6], rec[7] = 192, 168, hi, lo

	// NextHop = 0.0.0.0
	rec[8], rec[9], rec[10], rec[11] = 0, 0, 0, 0

	binary.BigEndian.PutUint16(rec[12:], 1)             // InputSnmp (ifIndex)
	binary.BigEndian.PutUint16(rec[14:], 2)             // OutputSnmp
	binary.BigEndian.PutUint32(rec[16:], 64+n%1000)     // Packets
	binary.BigEndian.PutUint32(rec[20:], 1024+(n%4000)) // Octets
	binary.BigEndian.PutUint32(rec[24:], uptimeMs-1000) // First (1s ago)
	binary.BigEndian.PutUint32(rec[28:], uptimeMs)      // Last (now)

	// SrcPort cycles through the ephemeral range so consecutive records
	// don't share ports; DstPort sits on a fixed service so the
	// downstream pipeline sees a TCP/443-style flow.
	binary.BigEndian.PutUint16(rec[32:], uint16(1024+(n%60000))) // SrcPort
	binary.BigEndian.PutUint16(rec[34:], 443)                    // DstPort

	rec[36] = 0    // Pad1
	rec[37] = 0x18 // TCPFlags = ACK|PSH (a real-looking flow)
	rec[38] = 6    // Proto = TCP
	rec[39] = 0    // Tos

	binary.BigEndian.PutUint16(rec[40:], 65001)        // SrcAS
	binary.BigEndian.PutUint16(rec[42:], 15169)        // DstAS (Google, recognizable)
	rec[44] = 24                                       // SrcMask
	rec[45] = 24                                       // DstMask
	binary.BigEndian.PutUint16(rec[46:], 0)            // Pad2
}
