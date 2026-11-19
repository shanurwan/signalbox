package notify

import (
	"net/netip"
	"testing"
)

func TestPublicAddresses(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8":         true,
		"2606:4700::1111": true,
		"127.0.0.1":       false,
		"10.1.2.3":        false,
		"172.16.0.1":      false,
		"192.168.1.1":     false,
		"169.254.169.254": false, // cloud metadata
		"100.100.1.1":     false, // carrier-grade NAT range, used by private networks
		"0.0.0.0":         false,
		"::1":             false,
		"fd00::1":         false,
		"fe80::1":         false,
		"::ffff:10.0.0.1": false, // IPv4-mapped private address
		"64:ff9b::a00:1":  false, // NAT64 to 10.0.0.1
	} {
		if got := public(netip.MustParseAddr(addr)); got != want {
			t.Errorf("public(%s) = %v, want %v", addr, got, want)
		}
	}
}
