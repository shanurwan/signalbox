package notify

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

var errNotPublic = errors.New("receivers created through the API may only send to public addresses")

// blocked are ranges a tenant's webhook must never reach: anything inside
// the network signalbox runs in, cloud metadata services included.
var blocked = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT, also used for internal networks
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("64:ff9b::/96"), // NAT64 can map to any IPv4 address
	netip.MustParsePrefix("fec0::/10"),
}

func public(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, p := range blocked {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// publicOnlyClient checks the address after DNS resolution, at connect
// time, so a name that resolves to a private address (or changes to one
// between checks) is refused. It ignores proxy settings, which would hide
// the real destination, and does not follow redirects.
func publicOnlyClient(allowPrivate bool) *http.Client {
	d := &net.Dialer{Timeout: 5 * time.Second}
	if !allowPrivate {
		d.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return fmt.Errorf("%w: %s", errNotPublic, address)
			}
			if !public(ap.Addr()) {
				return fmt.Errorf("%w: %s", errNotPublic, ap.Addr())
			}
			return nil
		}
	}
	return &http.Client{
		Timeout:       10 * time.Second,
		Transport:     &http.Transport{Proxy: nil, DialContext: d.DialContext, TLSHandshakeTimeout: 5 * time.Second, MaxIdleConnsPerHost: 2},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
