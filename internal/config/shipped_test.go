package config

import (
	"path/filepath"
	"regexp"
	"testing"
	"time"
)

// The configs shipped in the image must load, so a broken example fails CI
// instead of a user's first deploy.
func TestShippedConfigsLoad(t *testing.T) {
	t.Setenv("RAILWAY_PROJECT_TOKEN", "test")
	t.Setenv("RAILWAY_ENVIRONMENT_ID", "test")

	for _, path := range []string{"../../examples/signalbox.yaml", "../../deploy/railway/signalbox.yaml"} {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			c, err := Load(path)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if len(c.Rules) == 0 {
				t.Fatalf("%s: no rules", path)
			}
		})
	}
}

// Go's net/http recovers a handler panic and logs "http: panic serving ...",
// with no "panic:" in the line; the crash rule must still match it.
func TestCrashRuleMatchesRecoveredPanic(t *testing.T) {
	t.Setenv("RAILWAY_PROJECT_TOKEN", "test")
	t.Setenv("RAILWAY_ENVIRONMENT_ID", "test")

	c, err := Load("../../deploy/railway/signalbox.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var body string
	for _, r := range c.Rules {
		if r.Name == "crash" {
			body = r.Match.Body
		}
	}
	if body == "" {
		t.Fatal("no crash rule with a body pattern")
	}
	re := regexp.MustCompile(body)
	for _, line := range []string{
		"2026/11/22 16:00:01 http: panic serving 10.0.0.1:5000: runtime error: integer divide by zero",
		"panic: runtime error: index out of range [3] with length 3",
		"Traceback (most recent call last):",
	} {
		if !re.MatchString(line) {
			t.Errorf("crash rule does not match %q", line)
		}
	}
}

// The Railway config's thresholds come from variables with defaults, so a
// template user tunes them without forking.
func TestShippedRailwayThresholdVariables(t *testing.T) {
	t.Setenv("RAILWAY_PROJECT_TOKEN", "test")
	t.Setenv("RAILWAY_ENVIRONMENT_ID", "test")

	rule := func(c *Config, name string) Rule {
		t.Helper()
		for _, r := range c.Rules {
			if r.Name == name {
				return r
			}
		}
		t.Fatalf("no rule %q", name)
		return Rule{}
	}

	c, err := Load("../../deploy/railway/signalbox.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if r := rule(c, "http-5xx"); r.Threshold != 0.05 || r.MinRequests != 20 || r.Window != 5*time.Minute {
		t.Errorf("http-5xx defaults: threshold %v, min %d, window %s", r.Threshold, r.MinRequests, r.Window)
	}
	if r := rule(c, "http-slow"); r.Threshold != 2000 || r.Summary != "The slowest 1% of requests take more than 2000 ms." {
		t.Errorf("http-slow defaults: threshold %v, summary %q", r.Threshold, r.Summary)
	}
	if r := rule(c, "error-logs"); r.Threshold != 25 {
		t.Errorf("error-logs default threshold %v", r.Threshold)
	}

	t.Setenv("SIGNALBOX_WINDOW", "10m")
	t.Setenv("SIGNALBOX_5XX_RATIO", "0.2")
	t.Setenv("SIGNALBOX_MIN_REQUESTS", "50")
	t.Setenv("SIGNALBOX_P99_MS", "800")
	t.Setenv("SIGNALBOX_ERROR_LINES", "100")
	c, err = Load("../../deploy/railway/signalbox.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if r := rule(c, "http-5xx"); r.Threshold != 0.2 || r.MinRequests != 50 || r.Window != 10*time.Minute {
		t.Errorf("http-5xx overridden: threshold %v, min %d, window %s", r.Threshold, r.MinRequests, r.Window)
	}
	if r := rule(c, "http-slow"); r.Threshold != 800 || r.Summary != "The slowest 1% of requests take more than 800 ms." {
		t.Errorf("http-slow overridden: threshold %v, summary %q", r.Threshold, r.Summary)
	}
	if r := rule(c, "error-logs"); r.Threshold != 100 || r.Window != 10*time.Minute {
		t.Errorf("error-logs overridden: threshold %v, window %s", r.Threshold, r.Window)
	}
	if r := rule(c, "crash"); r.Window != 10*time.Minute {
		t.Errorf("crash window %s", r.Window)
	}

	// A bad value fails startup with a message, not a silent default.
	t.Setenv("SIGNALBOX_WINDOW", "five minutes")
	if _, err := Load("../../deploy/railway/signalbox.yaml"); err == nil {
		t.Error("an invalid SIGNALBOX_WINDOW loaded without error")
	}
}

func TestEnvDefaults(t *testing.T) {
	t.Setenv("SB_SET", "set")
	t.Setenv("SB_EMPTY", "")
	for in, want := range map[string]string{
		"${SB_SET:-x}":              "set",
		"${SB_EMPTY:-x}":            "x",
		"${SB_UNSET:-x}":            "x",
		"${SB_UNSET:-}":             "",
		"${SB_UNSET}":               "",
		"${SB_SET}":                 "set",
		"${SB_UNSET:-5m}s":          "5ms",
		"a${SB_SET}b${SB_UNSET:-c}": "asetbc",
	} {
		got := envRef.ReplaceAllStringFunc(in, expandRef)
		if got != want {
			t.Errorf("%s: got %q, want %q", in, got, want)
		}
	}
}

// SIGNALBOX_5XX_RATIO=5 (meaning 5%) must fail loudly, not load a rule that never fires.
func TestShippedRailwayRatioAsPercentFails(t *testing.T) {
	t.Setenv("RAILWAY_PROJECT_TOKEN", "test")
	t.Setenv("RAILWAY_ENVIRONMENT_ID", "test")
	t.Setenv("SIGNALBOX_5XX_RATIO", "5")
	if _, err := Load("../../deploy/railway/signalbox.yaml"); err == nil {
		t.Error("SIGNALBOX_5XX_RATIO=5 loaded without error")
	}
}
