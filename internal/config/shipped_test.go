package config

import (
	"path/filepath"
	"regexp"
	"testing"
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
