package chotki

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCounterSyncPeriodDefault(t *testing.T) {
	o := Options{}
	o.SetDefaults()
	assert.Equal(t, time.Second, o.CounterSyncPeriod)

	o2 := Options{CounterSyncPeriod: 5 * time.Minute}
	o2.SetDefaults()
	assert.Equal(t, 5*time.Minute, o2.CounterSyncPeriod, "explicit value must be preserved")

	// A negative period must be normalized, not left to silently disable the
	// background flush.
	o3 := Options{CounterSyncPeriod: -1}
	o3.SetDefaults()
	assert.Greater(t, o3.CounterSyncPeriod, time.Duration(0),
		"a non-positive CounterSyncPeriod must be normalized to the default")
}
