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
}
