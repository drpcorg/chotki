package testutils

import (
	"log/slog"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
)

func SyncData(a, b host.Host) error {
	synca := replication.Syncer{
		Host:          a,
		Mode:          replication.SyncRW,
		Name:          "a",
		WaitUntilNone: time.Millisecond,
		Src:           a.Source(),
		Log:           utils.NewDefaultLogger(slog.LevelError),
		PingWait:      time.Second,
	}
	syncb := replication.Syncer{
		Host:          b,
		Mode:          replication.SyncRW,
		WaitUntilNone: time.Millisecond,
		Name:          "b",
		Src:           b.Source(),
		Log:           utils.NewDefaultLogger(slog.LevelError),
		PingWait:      time.Second,
	}
	defer syncb.Close()
	defer synca.Close()
	// send handshake from b to a
	err := protocol.Relay(&syncb, &synca)
	if err != nil {
		return err
	}
	go protocol.Pump(&syncb, &synca)
	// send data a -> b
	return protocol.Pump(&synca, &syncb)

}
