package rdx_test

import (
	"fmt"
	"testing"

	"github.com/drpcorg/chotki/rdx"
)

func BenchmarkRdxIdTotring(b *testing.B) {
	test := ""
	id := rdx.ID0
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		id = (id & ^rdx.OffMask) + rdx.ProInc
		b.StartTimer()
		test = id.String()
	}
	fmt.Println(test)
}
