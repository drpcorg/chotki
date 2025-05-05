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
		id = rdx.NewID(id.Src(), id.Seq(), id.Off()+1)
		b.StartTimer()
		test = id.String()
	}
	fmt.Println(test)
}
