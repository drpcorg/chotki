package chotki

import (
	"testing"
	"time"

	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
)

func TestIndexManager_ParseIndexTasks(t *testing.T) {
	time := time.Now()
	task := &reindexTask{
		State:      reindexTaskStateDone,
		Field:      21,
		LastUpdate: time,
		Cid:        rdx.ID0,
		Revision:   10,
		Src:        100,
	}

	task2, err := parseReindexTasks(task.Key(), task.Value())
	assert.NoError(t, err)
	assert.Equal(t, task.State, task2[0].State)
	assert.Equal(t, task.Field, task2[0].Field)
	assert.Equal(t, task.Cid, task2[0].Cid)
	assert.Equal(t, task.Revision, task2[0].Revision)
	assert.Equal(t, task.Src, task2[0].Src)
}
