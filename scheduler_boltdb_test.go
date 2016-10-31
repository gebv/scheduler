package sheduler

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type TestPublisher struct {
	ch chan SheduleData
}

func (p *TestPublisher) Publishing(data SheduleData) error {
	go func() {
		// TODO: catch the close channel
		p.ch <- data
	}()

	return nil
}

func TestScheduleBoltdbRabbitmq(t *testing.T) {
	os.RemoveAll("./_testdb.db") // delete the old file

	db, err := bolt.Open("./_testdb.db", 0600, &bolt.Options{
		Timeout: 1 * time.Second,
	})
	defer func() {
		os.RemoveAll("./_testdb.db")
	}()
	assert.NoError(t, err)
	ch := make(chan SheduleData)

	s := NewBoltDBSheduler(
		db,
		&TestPublisher{ch},
		[]byte("name"),
		time.Millisecond*100,
		log.New(os.Stdout, "[sheduler]", -1),
	)
	counter := 0
	go func() {
		for {
			select {
			case <-ch:
				counter++
			}
		}
	}()
	s.Start()

	go func() {

		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * 3)
			s.Shedule(uuid.NewV4(), []byte("data"), time.Now().Add(-time.Millisecond*100*time.Duration(i)))
		}
	}()

	time.Sleep(time.Second)
	assert.Equal(t, counter, 100)
}
