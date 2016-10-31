package sheduler

import (
	"errors"
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
		time.Millisecond*10,
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

	for i := 0; i < 10; i++ {
		go func() {

			for i := 0; i < 50; i++ {
				time.Sleep(time.Millisecond * time.Duration(i/10))
				err := s.Shedule(uuid.NewV4(), []byte("data"), time.Now().Add(-time.Millisecond*10*time.Duration(i)))
				assert.NoError(t, err, "shedule")
			}
		}()
	}

	time.Sleep(time.Second)

	countTasks := 0
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("name"))
		bucket.ForEach(func(k, v []byte) error {
			countTasks++
			return nil
		})
		return nil
	})

	assert.Equal(t, counter, 500)
	assert.Equal(t, countTasks, 0)
}

type TestFailPublisher struct {
	TestPublisher
}

func (p *TestFailPublisher) Publishing(data SheduleData) error {
	return errors.New("fail")
}

func TestSheduler_failpublish(t *testing.T) {
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
		&TestFailPublisher{TestPublisher{ch}},
		[]byte("name"),
		time.Millisecond*10,
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

	abort := s.Start()

	go func() {
		<-abort
		t.Log("abort worker")
	}()

	for i := 0; i < 10; i++ {
		go func() {

			for i := 0; i < 50; i++ {
				time.Sleep(time.Millisecond * time.Duration(i/10))
				err := s.Shedule(uuid.NewV4(), []byte("data"), time.Now().Add(-time.Millisecond*10*time.Duration(i)))
				assert.NoError(t, err, "shedule")
			}
		}()
	}

	time.Sleep(time.Second)

	// check

	countTasks := 0
	db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("name"))
		bucket.ForEach(func(k, v []byte) error {
			countTasks++
			return nil
		})
		return nil
	})

	assert.Equal(t, counter, 0)
	assert.Equal(t, countTasks, 500)
}
