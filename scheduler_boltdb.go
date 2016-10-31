package sheduler

import (
	"log"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
	"gopkg.in/vmihailenco/msgpack.v2"
)

func NewBoltDBSheduler(
	db *bolt.DB,
	pub Publisher,
	bucketName []byte,
	period time.Duration,
	logger *log.Logger,
) Sheduler {

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})

	return &BoltDBScheduler{
		db:         db,
		pub:        pub,
		bucketName: bucketName,
		period:     period,
		logger:     logger,
	}
}

type BoltDBScheduler struct {
	db         *bolt.DB
	pub        Publisher
	bucketName []byte
	period     time.Duration
	logger     *log.Logger

	launcher sync.Once
}

func (s *BoltDBScheduler) Shedule(
	taskID uuid.UUID,
	payload []byte,
	startDateTime time.Time,
	notifyTo ...string,
) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		data, err := msgpack.Marshal(SheduleData{
			TaskID:        taskID,
			Payload:       payload,
			StartDateTime: startDateTime,
			NotifyTo:      notifyTo,
		})

		if err != nil {
			s.logger.Println("[ERR] marshal task ID", taskID, err)
			return err
		}

		b := tx.Bucket(s.bucketName)

		return b.Put(taskID.Bytes(), data)
	})
	return err
}

func (s *BoltDBScheduler) Start() <-chan struct{} {
	abort := make(chan struct{}, 1)

	s.launcher.Do(func() {
		go func() {
			defer func() {
				abort <- struct{}{}
			}()

			s.logger.Println("[INFO] start processing, period =", s.period)

			for {
				select {
				case <-time.After(s.period):
					// s.logger.Println("[DEBUG] beep...")

					tasks := []SheduleData{}

					err := s.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket(s.bucketName)

						err := b.ForEach(func(id, v []byte) error {
							task := SheduleData{}

							if err := msgpack.Unmarshal(v, &task); err != nil {
								s.logger.Println("[ERR] unmarshal task ID", id, err)
								return err
							}

							if task.StartDateTime.Sub(time.Now()) <= 0 {
								tasks = append(tasks, task)
							}

							return nil
						})

						if err != nil {
							s.logger.Println("[ERR] task processing", err)
						}

						return err
					})

					if err != nil {
						s.logger.Println("[ERR] view tasks", err)
						continue
					}

					if len(tasks) == 0 {
						continue
					}

					s.logger.Println("[INFO] count tasks", len(tasks))

					err = s.db.Update(func(tx *bolt.Tx) error {
						b := tx.Bucket(s.bucketName)

						for _, task := range tasks {

							if err := s.pub.Publishing(task); err != nil {
								s.logger.Println("[ERR] publish task ID", task.TaskID, err)
								return err
							}

							if err := b.Delete(task.TaskID.Bytes()); err != nil {
								s.logger.Println("[ERR] remove task ID", task.TaskID, err)
							}
						}
						return nil
					})

					if err != nil {
						s.logger.Println("[ERR] update tasks", err)
						continue
					}

				}
			}
		}()
	})

	return abort
}

// DTO

type SheduleData struct {
	TaskID        uuid.UUID
	Payload       []byte
	StartDateTime time.Time
	NotifyTo      []string
}
