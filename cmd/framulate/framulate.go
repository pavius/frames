package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/v3io/frames"
	"github.com/v3io/frames/http"
	"github.com/v3io/frames/pb"
	"github.com/v3io/frames/repeatingtask"

	"github.com/nuclio/errors"
	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
)

type framulate struct {
	ctx          context.Context
	logger       logger.Logger
	taskPool     *repeatingtask.Pool
	framesURL    string
	accessKey    string
	framesClient frames.Client
}

func newFramulate(ctx context.Context,
	framesURL string,
	containerName string,
	userName string,
	accessKey string,
	maxInflightRequests int) (*framulate, error) {
	var err error

	newFramulate := framulate{
		framesURL: framesURL,
	}

	newFramulate.taskPool, err = repeatingtask.NewPool(ctx,
		128,
		maxInflightRequests)

	if err != nil {
		return nil, errors.Wrap(err, "Failed to create pool")
	}

	newFramulate.logger, err = nucliozap.NewNuclioZapCmd("framulate", nucliozap.DebugLevel)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create logger")
	}

	session := pb.Session{
		Container: containerName,
		User:      userName,
		Token:     accessKey,
	}

	newFramulate.logger.DebugWith("Creating frames client",
		"container", session.Container,
		"user", session.User)

	newFramulate.framesClient, err = http.NewClient(newFramulate.framesURL, &session, newFramulate.logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create client")
	}

	return &newFramulate, nil
}

func (f *framulate) start(numTables int, numSeriesPerTable int) error {
	if err := f.createTSDBTables(numTables); err != nil {
		return errors.Wrap(err, "Failed to create TSDB tables")
	}

	if err := f.createTSDBSeries(numTables, numSeriesPerTable); err != nil {
		return errors.Wrap(err, "Failed to create TSDB series")
	}

	return nil
}

func (f *framulate) createTSDBTables(numTables int) error {
	f.logger.DebugWith("Creating tables")

	rateValue := pb.Value{}
	rateValue.SetValue("1/s")

	f.logger.DebugWith("Creating tables", "numTables", numTables)

	tableCreationTask := repeatingtask.Task{
		NumReptitions: numTables,
		MaxParallel:   256,
		Handler: func(cookie interface{}, repetitionIndex int) error {
			tableName := f.getTableName(repetitionIndex)

			f.logger.DebugWith("Creating table", "tableName", tableName)

			// try to delete first and ignore error
			f.framesClient.Delete(&pb.DeleteRequest{
				Backend: "tsdb",
				Table:   tableName,
			})

			return f.framesClient.Create(&pb.CreateRequest{
				Backend: "tsdb",
				Table:   tableName,
				AttributeMap: map[string]*pb.Value{
					"rate": &rateValue,
				},
			})
		},
	}

	taskErrors := f.taskPool.SubmitTaskAndWait(&tableCreationTask)
	return taskErrors.Error()
}

func (f *framulate) createTSDBSeries(numTables int, numSeriesPerTable int) error {
	seriesCreationTaskGroup := repeatingtask.TaskGroup{}

	// create a task per table and wait on these
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {

		// create a series creation task
		seriesCreationTask := repeatingtask.Task{
			NumReptitions: numSeriesPerTable,
			MaxParallel:   1024,
			Cookie:        f.getTableName(tableIdx),
			Handler: func(cookie interface{}, repetitionIndex int) error {
				tableName := cookie.(string)
				seriesName := fmt.Sprintf("series-%d", repetitionIndex)

				f.logger.DebugWith("Creating series",
					"tableName", tableName,
					"seriesName", seriesName)

				framesAppender, err := f.framesClient.Write(&frames.WriteRequest{
					Backend: "tsdb",
					Table:   tableName,
				})

				if err != nil {
					return errors.Wrap(err, "Failed to create err")
				}

				columns := map[string]interface{}{
					seriesName: []int{1},
				}

				indices := map[string]interface{}{
					"timestamp": []time.Time{time.Now()},
				}

				frame, err := frames.NewFrameFromMap(columns, indices)
				if err != nil {
					return errors.Wrap(err, "Failed to create frame")
				}

				// create a frame
				if err := framesAppender.Add(frame); err != nil {
					return errors.Wrap(err, "Failed to add frame")
				}

				return framesAppender.WaitForComplete(10 * time.Second)
			},
		}

		// submit the task
		f.taskPool.SubmitTask(&seriesCreationTask)

		// add the task
		seriesCreationTaskGroup.AddTask(&seriesCreationTask)
	}

	// wait for series
	taskGroupErrors := seriesCreationTaskGroup.Wait()

	return taskGroupErrors.Error()
}

func (f *framulate) getTableName(index int) string {
	return fmt.Sprintf("tsdb-%d", index)
}

func main() {
	framesURL := ""
	containerName := ""
	userName := ""
	accessKey := ""
	maxInflightRequests := 0
	numTables := 0
	numSeriesPerTable := 0

	flag.StringVar(&framesURL, "url", "", "")
	flag.StringVar(&containerName, "container-name", "", "")
	flag.StringVar(&userName, "username", "", "")
	flag.StringVar(&accessKey, "access-key", "", "")
	flag.IntVar(&maxInflightRequests, "max-inflight-requests", 256, "")
	flag.IntVar(&numTables, "num-tables", 16, "")
	flag.IntVar(&numSeriesPerTable, "num-series-per-table", 512, "")
	flag.Parse()

	framulateInstance, err := newFramulate(context.TODO(),
		framesURL,
		containerName,
		userName,
		accessKey,
		256)
	if err != nil {
		os.Exit(1)
	}

	if err := framulateInstance.start(numTables, numSeriesPerTable); err != nil {
		panic(errors.GetErrorStackString(err, 10))
	}
}
