package repeatingtask

import (
	"context"
	"testing"
	"time"

	"github.com/nuclio/logger"
	nucliozap "github.com/nuclio/zap"
	"github.com/stretchr/testify/suite"
)

type poolSuite struct {
	suite.Suite
	pool   *Pool
	logger logger.Logger
	ctx    context.Context
}

func (suite *poolSuite) SetupTest() {
	var err error

	suite.logger, _ = nucliozap.NewNuclioZapTest("test")
	suite.ctx = context.Background()

	suite.pool, err = NewPool(context.TODO(), 16, 4)
	suite.Require().NoError(err)
}

func (suite *poolSuite) TestNoParallel() {
	suite.T().Skip()

	task := &Task{
		NumReptitions:  16,
		MaxParallel:    1,
		MaxNumFailures: 0,
		Handler:        suite.delayingNoConcurrentHandler,
		Cookie:         100 * time.Millisecond,
	}

	err := suite.pool.SubmitTask(task)
	suite.Require().NoError(err)

	<-task.OnCompleteChan
}

func (suite *poolSuite) TestParallel() {
	task := &Task{
		NumReptitions:  512,
		MaxParallel:    4,
		MaxNumFailures: 0,
		Handler:        suite.delayingNoConcurrentHandler,
		Cookie:         1000 * time.Millisecond,
	}

	err := suite.pool.SubmitTask(task)
	suite.Require().NoError(err)

	<-task.OnCompleteChan
}

func (suite *poolSuite) delayingNoConcurrentHandler(cookie interface{}, repetitionIndex int) error {
	suite.logger.DebugWith("Called", "rep", repetitionIndex)

	// TODO: test not running in parallel
	time.Sleep(cookie.(time.Duration))

	return nil
}

func (suite *poolSuite) delayingHandler(cookie interface{}, repetitionIndex int) error {
	suite.logger.DebugWith("Called", "rep", repetitionIndex)

	// TODO: test not running in parallel
	time.Sleep(cookie.(time.Duration))

	return nil
}

func TestPoolSuite(t *testing.T) {
	suite.Run(t, new(poolSuite))
}
