package repeatingtask

import "fmt"

type TaskGroupErrors struct {
	taskErrors []TaskErrors
}

func (tge *TaskGroupErrors) Errors() []error {
	var errors []error

	for _, err := range tge.taskErrors {
		if err.Error() != nil {
			errors = append(errors, err.Error())
		}
	}

	return errors
}

func (tge *TaskGroupErrors) Error() error {
	errors := tge.Errors()

	if len(errors) == 0 {
		return nil
	}

	errorString := ""
	for _, err := range errors {
		errorString += fmt.Sprintf("%s\n", err.Error())
	}

	return nil
}

type TaskGroup struct {
	tasks []*Task
}

func (t *TaskGroup) AddTask(task *Task) error {
	t.tasks = append(t.tasks, task)

	return nil
}

func (t *TaskGroup) Wait() TaskGroupErrors {
	taskGroupErrors := TaskGroupErrors{}

	// iterate over tasks and read into task group errors
	for _, task := range t.tasks {

		// wait for task and add task errors
		taskGroupErrors.taskErrors = append(taskGroupErrors.taskErrors, task.wait())
	}

	return taskGroupErrors
}