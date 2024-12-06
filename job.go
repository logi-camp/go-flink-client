package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// JobManagerConfig returns the cluster configuration of
// job manager server.
func (c *Client) JobManagerConfig() ([]KV, error) {
	var r []KV
	req, err := http.NewRequest(
		"GET",
		c.url("/jobmanager/config"),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type Metric struct {
	ID string `json:"id"`
}

// JobManagerMetrics provides access to job manager
// metrics.
func (c *Client) JobManagerMetrics() ([]Metric, error) {
	var r []Metric
	req, err := http.NewRequest(
		"GET",
		c.url("/jobmanager/metrics"),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type JobsResp struct {
	Jobs []Job `json:"jobs"`
}

type Job struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// Jobs returns an overview over all jobs and their
// current state.
func (c *Client) Jobs() (JobsResp, error) {
	var r JobsResp
	req, err := http.NewRequest(
		"GET",
		c.url("/jobs"),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

// SubmitJob submits a job.
func (c *Client) SubmitJob() error {
	return fmt.Errorf("not implement")
}

type JobMetricsOpts struct {
	// Metrics (optional): string values to select
	// specific metrics.
	Metrics []string

	// Agg (optional): list of aggregation modes which
	// should be calculated. Available aggregations are:
	// "min, max, sum, avg".
	Agg []string

	// Jobs (optional): job list of 32-character
	// hexadecimal strings to select specific jobs.
	Jobs []string
}

// JobMetrics provides access to aggregated job metrics.
func (c *Client) JobMetrics(opts JobMetricsOpts) (map[string]interface{}, error) {
	var r map[string]interface{}
	req, err := http.NewRequest(
		"GET",
		c.url("/jobs/metrics"),
		nil,
	)
	if err != nil {
		return r, err
	}
	q := req.URL.Query()
	if len(opts.Metrics) > 0 {
		q.Add("get", strings.Join(opts.Metrics, ","))
	}

	if len(opts.Agg) > 0 {
		q.Add("agg", strings.Join(opts.Agg, ","))
	}

	if len(opts.Jobs) > 0 {
		q.Add("jobs", strings.Join(opts.Jobs, ","))
	}

	req.URL.RawQuery = q.Encode()

	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type OverviewResp struct {
	Jobs []JobOverview `json:"jobs"`
}

type JobOverview struct {
	ID               string `json:"jid"`
	Name             string `json:"name"`
	State            string `json:"state"`
	Start            int64  `json:"start-time"`
	End              int64  `json:"end-time"`
	Duration         int64  `json:"duration"`
	LastModification int64  `json:"last-modification"`
	Tasks            Status `json:"tasks"`
}

type Status struct {
	Total       int `json:"total,omitempty"`
	Created     int `json:"created"`
	Scheduled   int `json:"scheduled"`
	Deploying   int `json:"deploying"`
	Running     int `json:"running"`
	Finished    int `json:"finished"`
	Canceling   int `json:"canceling"`
	Canceled    int `json:"canceled"`
	Failed      int `json:"failed"`
	Reconciling int `json:"reconciling"`
}

// JobsOverview returns an overview over all jobs.
func (c *Client) JobsOverview() (OverviewResp, error) {
	var r OverviewResp
	req, err := http.NewRequest(
		"GET",
		c.url("/jobs/overview"),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type JobResp struct {
	ID          string `json:"jid"`
	Name        string `json:"name"`
	IsStoppable bool   `json:"isStoppable"`
	State       string `json:"state"`

	Start    int64 `json:"start-time"`
	End      int64 `json:"end-time"`
	Duration int64 `json:"duration"`
	Now      int64 `json:"now"`

	Timestamps   Timestamps `json:"timestamps"`
	Vertices     []Vertice  `json:"vertices"`
	StatusCounts Status     `json:"status-counts"`
	Plan         Plan       `json:"plan"`
}

type Timestamps struct {
	Canceled    int64 `json:"CANCELED"`
	Suspended   int64 `json:"SUSPENDED"`
	Finished    int64 `json:"FINISHED"`
	Canceling   int64 `json:"CANCELLING"`
	Running     int64 `json:"RUNNING"`
	Restarting  int64 `json:"RESTARTING"`
	Reconciling int64 `json:"RECONCILING"`
	Created     int64 `json:"CREATED"`
	Failed      int64 `json:"FAILED"`
	Failing     int64 `json:"FAILING"`
}

type Vertice struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Status      string                 `json:"status"`
	Parallelism int                    `json:"parallelism"`
	Start       int64                  `json:"start-time"`
	End         int64                  `json:"end-time"`
	Duration    int64                  `json:"duration"`
	Tasks       Status                 `json:"tasks"`
	Metrics     map[string]interface{} `json:"metrics"`
}

// Job returns details of a job.
func (c *Client) Job(jobID string) (JobResp, error) {
	var r JobResp
	uri := fmt.Sprintf("/jobs/%s", jobID)
	req, err := http.NewRequest(
		"GET",
		c.url(uri),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

// StopJob terminates a job.
func (c *Client) StopJob(jobID string) error {
	uri := fmt.Sprintf("/jobs/%s", jobID)
	req, err := http.NewRequest(
		"PATCH",
		c.url(uri),
		nil,
	)
	if err != nil {
		return err
	}
	_, err = c.client.Do(req)
	return err
}

type checkpointsResp struct {
	Counts  Counts                     `json:"counts"`
	Summary Summary                    `json:"summary"`
	Latest  Latest                     `json:"latest"`
	History []failedCheckpointsStatics `json:"history"`
}

type Counts struct {
	Restored   int `json:"restored"`
	Total      int `json:"total"`
	InProgress int `json:"in_progress"`
	Completed  int `json:"completed"`
	Failed     int `json:"failed"`
}

type Summary struct {
	StateSize         statics `json:"state_size"`
	End2EndDuration   statics `json:"end_to_end_duration"`
	AlignmentBuffered statics `json:"alignment_buffered"`
}

type statics struct {
	Min int `json:"min"`
	Max int `json:"max"`
	Avg int `json:"avg"`
}

type Latest struct {
	Completed CompletedCheckpointsStatics `json:"completed"`
	Savepoint savepointsStatics           `json:"savepoint"`
	Failed    failedCheckpointsStatics    `json:"failed"`
	Restored  restoredCheckpointsStatics  `json:"restored"`
}

type CompletedCheckpointsStatics struct {
	ID                      string                 `json:"id"`
	Status                  string                 `json:"status"`
	IsSavepoint             bool                   `json:"is_savepoint"`
	TriggerTimestamp        int64                  `json:"trigger_timestamp"`
	LatestAckTimestamp      int64                  `json:"latest_ack_timestamp"`
	StateSize               int64                  `json:"state_size"`
	End2EndDuration         int64                  `json:"end_to_end_duration"`
	AlignmentBuffered       int64                  `json:"alignment_buffered"`
	NumSubtasks             int64                  `json:"num_subtasks"`
	NumAcknowledgedSubtasks int64                  `json:"num_acknowledged_subtasks"`
	Tasks                   TaskCheckpointsStatics `json:"tasks"`
	ExternalPath            string                 `json:"external_path"`
	Discarded               bool                   `json:"discarded"`
}

type savepointsStatics struct {
	ID                      int                    `json:"id"`
	Status                  string                 `json:"status"`
	IsSavepoint             bool                   `json:"is_savepoint"`
	TriggerTimestamp        int64                  `json:"trigger_timestamp"`
	LatestAckTimestamp      int64                  `json:"latest_ack_timestamp"`
	StateSize               int64                  `json:"state_size"`
	End2EndDuration         int64                  `json:"end_to_end_duration"`
	AlignmentBuffered       int64                  `json:"alignment_buffered"`
	NumSubtasks             int64                  `json:"num_subtasks"`
	NumAcknowledgedSubtasks int64                  `json:"num_acknowledged_subtasks"`
	Tasks                   TaskCheckpointsStatics `json:"tasks"`
	ExternalPath            string                 `json:"external_path"`
	Discarded               bool                   `json:"discarded"`
}
type TaskCheckpointsStatics struct {
	ID     string `json:"id"`
	Status string `json:"status"`

	LatestAckTimestamp int64 `json:"latest_ack_timestamp"`

	FailureTimestamp int64  `json:"failure_timestamp"`
	FailureMessage   string `json:"failure_message"`

	StateSize               int64 `json:"state_size"`
	End2EndDuration         int64 `json:"end_to_end_duration"`
	AlignmentBuffered       int64 `json:"alignment_buffered"`
	NumSubtasks             int64 `json:"num_subtasks"`
	NumAcknowledgedSubtasks int64 `json:"num_acknowledged_subtasks"`
}

type failedCheckpointsStatics struct {
	ID                      int64                  `json:"id"`
	Status                  string                 `json:"status"`
	IsSavepoint             bool                   `json:"is_savepoint"`
	TriggerTimestamp        int64                  `json:"trigger_timestamp"`
	LatestAckTimestamp      int64                  `json:"latest_ack_timestamp"`
	StateSize               int64                  `json:"state_size"`
	End2EndDuration         int64                  `json:"end_to_end_duration"`
	AlignmentBuffered       int64                  `json:"alignment_buffered"`
	NumSubtasks             int64                  `json:"num_subtasks"`
	NumAcknowledgedSubtasks int64                  `json:"num_acknowledged_subtasks"`
	Tasks                   TaskCheckpointsStatics `json:"tasks"`
}

type restoredCheckpointsStatics struct {
	ID               int64  `json:"id"`
	RestoreTimestamp int64  `json:"restore_timestamp"`
	IsSavepoint      bool   `json:"is_savepoint"`
	ExternalPath     string `json:"external_path"`
}

// Checkpoints returns checkpointing statistics for a job.
func (c *Client) Checkpoints(jobID string) (checkpointsResp, error) {
	var r checkpointsResp
	uri := fmt.Sprintf("/jobs/%s/checkpoints", jobID)
	req, err := http.NewRequest(
		"GET",
		c.url(uri),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type SavePointsResp struct {
	RequestID string `json:"request-id"`
}

// SavePoints triggers a savepoint, and optionally cancels the
// job afterwards. This async operation would return a
// 'triggerid' for further query identifier.
func (c *Client) SavePoints(jobID string, saveDir string, cancelJob bool) (SavePointsResp, error) {
	var r SavePointsResp

	type SavePointsReq struct {
		SaveDir   string `json:"target-directory"`
		CancelJob bool   `json:"cancel-job"`
	}

	d := SavePointsReq{
		SaveDir:   saveDir,
		CancelJob: cancelJob,
	}
	data := new(bytes.Buffer)
	json.NewEncoder(data).Encode(d)
	uri := fmt.Sprintf("/jobs/%s/savepoints", jobID)
	req, err := http.NewRequest(
		"POST",
		c.url(uri),
		data,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type SavepointStatusId string

const (
	SavepointStatusInProgress  SavepointStatusId = "IN_PROGRESS"
	SavepointStatusInCompleted SavepointStatusId = "COMPLETED"
)

type TrackSavepointRespFailureCause struct {
	Class               string `json:"class"`
	StackTrace          string `json:"stack-trace"`
	SerializedThrowable string `json:"serialized-throwable"`
}

type TrackSavepointRespOperation struct {
	FailureCause TrackSavepointRespFailureCause `json:"failure-cause"`
	Location     string                         `json:"location"`
}
type TrackSavepointRespStatus struct {
	Id SavepointStatusId `json:"id"`
}
type TrackSavepointResp struct {
	Status    TrackSavepointRespStatus    `json:"status"`
	Operation TrackSavepointRespOperation `json:"operation"`
}

// Check the status of triggered savepoint
func (c *Client) TrackSavepoint(jobID string, triggerId string) (TrackSavepointResp, error) {
	var r TrackSavepointResp

	uri := fmt.Sprintf("/jobs/%s/savepoints/%s", jobID, triggerId)
	req, err := http.NewRequest(
		"GET",
		c.url(uri),
		nil,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}

type StopJobResp struct {
	RequestID string `json:"request-id"`
}

// StopJob stops a job with a savepoint. Optionally, it can also
// emit a MAX_WATERMARK before taking the savepoint to flush out
// any state waiting for timers to fire. This async operation
// would return a 'triggerid' for further query identifier.
func (c *Client) StopJobWithSavepoint(jobID string, saveDir string, drain bool) (StopJobResp, error) {
	var r StopJobResp
	type StopJobReq struct {
		SaveDir string `json:"targetDirectory"`
		Drain   bool   `json:"drain"`
	}

	d := StopJobReq{
		SaveDir: saveDir,
		Drain:   drain,
	}
	data := new(bytes.Buffer)
	json.NewEncoder(data).Encode(d)
	uri := fmt.Sprintf("/jobs/%s/stop", jobID)
	req, err := http.NewRequest(
		"POST",
		c.url(uri),
		data,
	)
	if err != nil {
		return r, err
	}
	b, err := c.client.Do(req)
	if err != nil {
		return r, err
	}
	err = json.Unmarshal(b, &r)
	return r, err
}
