// Package flow provides structured concurrency and workflow pipelines for flow.
package flow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// NodeStatus represents NodeStatus.
type NodeStatus string

const (
	// NodeStatusSuccess defines NodeStatusSuccess.
	NodeStatusSuccess NodeStatus = "SUCCESS"
	// NodeStatusFailed defines NodeStatusFailed.
	NodeStatusFailed NodeStatus = "FAILED"
	// NodeStatusSkipped defines NodeStatusSkipped.
	NodeStatusSkipped NodeStatus = "SKIPPED"
)

// NodeReport represents NodeReport.
type NodeReport struct {
	Name      string
	Status    NodeStatus
	Duration  time.Duration
	StartTime time.Time
	EndTime   time.Time
	Err       error
}

// DAGReport represents DAGReport.
type DAGReport struct {
	Duration  time.Duration
	StartTime time.Time
	EndTime   time.Time
	Nodes     []NodeReport
	nodeMap   map[string]*NodeReport
}

// Node executes the Node operation.
func (r *DAGReport) Node(name string) *NodeReport {
	if r == nil {
		return nil
	}
	if r.nodeMap == nil {
		r.nodeMap = make(map[string]*NodeReport, len(r.Nodes))
		for i := range r.Nodes {
			r.nodeMap[r.Nodes[i].Name] = &r.Nodes[i]
		}
	}
	return r.nodeMap[name]
}

// Successful executes the Successful operation.
func (r *DAGReport) Successful() []NodeReport {
	if r == nil {
		return nil
	}
	res := make([]NodeReport, 0, len(r.Nodes))
	for _, n := range r.Nodes {
		if n.Status == NodeStatusSuccess {
			res = append(res, n)
		}
	}
	return res
}

// Failed executes the Failed operation.
func (r *DAGReport) Failed() []NodeReport {
	if r == nil {
		return nil
	}
	res := make([]NodeReport, 0, len(r.Nodes))
	for _, n := range r.Nodes {
		if n.Status == NodeStatusFailed {
			res = append(res, n)
		}
	}
	return res
}

// Skipped executes the Skipped operation.
func (r *DAGReport) Skipped() []NodeReport {
	if r == nil {
		return nil
	}
	res := make([]NodeReport, 0, len(r.Nodes))
	for _, n := range r.Nodes {
		if n.Status == NodeStatusSkipped {
			res = append(res, n)
		}
	}
	return res
}

// DAGWithReport performs the DAGWithReport operation.
func DAGWithReport[T context.Context](nodes ...*DAGNode[T]) func(ctx T) (*DAGReport, error) {
	if len(nodes) == 0 {
		return func(ctx T) (*DAGReport, error) {
			return &DAGReport{}, nil
		}
	}

	compiled, compileErr := compileDAG(nodes)
	if compileErr != nil {
		return func(ctx T) (*DAGReport, error) {
			return nil, compileErr
		}
	}

	n := len(compiled)
	return func(ctx T) (*DAGReport, error) {
		startTime := time.Now()
		reports := make([]NodeReport, n)
		for i := range compiled {
			reports[i].Name = compiled[i].name
		}

		if ctx.Err() != nil {
			for i := range reports {
				reports[i].Status = NodeStatusFailed
				reports[i].Err = ctx.Err()
			}
			endTime := time.Now()
			return &DAGReport{
				StartTime: startTime,
				EndTime:   endTime,
				Duration:  endTime.Sub(startTime),
				Nodes:     reports,
			}, ctx.Err()
		}

		var failed atomic.Bool
		var hasErr atomic.Bool
		inDegrees := make([]atomic.Int32, n)
		for i := range compiled {
			inDegrees[i].Store(compiled[i].inDegree)
		}

		var wg sync.WaitGroup
		var runNode func(idx int)

		runNode = func(idx int) {
			defer wg.Done()

			if failed.Load() || ctx.Err() != nil {
				return
			}

			if compiled[idx].condition != nil && !compiled[idx].condition(ctx) {
				reports[idx].Status = NodeStatusSkipped
				for _, dep := range compiled[idx].dependents {
					if inDegrees[dep].Add(-1) == 0 {
						wg.Add(1)
						go runNode(dep)
					}
				}
				return
			}

			reports[idx].StartTime = time.Now()
			if compiled[idx].step != nil {
				reports[idx].Err = compiled[idx].step(ctx)
			}
			reports[idx].EndTime = time.Now()
			reports[idx].Duration = reports[idx].EndTime.Sub(reports[idx].StartTime)

			if reports[idx].Err != nil {
				reports[idx].Status = NodeStatusFailed
				hasErr.Store(true)
				failed.Store(true)
				return
			}
			reports[idx].Status = NodeStatusSuccess

			if failed.Load() || ctx.Err() != nil {
				return
			}

			for _, dep := range compiled[idx].dependents {
				if inDegrees[dep].Add(-1) == 0 {
					wg.Add(1)
					go runNode(dep)
				}
			}
		}

		for i := range compiled {
			if compiled[i].inDegree == 0 {
				wg.Add(1)
				go runNode(i)
			}
		}

		wg.Wait()
		endTime := time.Now()

		if ctx.Err() != nil {
			for i := range reports {
				if reports[i].Status == "" {
					reports[i].Status = NodeStatusFailed
					reports[i].Err = ctx.Err()
					hasErr.Store(true)
				}
			}
		}

		report := &DAGReport{
			StartTime: startTime,
			EndTime:   endTime,
			Duration:  endTime.Sub(startTime),
			Nodes:     reports,
		}

		if !hasErr.Load() {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			return report, nil
		}

		errs := make([]error, 0, n)
		for i := range reports {
			if reports[i].Err != nil {
				errs = append(errs, reports[i].Err)
			}
		}
		return report, errors.Join(errs...)
	}
}

// DAGNWithReport performs the DAGNWithReport operation.
func DAGNWithReport[T context.Context](limit int, nodes ...*DAGNode[T]) func(ctx T) (*DAGReport, error) {
	if limit <= 0 || limit >= len(nodes) {
		return DAGWithReport(nodes...)
	}

	compiled, compileErr := compileDAG(nodes)
	if compileErr != nil {
		return func(ctx T) (*DAGReport, error) {
			return nil, compileErr
		}
	}

	n := len(compiled)
	return func(ctx T) (*DAGReport, error) {
		startTime := time.Now()
		reports := make([]NodeReport, n)
		for i := range compiled {
			reports[i].Name = compiled[i].name
		}

		if ctx.Err() != nil {
			for i := range reports {
				reports[i].Status = NodeStatusFailed
				reports[i].Err = ctx.Err()
			}
			endTime := time.Now()
			return &DAGReport{
				StartTime: startTime,
				EndTime:   endTime,
				Duration:  endTime.Sub(startTime),
				Nodes:     reports,
			}, ctx.Err()
		}

		sem := make(chan struct{}, limit)
		var failed atomic.Bool
		var hasErr atomic.Bool
		inDegrees := make([]atomic.Int32, n)
		for i := range compiled {
			inDegrees[i].Store(compiled[i].inDegree)
		}

		var wg sync.WaitGroup
		var runNode func(idx int)

		runNode = func(idx int) {
			defer wg.Done()

			if compiled[idx].condition != nil && !compiled[idx].condition(ctx) {
				reports[idx].Status = NodeStatusSkipped
				for _, dep := range compiled[idx].dependents {
					if inDegrees[dep].Add(-1) == 0 {
						wg.Add(1)
						go runNode(dep)
					}
				}
				return
			}

			reports[idx].StartTime = time.Now()
			if compiled[idx].step != nil {
				select {
				case <-ctx.Done():
					reports[idx].Status = NodeStatusFailed
					reports[idx].Err = ctx.Err()
					reports[idx].EndTime = time.Now()
					reports[idx].Duration = reports[idx].EndTime.Sub(reports[idx].StartTime)
					hasErr.Store(true)
					failed.Store(true)
					return
				case sem <- struct{}{}:
				}

				if failed.Load() || ctx.Err() != nil {
					<-sem
					return
				}

				reports[idx].Err = compiled[idx].step(ctx)
				reports[idx].EndTime = time.Now()
				reports[idx].Duration = reports[idx].EndTime.Sub(reports[idx].StartTime)
				if reports[idx].Err != nil {
					reports[idx].Status = NodeStatusFailed
					hasErr.Store(true)
					failed.Store(true)
					<-sem
					return
				}
				<-sem
			} else {
				reports[idx].EndTime = time.Now()
				reports[idx].Duration = reports[idx].EndTime.Sub(reports[idx].StartTime)
			}
			reports[idx].Status = NodeStatusSuccess

			if failed.Load() || ctx.Err() != nil {
				return
			}

			for _, dep := range compiled[idx].dependents {
				if inDegrees[dep].Add(-1) == 0 {
					wg.Add(1)
					go runNode(dep)
				}
			}
		}

		for i := range compiled {
			if compiled[i].inDegree == 0 {
				wg.Add(1)
				go runNode(i)
			}
		}

		wg.Wait()
		endTime := time.Now()

		if ctx.Err() != nil {
			for i := range reports {
				if reports[i].Status == "" {
					reports[i].Status = NodeStatusFailed
					reports[i].Err = ctx.Err()
					hasErr.Store(true)
				}
			}
		}

		report := &DAGReport{
			StartTime: startTime,
			EndTime:   endTime,
			Duration:  endTime.Sub(startTime),
			Nodes:     reports,
		}

		if !hasErr.Load() {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			return report, nil
		}

		errs := make([]error, 0, n)
		for i := range reports {
			if reports[i].Err != nil {
				errs = append(errs, reports[i].Err)
			}
		}
		return report, errors.Join(errs...)
	}
}

// DAGEdgesWithReport performs the DAGEdgesWithReport operation.
func DAGEdgesWithReport[T context.Context](connections ...DAGConnection[T]) func(ctx T) (*DAGReport, error) {
	if len(connections) == 0 {
		return func(ctx T) (*DAGReport, error) {
			return &DAGReport{}, nil
		}
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return func(ctx T) (*DAGReport, error) {
			return nil, err
		}
	}
	return DAGWithReport(nodes...)
}

// DAGEdgesNWithReport performs the DAGEdgesNWithReport operation.
func DAGEdgesNWithReport[T context.Context](limit int, connections ...DAGConnection[T]) func(ctx T) (*DAGReport, error) {
	if len(connections) == 0 {
		return func(ctx T) (*DAGReport, error) {
			return &DAGReport{}, nil
		}
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return func(ctx T) (*DAGReport, error) {
			return nil, err
		}
	}
	return DAGNWithReport(limit, nodes...)
}
