// Package flow provides structured concurrency and workflow pipelines for flow.
package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrDAGCycle defines ErrDAGCycle.
	ErrDAGCycle = errors.New("cycle detected in DAG")
	// ErrDAGUnknownDependency defines ErrDAGUnknownDependency.
	ErrDAGUnknownDependency = errors.New("unknown dependency in DAG")
	// ErrDAGDuplicateNode defines ErrDAGDuplicateNode.
	ErrDAGDuplicateNode = errors.New("duplicate node name in DAG")
	// ErrDAGNilNode defines ErrDAGNilNode.
	ErrDAGNilNode = errors.New("nil node in DAG")
	// ErrDAGEmptyNodeName defines ErrDAGEmptyNodeName.
	ErrDAGEmptyNodeName = errors.New("empty node name in DAG")
)

// DAGNode represents DAGNode.
type DAGNode[T context.Context] struct {
	name      string
	step      Step[T]
	dependsOn []string
	condition func(ctx T) bool
}

// DAGConnection represents DAGConnection.
type DAGConnection[T context.Context] struct {
	from Step[T]
	to   []Step[T]
}

// DAGEdgeBuilder represents DAGEdgeBuilder.
type DAGEdgeBuilder[T context.Context] struct {
	from Step[T]
}

// Node performs the Node operation.
func Node[T context.Context](name string, step Step[T]) *DAGNode[T] {
	return &DAGNode[T]{
		name: name,
		step: step,
	}
}

// Edge performs the Edge operation.
func Edge[T context.Context](from Step[T], to ...Step[T]) DAGConnection[T] {
	return DAGConnection[T]{
		from: from,
		to:   to,
	}
}

// From performs the From operation.
func From[T context.Context](from Step[T]) DAGEdgeBuilder[T] {
	return DAGEdgeBuilder[T]{from: from}
}

// To executes the To operation.
func (b DAGEdgeBuilder[T]) To(to ...Step[T]) DAGConnection[T] {
	return DAGConnection[T]{
		from: b.from,
		to:   to,
	}
}

// After executes the After operation.
func (n *DAGNode[T]) After(deps ...string) *DAGNode[T] {
	n.dependsOn = append(n.dependsOn, deps...)
	return n
}

// When executes the When operation.
func (n *DAGNode[T]) When(predicate func(ctx T) bool) *DAGNode[T] {
	if predicate != nil {
		n.condition = predicate
	}
	return n
}

// Unless executes the Unless operation.
func (n *DAGNode[T]) Unless(predicate func(ctx T) bool) *DAGNode[T] {
	if predicate != nil {
		n.condition = func(ctx T) bool {
			return !predicate(ctx)
		}
	}
	return n
}

// WithTimeout executes the WithTimeout operation.
func (n *DAGNode[T]) WithTimeout(d time.Duration) *DAGNode[T] {
	if n.step != nil {
		n.step = n.step.Timeout(d)
	}
	return n
}

// WithRetry executes the WithRetry operation.
func (n *DAGNode[T]) WithRetry(attempts int, delay ...time.Duration) *DAGNode[T] {
	if n.step != nil {
		var d time.Duration
		if len(delay) > 0 {
			d = delay[0]
		}
		n.step = n.step.Retry(attempts, d)
	}
	return n
}

// WithRecover executes the WithRecover operation.
func (n *DAGNode[T]) WithRecover() *DAGNode[T] {
	if n.step != nil {
		n.step = n.step.Recover()
	}
	return n
}

// WithCatch executes the WithCatch operation.
func (n *DAGNode[T]) WithCatch(handler func(ctx T, err error) error) *DAGNode[T] {
	if n.step != nil {
		n.step = n.step.Catch(handler)
	}
	return n
}

// WithFallback executes the WithFallback operation.
func (n *DAGNode[T]) WithFallback(fallback Step[T]) *DAGNode[T] {
	if n.step != nil {
		n.step = n.step.Fallback(fallback)
	}
	return n
}

func findCyclePath[T context.Context](nodes []*DAGNode[T], nodeMap map[string]*DAGNode[T], inDegree map[string]int) []string {
	var start string
	for _, n := range nodes {
		if inDegree[n.name] > 0 {
			start = n.name
			break
		}
	}

	pos := make(map[string]int)
	var path []string
	curr := start

	for {
		if idx, exists := pos[curr]; exists {
			cycle := append([]string{}, path[idx:]...)
			cycle = append(cycle, curr)
			for i, j := 0, len(cycle)-1; i < j; i, j = i+1, j-1 {
				cycle[i], cycle[j] = cycle[j], cycle[i]
			}
			return cycle
		}

		pos[curr] = len(path)
		path = append(path, curr)

		node := nodeMap[curr]
		for _, dep := range node.dependsOn {
			if inDegree[dep] > 0 {
				curr = dep
				break
			}
		}
	}
}

func validateDAG[T context.Context](nodes []*DAGNode[T]) error {
	nodeMap := make(map[string]*DAGNode[T], len(nodes))
	for _, n := range nodes {
		if n == nil {
			return ErrDAGNilNode
		}
		if n.name == "" {
			return ErrDAGEmptyNodeName
		}
		if _, exists := nodeMap[n.name]; exists {
			return fmt.Errorf("%w: %s", ErrDAGDuplicateNode, n.name)
		}
		nodeMap[n.name] = n
	}

	inDegree := make(map[string]int, len(nodes))
	graph := make(map[string][]string, len(nodes))

	for _, n := range nodes {
		inDegree[n.name] = len(n.dependsOn)
		for _, dep := range n.dependsOn {
			if dep == n.name {
				return fmt.Errorf("%w: node %s cannot depend on itself", ErrDAGCycle, n.name)
			}
			if _, exists := nodeMap[dep]; !exists {
				return fmt.Errorf("%w: unknown dependency %q for node %q", ErrDAGUnknownDependency, dep, n.name)
			}
			graph[dep] = append(graph[dep], n.name)
		}
	}

	q := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if inDegree[n.name] == 0 {
			q = append(q, n.name)
		}
	}

	visited := 0
	for len(q) > 0 {
		curr := q[0]
		q = q[1:]
		visited++

		for _, next := range graph[curr] {
			inDegree[next]--
			if inDegree[next] == 0 {
				q = append(q, next)
			}
		}
	}

	if visited != len(nodes) {
		cycle := findCyclePath(nodes, nodeMap, inDegree)
		return fmt.Errorf("%w: %s", ErrDAGCycle, strings.Join(cycle, " -> "))
	}

	return nil
}

type compiledDAGNode[T context.Context] struct {
	name       string
	step       Step[T]
	depIdxs    []int
	dependents []int
	inDegree   int32
	condition  func(ctx T) bool
}

func compileDAG[T context.Context](nodes []*DAGNode[T]) ([]compiledDAGNode[T], error) {
	if err := validateDAG(nodes); err != nil {
		return nil, err
	}
	nodeIndices := make(map[string]int, len(nodes))
	for i, n := range nodes {
		nodeIndices[n.name] = i
	}
	compiled := make([]compiledDAGNode[T], len(nodes))
	for i, n := range nodes {
		compiled[i].name = n.name
		compiled[i].step = n.step
		compiled[i].condition = n.condition
		if len(n.dependsOn) > 0 {
			compiled[i].depIdxs = make([]int, len(n.dependsOn))
			compiled[i].inDegree = int32(len(n.dependsOn))
			for j, dep := range n.dependsOn {
				depIdx := nodeIndices[dep]
				compiled[i].depIdxs[j] = depIdx
			}
		}
	}
	for i, n := range nodes {
		for _, dep := range n.dependsOn {
			depIdx := nodeIndices[dep]
			compiled[depIdx].dependents = append(compiled[depIdx].dependents, i)
		}
	}
	return compiled, nil
}

// DAG performs the DAG operation.
func DAG[T context.Context](nodes ...*DAGNode[T]) Step[T] {
	if len(nodes) == 0 {
		return func(ctx T) error { return nil }
	}

	compiled, compileErr := compileDAG(nodes)
	if compileErr != nil {
		return func(ctx T) error { return compileErr }
	}

	n := len(compiled)
	return func(ctx T) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		type execNode struct {
			inDegree atomic.Int32
			err      error
		}
		state := make([]execNode, n)
		for i := range compiled {
			state[i].inDegree.Store(compiled[i].inDegree)
		}

		var hasErr atomic.Bool
		var failed atomic.Bool
		var wg sync.WaitGroup
		var runNode func(idx int)

		runNode = func(idx int) {
			defer wg.Done()

			if failed.Load() || ctx.Err() != nil {
				return
			}

			if compiled[idx].condition != nil && !compiled[idx].condition(ctx) {
				for _, dep := range compiled[idx].dependents {
					if state[dep].inDegree.Add(-1) == 0 {
						wg.Add(1)
						go runNode(dep)
					}
				}
				return
			}

			if compiled[idx].step != nil {
				if err := compiled[idx].step(ctx); err != nil {
					state[idx].err = err
					hasErr.Store(true)
					failed.Store(true)
					return
				}
			}

			if failed.Load() || ctx.Err() != nil {
				return
			}

			for _, dep := range compiled[idx].dependents {
				if state[dep].inDegree.Add(-1) == 0 {
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
		if ctx.Err() != nil && !hasErr.Load() {
			return ctx.Err()
		}
		if !hasErr.Load() {
			return nil
		}
		var joinedErr error
		for i := range state {
			if state[i].err != nil {
				joinedErr = errors.Join(joinedErr, state[i].err)
			}
		}
		return joinedErr
	}
}

// DAGN performs the DAGN operation.
func DAGN[T context.Context](limit int, nodes ...*DAGNode[T]) Step[T] {
	if limit <= 0 || limit >= len(nodes) {
		return DAG(nodes...)
	}

	compiled, compileErr := compileDAG(nodes)
	if compileErr != nil {
		return func(ctx T) error { return compileErr }
	}

	n := len(compiled)
	return func(ctx T) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		type execNode struct {
			inDegree atomic.Int32
			err      error
		}
		state := make([]execNode, n)
		for i := range compiled {
			state[i].inDegree.Store(compiled[i].inDegree)
		}

		var hasErr atomic.Bool
		var failed atomic.Bool
		sem := make(chan struct{}, limit)
		var wg sync.WaitGroup
		var runNode func(idx int)

		runNode = func(idx int) {
			defer wg.Done()

			if compiled[idx].condition != nil && !compiled[idx].condition(ctx) {
				for _, dep := range compiled[idx].dependents {
					if state[dep].inDegree.Add(-1) == 0 {
						wg.Add(1)
						go runNode(dep)
					}
				}
				return
			}

			if compiled[idx].step != nil {
				select {
				case <-ctx.Done():
					state[idx].err = ctx.Err()
					hasErr.Store(true)
					failed.Store(true)
					return
				case sem <- struct{}{}:
				}

				if failed.Load() || ctx.Err() != nil {
					<-sem
					return
				}

				err := compiled[idx].step(ctx)
				if err != nil {
					state[idx].err = err
					hasErr.Store(true)
					failed.Store(true)
					<-sem
					return
				}
				<-sem
			}

			if failed.Load() || ctx.Err() != nil {
				return
			}

			for _, dep := range compiled[idx].dependents {
				if state[dep].inDegree.Add(-1) == 0 {
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
		if ctx.Err() != nil && !hasErr.Load() {
			return ctx.Err()
		}
		if !hasErr.Load() {
			return nil
		}
		var joinedErr error
		for i := range state {
			if state[i].err != nil {
				joinedErr = errors.Join(joinedErr, state[i].err)
			}
		}
		return joinedErr
	}
}

func buildNodesFromConnections[T context.Context](connections []DAGConnection[T]) ([]*DAGNode[T], error) {
	nodeMap := make(map[uintptr]*DAGNode[T])
	order := make([]uintptr, 0)

	getNode := func(s Step[T]) (*DAGNode[T], error) {
		if s == nil {
			return nil, errors.New("nil step in DAG edge")
		}
		ptr := reflect.ValueOf(s).Pointer()
		if node, exists := nodeMap[ptr]; exists {
			return node, nil
		}
		name := fmt.Sprintf("fn_%x", ptr)
		node := &DAGNode[T]{
			name: name,
			step: s,
		}
		nodeMap[ptr] = node
		order = append(order, ptr)
		return node, nil
	}

	for _, conn := range connections {
		fromNode, err := getNode(conn.from)
		if err != nil {
			return nil, err
		}
		for _, toStep := range conn.to {
			toNode, err := getNode(toStep)
			if err != nil {
				return nil, err
			}
			toNode.dependsOn = append(toNode.dependsOn, fromNode.name)
		}
	}

	nodes := make([]*DAGNode[T], 0, len(order))
	for _, ptr := range order {
		nodes = append(nodes, nodeMap[ptr])
	}
	return nodes, nil
}

// DAGEdges performs the DAGEdges operation.
func DAGEdges[T context.Context](connections ...DAGConnection[T]) Step[T] {
	if len(connections) == 0 {
		return func(ctx T) error { return nil }
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return func(ctx T) error { return err }
	}
	return DAG(nodes...)
}

// DAGEdgesN performs the DAGEdgesN operation.
func DAGEdgesN[T context.Context](limit int, connections ...DAGConnection[T]) Step[T] {
	if len(connections) == 0 {
		return func(ctx T) error { return nil }
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return func(ctx T) error { return err }
	}
	return DAGN(limit, nodes...)
}

// DAGPlan represents DAGPlan.
type DAGPlan[T context.Context] struct {
	nodes []*DAGNode[T]
}

// NewDAG performs the NewDAG operation.
func NewDAG[T context.Context](nodes ...*DAGNode[T]) *DAGPlan[T] {
	return &DAGPlan[T]{nodes: nodes}
}

// NewDAGEdges performs the NewDAGEdges operation.
func NewDAGEdges[T context.Context](connections ...DAGConnection[T]) (*DAGPlan[T], error) {
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return nil, err
	}
	return &DAGPlan[T]{nodes: nodes}, nil
}

// Step executes the Step operation.
func (p *DAGPlan[T]) Step() Step[T] {
	if p == nil {
		return func(ctx T) error { return nil }
	}
	return DAG(p.nodes...)
}

// StepN executes the StepN operation.
func (p *DAGPlan[T]) StepN(limit int) Step[T] {
	if p == nil {
		return func(ctx T) error { return nil }
	}
	return DAGN(limit, p.nodes...)
}

// ExecWithReport executes the ExecWithReport operation.
func (p *DAGPlan[T]) ExecWithReport(ctx T) (*DAGReport, error) {
	if p == nil || len(p.nodes) == 0 {
		return &DAGReport{}, nil
	}
	return DAGWithReport(p.nodes...)(ctx)
}

// ExecNWithReport executes the ExecNWithReport operation.
func (p *DAGPlan[T]) ExecNWithReport(limit int, ctx T) (*DAGReport, error) {
	if p == nil || len(p.nodes) == 0 {
		return &DAGReport{}, nil
	}
	return DAGNWithReport(limit, p.nodes...)(ctx)
}

// StepWithReport executes the StepWithReport operation.
func (p *DAGPlan[T]) StepWithReport() func(ctx T) (*DAGReport, error) {
	if p == nil || len(p.nodes) == 0 {
		return func(ctx T) (*DAGReport, error) { return &DAGReport{}, nil }
	}
	return DAGWithReport(p.nodes...)
}

// StepNWithReport executes the StepNWithReport operation.
func (p *DAGPlan[T]) StepNWithReport(limit int) func(ctx T) (*DAGReport, error) {
	if p == nil || len(p.nodes) == 0 {
		return func(ctx T) (*DAGReport, error) { return &DAGReport{}, nil }
	}
	return DAGNWithReport(limit, p.nodes...)
}

// Validate executes the Validate operation.
func (p *DAGPlan[T]) Validate() error {
	if p == nil || len(p.nodes) == 0 {
		return nil
	}
	return validateDAG(p.nodes)
}

// ToMermaid executes the ToMermaid operation.
func (p *DAGPlan[T]) ToMermaid() (string, error) {
	if p == nil {
		return "graph TD", nil
	}
	return DAGToMermaid(p.nodes...)
}

// ToDOT executes the ToDOT operation.
func (p *DAGPlan[T]) ToDOT() (string, error) {
	if p == nil {
		return "digraph DAG {\n}", nil
	}
	return DAGToDOT(p.nodes...)
}

func escapeMermaidID(name string) string {
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	s := sb.String()
	if s == "" {
		return "node"
	}
	if s[0] >= '0' && s[0] <= '9' {
		return "n_" + s
	}
	return s
}

// DAGToMermaid performs the DAGToMermaid operation.
func DAGToMermaid[T context.Context](nodes ...*DAGNode[T]) (string, error) {
	if len(nodes) == 0 {
		return "graph TD", nil
	}
	if err := validateDAG(nodes); err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("graph TD\n")

	hasEdge := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		for _, dep := range n.dependsOn {
			hasEdge[dep] = true
			hasEdge[n.name] = true
			fmt.Fprintf(&sb, "    %s[\"%s\"] --> %s[\"%s\"]\n", escapeMermaidID(dep), dep, escapeMermaidID(n.name), n.name)
		}
	}

	for _, n := range nodes {
		if !hasEdge[n.name] {
			fmt.Fprintf(&sb, "    %s[\"%s\"]\n", escapeMermaidID(n.name), n.name)
		}
	}

	return strings.TrimRight(sb.String(), "\n"), nil
}

// DAGToDOT performs the DAGToDOT operation.
func DAGToDOT[T context.Context](nodes ...*DAGNode[T]) (string, error) {
	if len(nodes) == 0 {
		return "digraph DAG {\n}", nil
	}
	if err := validateDAG(nodes); err != nil {
		return "", err
	}

	var sb strings.Builder
	sb.WriteString("digraph DAG {\n")

	hasEdge := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		for _, dep := range n.dependsOn {
			hasEdge[dep] = true
			hasEdge[n.name] = true
			fmt.Fprintf(&sb, "  \"%s\" -> \"%s\";\n", dep, n.name)
		}
	}

	for _, n := range nodes {
		if !hasEdge[n.name] {
			fmt.Fprintf(&sb, "  \"%s\";\n", n.name)
		}
	}

	sb.WriteString("}")
	return sb.String(), nil
}

// DAGEdgesToMermaid performs the DAGEdgesToMermaid operation.
func DAGEdgesToMermaid[T context.Context](connections ...DAGConnection[T]) (string, error) {
	if len(connections) == 0 {
		return "graph TD", nil
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return "", err
	}
	return DAGToMermaid(nodes...)
}

// DAGEdgesToDOT performs the DAGEdgesToDOT operation.
func DAGEdgesToDOT[T context.Context](connections ...DAGConnection[T]) (string, error) {
	if len(connections) == 0 {
		return "digraph DAG {\n}", nil
	}
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return "", err
	}
	return DAGToDOT(nodes...)
}
