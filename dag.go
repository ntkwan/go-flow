package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

var (
	ErrDAGCycle             = errors.New("cycle detected in DAG")
	ErrDAGUnknownDependency = errors.New("unknown dependency in DAG")
	ErrDAGDuplicateNode     = errors.New("duplicate node name in DAG")
	ErrDAGNilNode           = errors.New("nil node in DAG")
	ErrDAGEmptyNodeName     = errors.New("empty node name in DAG")
)

type DAGNode[T context.Context] struct {
	name      string
	step      Step[T]
	dependsOn []string
}

type DAGConnection[T context.Context] struct {
	from Step[T]
	to   []Step[T]
}

type DAGEdgeBuilder[T context.Context] struct {
	from Step[T]
}

func Node[T context.Context](name string, step Step[T]) *DAGNode[T] {
	return &DAGNode[T]{
		name: name,
		step: step,
	}
}

func Edge[T context.Context](from Step[T], to ...Step[T]) DAGConnection[T] {
	return DAGConnection[T]{
		from: from,
		to:   to,
	}
}

func From[T context.Context](from Step[T]) DAGEdgeBuilder[T] {
	return DAGEdgeBuilder[T]{from: from}
}

func (b DAGEdgeBuilder[T]) To(to ...Step[T]) DAGConnection[T] {
	return DAGConnection[T]{
		from: b.from,
		to:   to,
	}
}

func (n *DAGNode[T]) After(deps ...string) *DAGNode[T] {
	n.dependsOn = append(n.dependsOn, deps...)
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
	step    Step[T]
	depIdxs []int
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
		compiled[i].step = n.step
		if len(n.dependsOn) > 0 {
			compiled[i].depIdxs = make([]int, len(n.dependsOn))
			for j, dep := range n.dependsOn {
				compiled[i].depIdxs[j] = nodeIndices[dep]
			}
		}
	}
	return compiled, nil
}

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
		done := make([]chan struct{}, n)
		for i := range done {
			done[i] = make(chan struct{})
		}
		errs := make([]error, n)

		var wg sync.WaitGroup
		wg.Add(n)
		for i := range compiled {
			go func(idx int) {
				defer func() {
					close(done[idx])
					wg.Done()
				}()

				for _, depIdx := range compiled[idx].depIdxs {
					select {
					case <-ctx.Done():
						errs[idx] = ctx.Err()
						return
					case <-done[depIdx]:
						if errs[depIdx] != nil {
							return
						}
					}
				}

				if ctx.Err() != nil {
					errs[idx] = ctx.Err()
					return
				}

				if compiled[idx].step != nil {
					errs[idx] = compiled[idx].step(ctx)
				}
			}(i)
		}

		wg.Wait()
		return errors.Join(errs...)
	}
}

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
		done := make([]chan struct{}, n)
		for i := range done {
			done[i] = make(chan struct{})
		}
		errs := make([]error, n)

		sem := make(chan struct{}, limit)
		var wg sync.WaitGroup
		wg.Add(n)
		for i := range compiled {
			go func(idx int) {
				defer func() {
					close(done[idx])
					wg.Done()
				}()

				for _, depIdx := range compiled[idx].depIdxs {
					select {
					case <-ctx.Done():
						errs[idx] = ctx.Err()
						return
					case <-done[depIdx]:
						if errs[depIdx] != nil {
							return
						}
					}
				}

				if ctx.Err() != nil {
					errs[idx] = ctx.Err()
					return
				}

				if compiled[idx].step != nil {
					select {
					case <-ctx.Done():
						errs[idx] = ctx.Err()
						return
					case sem <- struct{}{}:
						defer func() { <-sem }()
						errs[idx] = compiled[idx].step(ctx)
					}
				}
			}(i)
		}

		wg.Wait()
		return errors.Join(errs...)
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

type DAGPlan[T context.Context] struct {
	nodes []*DAGNode[T]
}

func NewDAG[T context.Context](nodes ...*DAGNode[T]) *DAGPlan[T] {
	return &DAGPlan[T]{nodes: nodes}
}

func NewDAGEdges[T context.Context](connections ...DAGConnection[T]) (*DAGPlan[T], error) {
	nodes, err := buildNodesFromConnections(connections)
	if err != nil {
		return nil, err
	}
	return &DAGPlan[T]{nodes: nodes}, nil
}

func (p *DAGPlan[T]) Step() Step[T] {
	if p == nil {
		return func(ctx T) error { return nil }
	}
	return DAG(p.nodes...)
}

func (p *DAGPlan[T]) StepN(limit int) Step[T] {
	if p == nil {
		return func(ctx T) error { return nil }
	}
	return DAGN(limit, p.nodes...)
}

func (p *DAGPlan[T]) Validate() error {
	if p == nil || len(p.nodes) == 0 {
		return nil
	}
	return validateDAG(p.nodes)
}

func (p *DAGPlan[T]) ToMermaid() (string, error) {
	if p == nil {
		return "graph TD", nil
	}
	return DAGToMermaid(p.nodes...)
}

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
