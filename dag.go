package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
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

type dagRuntimeNode[T context.Context] struct {
	node *DAGNode[T]
	done chan struct{}
	err  error
}

func validateDAG[T context.Context](nodes []*DAGNode[T]) error {
	nodeMap := make(map[string]*DAGNode[T], len(nodes))
	for _, n := range nodes {
		if n == nil {
			return errors.New("nil node in DAG")
		}
		if n.name == "" {
			return errors.New("empty node name in DAG")
		}
		if _, exists := nodeMap[n.name]; exists {
			return fmt.Errorf("duplicate node name: %s", n.name)
		}
		nodeMap[n.name] = n
	}

	inDegree := make(map[string]int, len(nodes))
	graph := make(map[string][]string, len(nodes))

	for _, n := range nodes {
		inDegree[n.name] = len(n.dependsOn)
		for _, dep := range n.dependsOn {
			if dep == n.name {
				return fmt.Errorf("node %s cannot depend on itself", n.name)
			}
			if _, exists := nodeMap[dep]; !exists {
				return fmt.Errorf("unknown dependency %q for node %q", dep, n.name)
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
		return errors.New("cycle detected in DAG")
	}

	return nil
}

func DAG[T context.Context](nodes ...*DAGNode[T]) Step[T] {
	return func(ctx T) error {
		if len(nodes) == 0 {
			return nil
		}

		if err := validateDAG(nodes); err != nil {
			return err
		}

		runtimeMap := make(map[string]*dagRuntimeNode[T], len(nodes))
		for _, n := range nodes {
			runtimeMap[n.name] = &dagRuntimeNode[T]{
				node: n,
				done: make(chan struct{}),
			}
		}

		var wg sync.WaitGroup
		wg.Add(len(nodes))
		for _, rn := range runtimeMap {
			go func(r *dagRuntimeNode[T]) {
				defer func() {
					close(r.done)
					wg.Done()
				}()

				for _, depName := range r.node.dependsOn {
					dep := runtimeMap[depName]
					select {
					case <-ctx.Done():
						r.err = ctx.Err()
						return
					case <-dep.done:
						if dep.err != nil {
							return
						}
					}
				}

				if ctx.Err() != nil {
					r.err = ctx.Err()
					return
				}

				if r.node.step != nil {
					r.err = r.node.step(ctx)
				}
			}(rn)
		}

		wg.Wait()

		var errs []error
		for _, n := range nodes {
			if rn := runtimeMap[n.name]; rn.err != nil {
				errs = append(errs, rn.err)
			}
		}

		return errors.Join(errs...)
	}
}

func DAGN[T context.Context](limit int, nodes ...*DAGNode[T]) Step[T] {
	if limit <= 0 || limit >= len(nodes) {
		return DAG(nodes...)
	}
	return func(ctx T) error {
		if err := validateDAG(nodes); err != nil {
			return err
		}

		runtimeMap := make(map[string]*dagRuntimeNode[T], len(nodes))
		for _, n := range nodes {
			runtimeMap[n.name] = &dagRuntimeNode[T]{
				node: n,
				done: make(chan struct{}),
			}
		}

		sem := make(chan struct{}, limit)
		var wg sync.WaitGroup
		wg.Add(len(nodes))
		for _, rn := range runtimeMap {
			go func(r *dagRuntimeNode[T]) {
				defer func() {
					close(r.done)
					wg.Done()
				}()

				for _, depName := range r.node.dependsOn {
					dep := runtimeMap[depName]
					select {
					case <-ctx.Done():
						r.err = ctx.Err()
						return
					case <-dep.done:
						if dep.err != nil {
							return
						}
					}
				}

				if ctx.Err() != nil {
					r.err = ctx.Err()
					return
				}

				if r.node.step != nil {
					select {
					case <-ctx.Done():
						r.err = ctx.Err()
						return
					case sem <- struct{}{}:
						defer func() { <-sem }()
						r.err = r.node.step(ctx)
					}
				}
			}(rn)
		}

		wg.Wait()

		var errs []error
		for _, n := range nodes {
			if rn := runtimeMap[n.name]; rn.err != nil {
				errs = append(errs, rn.err)
			}
		}

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
