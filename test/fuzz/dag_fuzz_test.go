package fuzz_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ntkwan/go-flow"
)

func FuzzDAG(f *testing.F) {
	f.Add([]byte("A:B,C\nB:C\nC:\n"))
	f.Add([]byte("A:A\n"))
	f.Add([]byte("A:B\nB:A\n"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		lines := bytes.Split(data, []byte("\n"))
		if len(lines) > 20 {
			lines = lines[:20]
		}

		var nodes []*flow.DAGNode[context.Context]
		for i, line := range lines {
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			parts := bytes.SplitN(line, []byte(":"), 2)
			name := string(bytes.TrimSpace(parts[0]))
			if name == "" {
				name = fmt.Sprintf("node_%d", i)
			}

			var deps []string
			if len(parts) == 2 && len(parts[1]) > 0 {
				depParts := bytes.Split(parts[1], []byte(","))
				for _, dep := range depParts {
					depName := string(bytes.TrimSpace(dep))
					if depName != "" {
						deps = append(deps, depName)
					}
				}
			}

			step := func(ctx context.Context) error {
				return nil
			}
			nodes = append(nodes, flow.Node(name, step).After(deps...))
		}

		dagStep := flow.DAG(nodes...)
		_ = dagStep(context.Background())

		dagNStep := flow.DAGN(2, nodes...)
		_ = dagNStep(context.Background())

		_, _ = flow.DAGToMermaid(nodes...)
		_, _ = flow.DAGToDOT(nodes...)
	})
}
