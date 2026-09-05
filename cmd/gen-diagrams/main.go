package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ntkwan/go-flow"
)

func buildShowcasePlan() (*flow.DAGPlan[context.Context], error) {
	fetchUser := flow.Node("fetch-user", func(ctx context.Context) error { return nil })
	fetchCart := flow.Node("fetch-cart", func(ctx context.Context) error { return nil })
	processPayment := flow.Node("process-payment", func(ctx context.Context) error { return nil }).After("fetch-user", "fetch-cart")
	sendReceipt := flow.Node("send-receipt", func(ctx context.Context) error { return nil }).After("process-payment")

	return flow.NewDAG(fetchUser, fetchCart, processPayment, sendReceipt), nil
}

func syncMermaidDiagram(readmePath, startTag, endTag, mermaid string, checkOnly bool) error {
	data, err := os.ReadFile(readmePath)
	if err != nil {
		return err
	}

	content := string(data)
	startIdx := strings.Index(content, startTag)
	if startIdx == -1 {
		return fmt.Errorf("start tag %q not found in %s", startTag, readmePath)
	}

	endIdx := strings.Index(content, endTag)
	if endIdx == -1 {
		return fmt.Errorf("end tag %q not found in %s", endTag, readmePath)
	}

	if endIdx < startIdx {
		return fmt.Errorf("invalid tag order in %s", readmePath)
	}

	var sb strings.Builder
	sb.WriteString(content[:startIdx+len(startTag)])
	sb.WriteString("\n```mermaid\n")
	sb.WriteString(mermaid)
	sb.WriteString("\n```\n")
	sb.WriteString(content[endIdx:])

	newContent := sb.String()
	if checkOnly {
		if content != newContent {
			return fmt.Errorf("diagrams in %s are out of date; run 'make gen'", readmePath)
		}
		return nil
	}

	if content == newContent {
		return nil
	}

	return os.WriteFile(readmePath, []byte(newContent), 0644)
}

func main() {
	checkOnly := len(os.Args) > 1 && os.Args[1] == "--check"

	plan, err := buildShowcasePlan()
	if err != nil {
		panic(err)
	}

	mermaid, err := plan.ToMermaid()
	if err != nil {
		panic(err)
	}

	readmePath := "README.md"
	startTag := "<!-- AUTO-GENERATED-DAG:START -->"
	endTag := "<!-- AUTO-GENERATED-DAG:END -->"

	if err := syncMermaidDiagram(readmePath, startTag, endTag, mermaid, checkOnly); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if checkOnly {
		fmt.Println("Diagrams check passed: README.md is in sync")
	} else {
		fmt.Println("Successfully generated and synced Mermaid diagram into README.md")
	}
}
