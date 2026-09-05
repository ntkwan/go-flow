package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ntkwan/go-flow"
)

func buildShowcasePlan() (*flow.DAGPlan[context.Context], error) {
	validateNode := flow.Node("validate-order", func(ctx context.Context) error { return nil })
	userNode := flow.Node("fetch-user", func(ctx context.Context) error { return nil }).After("validate-order")
	inventoryNode := flow.Node("fetch-inventory", func(ctx context.Context) error { return nil }).After("validate-order")
	discountNode := flow.Node("calculate-discounts", func(ctx context.Context) error { return nil }).After("fetch-user")
	paymentNode := flow.Node("process-payment", func(ctx context.Context) error { return nil }).After("calculate-discounts", "fetch-inventory")
	updateInvNode := flow.Node("update-inventory", func(ctx context.Context) error { return nil }).After("process-payment")
	invoiceNode := flow.Node("generate-invoice", func(ctx context.Context) error { return nil }).After("process-payment")
	auditNode := flow.Node("audit-log", func(ctx context.Context) error { return nil }).After("process-payment")
	notifyNode := flow.Node("notify-customer", func(ctx context.Context) error { return nil }).After("generate-invoice")
	dispatchNode := flow.Node("dispatch-warehouse", func(ctx context.Context) error { return nil }).After("update-inventory")

	return flow.NewDAG(
		validateNode,
		userNode,
		inventoryNode,
		discountNode,
		paymentNode,
		updateInvNode,
		invoiceNode,
		auditNode,
		notifyNode,
		dispatchNode,
	), nil
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
