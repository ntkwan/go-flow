// Package main provides a tool to inspect and scaffold missing GoDoc comments across the codebase.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	var (
		writeFlag = flag.Bool("w", false, "write result to (source) file instead of stdout")
		checkFlag = flag.Bool("check", false, "check for missing doc comments without modifying (exit 1 if missing)")
		dirFlag   = flag.String("dir", ".", "directory to scan")
	)
	flag.Parse()

	missingCount, err := processDirectory(*dirFlag, *writeFlag, *checkFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *checkFlag && missingCount > 0 {
		fmt.Fprintf(os.Stderr, "%d exported identifiers missing doc comments\n", missingCount)
		os.Exit(1)
	}

	if missingCount == 0 {
		fmt.Println("All exported identifiers have GoDoc comments.")
	} else if *writeFlag {
		fmt.Printf("Scaffolded %d missing GoDoc comments.\n", missingCount)
	}
}

func processDirectory(root string, write, check bool) (int, error) {
	totalMissing := 0

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip vendor, git, hidden directories, test fixtures
		if d.IsDir() {
			name := d.Name()
			if name != "." && (strings.HasPrefix(name, ".") || name == "vendor" || name == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		missing, err := processFile(path, write, check)
		if err != nil {
			return fmt.Errorf("processing %s: %w", path, err)
		}
		totalMissing += missing
		return nil
	})

	return totalMissing, err
}

type insertion struct {
	line int
	text string
}

func processFile(filePath string, write, check bool) (int, error) {
	src, err := os.ReadFile(filePath)
	if err != nil {
		return 0, err
	}

	fset := token.NewFileSet()
	fileNode, err := parser.ParseFile(fset, filePath, src, parser.ParseComments)
	if err != nil {
		return 0, err
	}

	missing := 0
	var insertions []insertion

	// Check package comment
	if fileNode.Name.Name != "main" && fileNode.Doc == nil {
		missing++
		pos := fset.Position(fileNode.Package)
		if check || !write {
			fmt.Printf("%s: package %s missing package doc comment\n", filePath, fileNode.Name.Name)
		}
		if write {
			insertions = append(insertions, insertion{
				line: pos.Line,
				text: fmt.Sprintf("// Package %s provides structured concurrency and workflow pipelines for %s.", fileNode.Name.Name, fileNode.Name.Name),
			})
		}
	}

	// Check declarations
	for _, decl := range fileNode.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Name.IsExported() && d.Doc == nil {
				missing++
				pos := fset.Position(d.Pos())
				if check || !write {
					fmt.Printf("%s:%d: exported function %s missing doc comment\n", filePath, pos.Line, d.Name.Name)
				}
				if write {
					var text string
					if d.Recv != nil && len(d.Recv.List) > 0 {
						text = fmt.Sprintf("// %s executes the %s operation.", d.Name.Name, d.Name.Name)
					} else {
						text = fmt.Sprintf("// %s performs the %s operation.", d.Name.Name, d.Name.Name)
					}
					insertions = append(insertions, insertion{
						line: pos.Line,
						text: text,
					})
				}
			}
		case *ast.GenDecl:
			isGroup := d.Lparen != token.NoPos
			if !isGroup && d.Doc != nil {
				continue
			}
			for _, spec := range d.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					if s.Name.IsExported() && s.Doc == nil && (isGroup || d.Doc == nil) {
						missing++
						pos := fset.Position(s.Pos())
						if !isGroup {
							pos = fset.Position(d.Pos())
						}
						if check || !write {
							fmt.Printf("%s:%d: exported type %s missing doc comment\n", filePath, pos.Line, s.Name.Name)
						}
						if write {
							insertions = append(insertions, insertion{
								line: pos.Line,
								text: fmt.Sprintf("// %s represents %s.", s.Name.Name, s.Name.Name),
							})
						}
					}
				case *ast.ValueSpec:
					for _, name := range s.Names {
						if name.IsExported() && s.Doc == nil && (isGroup || d.Doc == nil) {
							missing++
							pos := fset.Position(name.Pos())
							if !isGroup {
								pos = fset.Position(d.Pos())
							}
							if check || !write {
								fmt.Printf("%s:%d: exported identifier %s missing doc comment\n", filePath, pos.Line, name.Name)
							}
							if write {
								insertions = append(insertions, insertion{
									line: pos.Line,
									text: fmt.Sprintf("// %s defines %s.", name.Name, name.Name),
								})
								break
							}
						}
					}
				}
			}
		}
	}

	if write && len(insertions) > 0 {
		lines := strings.Split(string(src), "\n")
		// Sort insertions by line in descending order to insert from bottom to top
		sortInsertionsDescending(insertions)

		for _, ins := range insertions {
			targetIdx := ins.line - 1
			if targetIdx < 0 {
				targetIdx = 0
			}
			if targetIdx > len(lines) {
				targetIdx = len(lines)
			}
			lines = append(lines[:targetIdx], append([]string{ins.text}, lines[targetIdx:]...)...)
		}

		newSrc := strings.Join(lines, "\n")
		formatted, err := format.Source([]byte(newSrc))
		if err != nil {
			return missing, fmt.Errorf("formatting %s: %w", filePath, err)
		}
		if err := os.WriteFile(filePath, formatted, 0644); err != nil {
			return missing, err
		}
	}

	return missing, nil
}

func sortInsertionsDescending(items []insertion) {
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].line < items[j].line {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}
