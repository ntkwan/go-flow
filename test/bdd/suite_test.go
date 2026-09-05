package bdd_test

import (
	"testing"

	"github.com/cucumber/godog"
)

func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: func(s *godog.ScenarioContext) {
			registerPatternsSteps(s)
			registerCompositionSteps(s)
			registerCompositeSteps(s)
			registerResilienceSteps(s)
		},
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../../features"},
			TestingT: t,
			Strict:   true,
		},
	}

	if suite.Run() != 0 {
		t.Fatal("godog test suite failed")
	}
}
