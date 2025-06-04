package internal

import (
	"bufio"
	"os"
)

const (
	expectedReportFile = "app/expected_report.txt"
)

func CompareWithExpectedReport(toCompare string) {
	logger.Info("Starting comparison with expected report...")
	compareFiles(expectedReportFile, toCompare)
}

func compareFiles(expectedFile string, actualFile string) {

	expected, err := os.Open(expectedFile)
	if err != nil {
		logger.Fatalf("Error opening file %s: %v", expectedFile, err)
	}
	defer expected.Close()

	actual, err := os.Open(actualFile)
	if err != nil {
		logger.Fatalf("Error opening file %s: %v", actualFile, err)
	}
	defer actual.Close()

	differences := compareFileByLine(expected, actual)

	if differences == 0 {
		logger.Info("Success! The reports match exactly.")
	} else {
		logger.Warningf("Found %d differences between the files", differences)
	}
}

func compareFileByLine(expected, actual *os.File) int {
	scExpected := bufio.NewScanner(expected)
	scActual := bufio.NewScanner(actual)
	lineNum := 1
	differences := 0

	for {
		hasExpected := scExpected.Scan()
		hasActual := scActual.Scan()

		expectedLine := ""
		actualLine := ""

		if hasExpected {
			expectedLine = scExpected.Text()
		}
		if hasActual {
			actualLine = scActual.Text()
		}

		// If both files are exhausted, we're done
		if !hasExpected && !hasActual {
			break
		}

		// Check for differences
		if expectedLine != actualLine {
			logDifference(lineNum, expectedLine, actualLine)
			differences++
		}

		lineNum++

		// If one file is longer than the other, keep comparing the remaining lines
		if (!hasExpected || !hasActual) && (hasExpected || hasActual) {
			for scExpected.Scan() {
				logDifference(lineNum, scExpected.Text(), "")
				lineNum++
				differences++
			}
			for scActual.Scan() {
				logDifference(lineNum, "", scActual.Text())
				lineNum++
				differences++
			}
			break
		}
	}

	return differences
}

func logDifference(lineNum int, expected, actual string) {
	if expected == "" {
		logger.Warningf("Line %d: Extra line in actual: %s", lineNum, actual)
	} else if actual == "" {
		logger.Warningf("Line %d: Missing line (expected): %s", lineNum, expected)
	} else {
		logger.Warningf("Line %d:\n\tExpected: %s\n\tActual:   %s", lineNum, expected, actual)
	}
}
