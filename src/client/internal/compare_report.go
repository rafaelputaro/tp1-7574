package internal

import (
	"bufio"
	"os"
	"time"

	"github.com/op/go-logging"
)

const MSG_ERROR_READING_FILE = "Error reading file: %v"
const MSG_ERROR_READING_DIR = "Error reading directory %s: %v"
const MSG_NO_REPORTS_FOUND = "No report files found in the current directory."
const MSG_COMPARE_REPORT = "Comparing report: %s with %s"
const MSG_COMPARING_LINES = "[x]Comparing lines: \n	Expected: %s \n	Found: %s"
const MODULE_TO_LOG = "client"

type FileWithDate struct {
	FileInfo os.FileInfo
	ModTime  time.Time
}

// Compares the latest report file in the specified directory with a provided file.
func CompareReport(directory string, toCompare string) {
	logger := logging.MustGetLogger(MODULE_TO_LOG)
	// Get the latest report file
	latestReport, err := getOldestFile(directory)
	if err != nil {
		logger.Errorf(MSG_ERROR_READING_DIR, directory, err)
		return
	}
	if latestReport == "" {
		logger.Warningf(MSG_NO_REPORTS_FOUND)
		return
	}
	logger.Infof(MSG_COMPARE_REPORT, latestReport, toCompare)
	// Compare the latest report with the provided file
	compareFiles(latestReport, toCompare)
}

// compareFiles compares two files line by line and logs the differences.
func compareFiles(patternFile string, toCompare string) {
	logger := logging.MustGetLogger(MODULE_TO_LOG)
	pattern, err := os.Open(patternFile)
	if err != nil {
		logger.Errorf(MSG_ERROR_READING_FILE, err)
	}
	defer pattern.Close()
	toCampare, err := os.Open(toCompare)
	if err != nil {
		logger.Errorf(MSG_ERROR_READING_FILE, err)
	}
	defer toCampare.Close()
	compareFileByLine(pattern, toCampare)
}

// Compare two files line by line
func compareFileByLine(patternFile *os.File, toCompare *os.File) {
	sc1 := bufio.NewScanner(patternFile)
	sc2 := bufio.NewScanner(toCompare)
	for {
		sc1Bool := sc1.Scan()
		sc2Bool := sc2.Scan()
		if !sc1Bool && !sc2Bool {
			break
		}
		compareLines(sc1.Text(), sc2.Text())
	}
}

// Compare two lines and log if they are equal or not
func compareLines(line1 string, line2 string) {
	logger := logging.MustGetLogger(MODULE_TO_LOG)
	if line1 != line2 {
		logger.Debugf(MSG_COMPARING_LINES, line2, line1)
	}
}

// Returns the oldest file in a directory based on modification time.
func getOldestFile(dirPath string) (string, error) {
	logger := logging.MustGetLogger(MODULE_TO_LOG)
	// Get the list of files in the directory
	files, err := os.ReadDir(dirPath)
	if err != nil {
		logger.Errorf(MSG_ERROR_READING_DIR, dirPath, err)
		return "", nil
	}
	var oldestFile os.DirEntry
	var oldestTime time.Time
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			continue
		}
		if oldestFile == nil { // First file found
			oldestFile = file
			oldestTime = info.ModTime()
		} else if info.ModTime().Before(oldestTime) { // Check if the current file is older
			oldestFile = file
			oldestTime = info.ModTime()
		}
	}
	return oldestFile.Name(), nil
}
