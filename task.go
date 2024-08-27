package grsync

import (
	"bufio"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var (
	progressMatcher *matcher
	speedMatcher    *matcher
	fileMatcher     *regexp.Regexp
)

// Task is high-level API under rsync
type Task struct {
	rsync *Rsync

	state *State
	log   *Log

	stdout io.Writer
	stderr io.Writer
}

func (t *Task) SetStdout(stdout io.Writer) {
	t.stdout = stdout
}

func (t *Task) SetStderr(stderr io.Writer) {
	t.stderr = stderr
}

// State contains information about rsync process
type State struct {
	Remain       int     `json:"remain"`
	Total        int     `json:"total"`
	Speed        string  `json:"speed"`
	Progress     float64 `json:"progress"`
	CopiedObject string  `json:"copied object"`
}

// Log contains raw stderr and stdout outputs
type Log struct {
	Stderr string `json:"stderr"`
	Stdout string `json:"stdout"`
}

// State returns information about rsync processing task
func (t Task) State() State {
	return *t.state
}

// Log return structure which contains raw stderr and stdout outputs
func (t Task) Log() Log {
	return Log{
		Stderr: t.log.Stderr,
		Stdout: t.log.Stdout,
	}
}

// Run starts rsync process with options
func (t *Task) Run() (err error) {
	var stderr, stdout io.ReadCloser
	if stderr, err = t.rsync.StderrPipe(); err != nil {
		return err
	}
	defer func() {
		_ = stderr.Close()
	}()

	if stdout, err = t.rsync.StdoutPipe(); err != nil {
		return err
	}
	defer func() {
		_ = stdout.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		processStdout(t, stdout)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		processStderr(t, stderr)
		wg.Done()
	}()

	err = t.rsync.Run()
	wg.Wait()

	return err
}

// NewTask returns new rsync task
func NewTask(source, destination string, rsyncOptions RsyncOptions) *Task {
	// Force set required options
	rsyncOptions.HumanReadable = true
	rsyncOptions.Partial = true
	rsyncOptions.Progress = true
	rsyncOptions.Archive = true

	return &Task{
		rsync:  NewRsync(source, destination, rsyncOptions),
		state:  &State{},
		log:    &Log{},
		stdout: io.Discard,
		stderr: io.Discard,
	}
}

func NewTaskWithoutForceOptions(source, destination string, rsyncOptions RsyncOptions) *Task {
	return &Task{
		rsync:  NewRsync(source, destination, rsyncOptions),
		state:  &State{},
		log:    &Log{},
		stdout: io.Discard,
		stderr: io.Discard,
	}
}

func processStdout(task *Task, stdout io.Reader) {
	const maxPercents = float64(100)
	const minDivider = 1

	// Extract data from strings:
	//         999,999 99%  999.99kB/s    0:00:59 (xfr#9, to-chk=999/9999)
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		logStr := scanner.Text()

		_, _ = task.stdout.Write(scanner.Bytes())
		if progressMatcher.Match(logStr) {
			task.state.Remain, task.state.Total = getTaskProgress(progressMatcher.Extract(logStr))

			copiedCount := float64(task.state.Total - task.state.Remain)
			task.state.Progress = copiedCount / math.Max(float64(task.state.Total), float64(minDivider)) * maxPercents
		}

		if speedMatcher.Match(logStr) {
			task.state.Speed = getTaskSpeed(speedMatcher.ExtractAllStringSubmatch(logStr, 2))
		}

		if fileMatcher.MatchString(logStr) {
			task.state.CopiedObject = fileMatcher.FindString(logStr)
		}

		task.log.Stdout += logStr + "\n"
	}
}

func processStderr(task *Task, stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		task.log.Stderr += scanner.Text() + "\n"
		_, _ = task.stderr.Write(scanner.Bytes())
	}
}

func getTaskProgress(remTotalString string) (int, int) {
	const remTotalSeparator = "/"
	const numbersCount = 2
	const (
		indexRem = iota
		indexTotal
	)

	info := strings.Split(remTotalString, remTotalSeparator)
	if len(info) < numbersCount {
		return 0, 0
	}

	remain, _ := strconv.Atoi(info[indexRem])
	total, _ := strconv.Atoi(info[indexTotal])

	return remain, total
}

func getTaskSpeed(data [][]string) string {
	if len(data) < 2 || len(data[1]) < 2 {
		return ""
	}

	return data[1][1]
}

func init() {
	progressMatcher = newMatcher(`\(.+-chk=(\d+.\d+)`)
	speedMatcher = newMatcher(`(\d+\.\d+.{2}\/s)`)
	fileMatcher = regexp.MustCompile(`^(\S+.*\S+)$`)
}
