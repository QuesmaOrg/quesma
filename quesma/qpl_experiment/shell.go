package qpl_experiment

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
)

type ShellTVF struct {
	cmd string
}

func (s ShellTVF) Fn(t Table) (Table, error) {

	csvInput, err := WriteTableToString(t)
	if err != nil {
		return Table{}, fmt.Errorf("error converting table to CSV: %w", err)
	}

	args := []string{"-c", s.cmd}

	log.Println("Executing command: ", args)
	// Create the command
	cmd := exec.Command("/bin/sh", args...)

	// Pass CSV data to the command's stdin
	cmd.Stdin = bytes.NewBufferString(csvInput)

	// Capture the command's stdout
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	// Run the command
	if err := cmd.Run(); err != nil {
		log.Println("Error running command: ", err)
		return Table{}, fmt.Errorf("error running command: %w", err)
	}

	// Read the resulting CSV from the command's stdout
	outputTable, err := ReadTableFromString(stdout.String())
	if err != nil {
		return Table{}, fmt.Errorf("error reading output CSV: %w", err)
	}

	return outputTable, nil

}
