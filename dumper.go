package mydump2bq

import (
	"io"
	"os/exec"
)

type Dumper struct {
	Host string
	Port int
	User string
	Password string
	Database string
	*TableMap
	Command string
	Options []string
}

func NewDumper(options []string, tm *TableMap) *Dumper {
	return &Dumper{Command: , Options: options}
}

func (d *Dumper) Dump(outputHandler func(r io.ReadCloser)) error {
	cmd := exec.Command("ls", "-al")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Start()
	go outputHandler(stdout)
	return cmd.Wait()
}
