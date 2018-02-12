package mydump2bq

import (
	"fmt"
	"io"
	"os/exec"
)

type Dumper struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Table    string
	*TableMap
	Command string
	Options []string
	DoneCh  chan bool
}

func NewDumper(tm *TableMap, myconf MySQLConfig, mydconf MyDump2BQConfig) *Dumper {
	dumper := &Dumper{
		Host:     myconf.Host,
		Port:     myconf.Port,
		User:     myconf.User,
		Password: myconf.Password,
		Database: tm.Config.MySQL.Database,
		Table:    tm.Config.MySQL.Table,
		Command:  mydconf.Command,
	}
	opt := make([]string, 13)
	opt = append(opt, fmt.Sprintf("--host=%s", dumper.Host))
	opt = append(opt, fmt.Sprintf("--port=%v", dumper.Port))
	opt = append(opt, fmt.Sprintf("--user=%s", dumper.User))
	opt = append(opt, fmt.Sprintf("--password=%s", dumper.Password))
	opt = append(opt, fmt.Sprintf("--database=%s", dumper.Database))
	opt = append(opt, fmt.Sprintf("--tables=%s", dumper.Table))
	opt = append(opt, "--single-transaction")
	opt = append(opt, "--skip-lock-tables")
	opt = append(opt, "--compact")
	opt = append(opt, "--skip-opt")
	opt = append(opt, "--quick")
	opt = append(opt, "--no-create-info")
	opt = append(opt, "--skip-extended-insert")
	dumper.Options = opt

	return dumper
}

func (d *Dumper) Dump(outputHandler func(r io.Reader)) error {
	cmd := exec.Command(d.Command, d.Options...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	go func() {
		cmd.Start()
		outputHandler(stdout)
		cmd.Wait()
		d.DoneCh <- true
	}()
	return nil
}
