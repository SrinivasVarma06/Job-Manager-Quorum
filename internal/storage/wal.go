package storage

import (
	"encoding/json"
	"os"
	"io"
	"bufio"
	"quorum/internal/job"
)

type WAL struct {
	file *os.File
}

func NewWal(path string) (*WAL,error){
	file,err:=os.OpenFile(path,os.O_CREATE|os.O_APPEND|os.O_RDWR,0644)
	if err!=nil{
		return nil,err
	}
	return &WAL{
		file:file,
	},nil
}

func (w *WAL)Append(j job.Job) error{
	data,err:=json.Marshal(j)
	if err!=nil{
		return err
	}
	data=append(data,'\n')
	_,err=w.file.Write(data)
	return err
}

func (w *WAL) Replay() ([]job.Job, error) {
	var jobs []job.Job

	_, err := w.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(w.file)
	for scanner.Scan() {
		var j job.Job
		err := json.Unmarshal(scanner.Bytes(), &j)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (w *WAL) Reset() error {

	if err := w.file.Close(); err != nil {
		return err
	}

	file, err := os.OpenFile("jobs.log",os.O_CREATE|os.O_TRUNC|os.O_RDWR,0644)

	if err != nil {
		return err
	}

	w.file = file

	return nil
}