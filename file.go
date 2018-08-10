package memdb

import (
	"os"

	"io"

	"github.com/pkg/errors"
	"github.com/tidwall/resp"
	"strconv"
)

var ErrOpenFile = errors.New("opening file")

type FileStorage struct {
	file *os.File
}

type Command int8

const (
	CommandSET Command = iota
	CommandDEL
)

type rowItem struct {
	dbItem
	command Command
}

func OpenFileStorage(path string) (*FileStorage, error) {
	var err error
	fs := &FileStorage{}

	fs.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return fs, nil
}

type ReadResult struct {
	item rowItem
	err  error
}

func (fs *FileStorage) Read() chan ReadResult {
	results := make(chan ReadResult)

	go func() {
		rd := resp.NewReader(fs.file)

		for {
			result := ReadResult{}
			v, _, err := rd.ReadValue()
			if err == io.EOF {
				break
			}
			if err != nil {
				result.err = err
				break
			}

			if v.Type() == resp.Array {
				for _, v := range v.Array() {
					for i, v := range v.Array() {
						switch i {
						case 0:
							command := v.String()
							if command == "set" {
								result.item.command = CommandSET
							} else if command == "del" {
								result.item.command = CommandDEL
							}
						case 1:
							result.item.key = Key(v.String())
						case 2:
							result.item.value = v.String()
						case 3:
							result.item.createdTx, err = strconv.ParseUint(v.String(), 10, 64)
							if err != nil {
								result.err = err
							}
						case 4:
							result.item.deletedTx, err = strconv.ParseUint(v.String(), 10, 64)
							if err != nil {
								result.err = err
							}
						}
					}
					results <- result
				}
			}
		}

		close(results)
	}()

	return results
}

func (fs *FileStorage) Write(items ...*dbItem) error {
	values := make([]resp.Value, 0)
	for _, item := range items {
		values = append(values, resp.MultiBulkValue("set", string(item.key), item.value, item.createdTx, item.deletedTx))
	}

	writer := resp.NewWriter(fs.file)

	return writer.WriteArray(values)
}

func (fs *FileStorage) Close() error {
	return fs.file.Close()
}
