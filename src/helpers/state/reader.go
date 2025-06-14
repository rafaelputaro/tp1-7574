package state

import (
	"io"
	"os"
)

const BUFFER_SIZE = 1024 // in bytes

// Custom file reader.
type Reader struct {
	buffer    []byte
	bufferLen int
	fileDesc  *os.File // File descriptor for reading
}

func NewReader(fileDesc *os.File) *Reader {
	return &Reader{
		buffer:    make([]byte, BUFFER_SIZE),
		bufferLen: 0,
		fileDesc:  fileDesc,
	}
}

// Returns a line from the file. When the file ends it returns io.Eof as an error.
func (reader *Reader) ReadLine() (string, error) {
	var errToReturn error = nil
	toReturn := ""
	foundEndLn := false
	foundEof := false
	// concatenate until reading eof
	for !foundEndLn && !foundEof {
		bytes, err := reader.fileDesc.Read(reader.buffer[reader.bufferLen:])
		reader.bufferLen = reader.bufferLen + bytes
		if err != nil {
			errToReturn = err
			if err != io.EOF {
				break
			} else {
				foundEof = true
			}
		}
		// count until first \n
		count := 0
		for index := range reader.bufferLen {
			count++
			// check /n
			if reader.buffer[index] == '\n' {
				foundEndLn = true
				break
			}
		}
		// append until first \n
		if count > 0 {
			if foundEndLn || foundEof {
				toReturn = toReturn + string(reader.buffer[:count-1])
			} else {
				toReturn = toReturn + string(reader.buffer[:count])
			}
		}
		// update buffer len
		reader.bufferLen -= count
		copy(reader.buffer, reader.buffer[count:])
	}
	return toReturn, errToReturn
}
