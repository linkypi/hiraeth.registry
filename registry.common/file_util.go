package common

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"google.golang.org/appengine/log"
	"hash/crc32"
	"io"
	"os"
)

func calcFileChecksum(filePath string) (uint32, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Errorf(context.Background(), "Error closing file: %v", err)
		}
	}(f)

	h := crc32.NewIEEE()
	_, err = io.Copy(h, f)
	if err != nil && err != io.EOF {
		return 0, err
	}

	return h.Sum32(), nil
}

func CreateFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf(context.Background(), "failed to close file %v", path)
		}
	}(file)
	return nil
}

func saveJsonFile(filePath string, obj any) error {
	bytes, _ := json.Marshal(obj)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf(context.Background(), "failed to close file %s, %v", filePath, err)
		}
	}(file)
	_, err = file.Write(bytes)
	if err != nil {
		return nil
	}
	return file.Sync()
}

func PersistToJsonFileWithCheckSum(filePath string, content any) error {
	bytes, _ := json.Marshal(content)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf(context.Background(), "failed to close file %s, %v", filePath, err)
		}
	}(file)

	// First, write the checksum
	checksum := crc32.ChecksumIEEE(bytes)
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, checksum)
	n, err := file.Write(buf)
	if err != nil {
		return err
	}

	// Then, write the actual content
	_, err = file.WriteAt(bytes, int64(n))
	if err != nil {
		return nil
	}
	return file.Sync()
}

func ReadJsonFileWithCheckSum(filePath string) (string, error) {

	info, err := os.Stat(filePath)
	if err != nil {
		return "", err
	}

	if !info.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	totalBytes := info.Size()
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Errorf(context.Background(), "failed to close file %s, %v", filePath, err)
		}
	}(file)

	intByteLen := 4
	// Read the checksum
	buf := make([]byte, intByteLen)
	_, err = file.Read(buf)
	if err != nil {
		return "", err
	}
	checksum := binary.BigEndian.Uint32(buf)

	// Read the actual content
	content := make([]byte, totalBytes-int64(intByteLen))
	_, err = file.ReadAt(content, int64(intByteLen))
	if err != nil {
		return "", err
	}

	actualChecksum := crc32.ChecksumIEEE(content)
	if actualChecksum != checksum {
		return "", errors.New("checksum mismatch")
	}

	return string(content), nil
}
