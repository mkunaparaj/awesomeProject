package migrationfile

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

// File constants for directories, file prefixes and done marks.
const (
	DirUID  string = "uids/"
	DirDyn  string = "dyn/"
	DirSnow string = "snow/"

	PrefixUID  string = "user_ids"
	PrefixDyn  string = "dyn_con"
	PrefixSnow string = "snow_con"

	DoneProc string = "DONE"
)

// UserID struct for user_id files
type UserID struct {
	UserID int `json:"UserId"`
}

// DynamoContact struct for dynamo files
type DynamoContact struct {
	UserID int    `json:"UserId"`
	ListID string `json:"ListId"`
}

// SnowContact struct for snowflake files to be used by redis
type SnowContact struct {
	UserID    int
	ListID    string
	ContactID string
	UpdatedAt int64
}

// nolint
func init() {

	// create dirs for files
	// throwaway err incase dir already created
	os.Mkdir(DirUID, 0644)
	os.Mkdir(DirDyn, 0644)
	os.Mkdir(DirSnow, 0644)
}

// Read file into struct
func Read(fileName string, output interface{}) error {

	// read file contents
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	// unmarshal to struct
	if err := json.Unmarshal(b, &output); err != nil {
		return err
	}

	return nil
}

// Batch will batch and write lists to files in json
// Create directories
func Batch(size int, prefix string, list interface{}) ([]string, error) {

	// batch based on struct
	records := make(map[string][]interface{})
	var err error
	switch t := list.(type) {
	case []UserID:

		records, err = batchUserIDs(size, prefix, t)
		if err != nil {
			return nil, err
		}
	case []DynamoContact:

		records, err = batchDynamoContacts(size, prefix, t)
		if err != nil {
			return nil, err
		}
	case []SnowContact:

		records, err = batchSnowflakeContacts(size, prefix, t)
		if err != nil {
			return nil, err
		}
	}

	// range over each key value pair
	var fileNames []string
	for k, v := range records {

		// marshal to json
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		// write to file
		err = write(k, b)
		if err != nil {
			return nil, err
		}

		// add filename to list
		fileNames = append(fileNames, k)
	}

	return fileNames, nil
}

// DoneProcessing returns true if a files contents equal const doneProcessing
func DoneProcessing(fileName string) bool {

	// read file
	b, _ := ioutil.ReadFile(fileName)
	return strings.HasSuffix(string(b), DoneProc)
}

// MarkDone marks a file as done processing
func MarkDone(fileName string) error {

	// open file for appending
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// mark as done
	if _, err := f.Write([]byte("\nDONE\n")); err != nil {
		return err
	}

	return nil
}

// Load will read a dir and return found files
func Load(d string) ([]string, error) {

	files, err := ioutil.ReadDir(d)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0, len(files))
	for _, f := range files {
		fileNames = append(fileNames, fmt.Sprintf("%s%s", d, f.Name())) // return relative path
	}

	return fileNames, nil
}

func batchUserIDs(size int, prefix string, ids []UserID) (map[string][]interface{}, error) {

	// batch list
	rec := make(map[string][]interface{})
	var key string
	for i, id := range ids {

		// creates first file when i == 0
		if i%size == 0 {

			n, err := buildFileName(DirUID, prefix, i, time.Now().UnixNano())
			if err != nil {
				return nil, err
			}
			key = n
		}

		// append to file slice within map
		rec[key] = append(rec[key], id)
	}

	return rec, nil
}

func batchDynamoContacts(size int, prefix string, d []DynamoContact) (map[string][]interface{}, error) {

	// batch list
	rec := make(map[string][]interface{})
	var key string
	for i, con := range d {

		// creates first file when i == 0
		if i%size == 0 {

			n, err := buildFileName(DirDyn, prefix, i, time.Now().UnixNano())
			if err != nil {
				return nil, err
			}
			key = n
		}

		// append to file slice within map
		rec[key] = append(rec[key], con)
	}

	return rec, nil
}

func batchSnowflakeContacts(size int, prefix string, s []SnowContact) (map[string][]interface{}, error) {

	// batch list
	rec := make(map[string][]interface{})
	var key string
	for i, con := range s {

		// creates first file when i == 0
		if i%size == 0 {

			n, err := buildFileName(DirSnow, prefix, i, time.Now().UnixNano())
			if err != nil {
				return nil, err
			}
			key = n
		}

		// append to file slice within map
		rec[key] = append(rec[key], con)
	}

	return rec, nil
}

// write batch to files
func write(path string, b []byte) error {

	if err := ioutil.WriteFile(path, b, 0755); err != nil {
		return err
	}

	return nil
}

func buildFileName(dir, prefix string, i ...interface{}) (string, error) {

	var b strings.Builder

	if _, err := b.WriteString(dir); err != nil {
		return "", err
	}

	if _, err := b.WriteString(prefix); err != nil {
		return "", err
	}

	for range i {

		if _, err := b.WriteString(`_%d`); err != nil {
			return "", err
		}
	}

	if _, err := b.WriteString(`.json`); err != nil {
		return "", err
	}

	fileName := fmt.Sprintf(b.String(), i...)
	return fileName, nil
}
