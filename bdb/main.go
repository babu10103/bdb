package bdb

import (
	"encoding/json"
	"fmt"

	"os"
	"path/filepath"
	"sync"

	"github.com/babu10103/bdb/util"
	"github.com/jcelliott/lumber"
)

type (
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Trace(string, ...interface{})
		Debug(string, ...interface{})
	}
)

type Options struct {
	Logger
}

// New creates a new database driver.
//
// Parameters:
// - dir: The directory where the database is stored.
// - options: Additional options for the database (optional).
//
// Returns:
// - *Driver: The newly created database driver.
// - error: An error if the database cannot be created.
func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}

	if options != nil {
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

// getOrCreateMutex returns a mutex for the specified collection.
//
// The mutex is used to ensure that only one goroutine at a time
// writes to a collection.
//
// Parameters:
// - collection: The name of the collection.
//
// Returns:
// - *sync.Mutex: The mutex for the collection.
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	// Lock the mutex to ensure that only one goroutine at a
	// time can access the map.
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Attempt to get the mutex for the collection from the map.
	m, ok := d.mutexes[collection]

	// If the mutex does not exist, create a new mutex and add
	// it to the map.
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m

}

// Write writes the data to the database.
//
// Parameters:
// - collection: The name of the collection to write to.
// - resource: The name of the resource to write.
// - v: The data to write.
//
// Returns:
// - error: An error if the write operation fails.
func (d *Driver) Write(collection string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save records")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := util.ToMap(v)

	id := util.GenerateObjectId()
	data["_id"] = id

	if err != nil {
		return err
	}

	bytes, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}

	bytes = append(bytes, byte('\n'))

	tempPath := filepath.Join(dir, id+".json.tmp")
	if err := os.WriteFile(tempPath, bytes, 0644); err != nil {
		return err
	}
	finalPath := filepath.Join(dir, id+".json")
	if err := os.Rename(tempPath, finalPath); err != nil {
		return err
	}

	return nil
}

// Read retrieves a record from the database.
//
// Parameters:
// - collection: The name of the collection to read from.
// - resource: The name of the resource to read.
// - v: The variable to unmarshal the record into.
//
// Returns:
// - error: An error if the read operation fails.
func (d *Driver) Read(collection, resource string, v interface{}) error {
	d.log.Debug("Reading record: %s from collection: %s", resource, collection)

	if collection == "" {
		return fmt.Errorf("missing collection - unable to read!")
	}

	if resource == "" {
		return fmt.Errorf("missing resource - unable to read record (no name)!")
	}

	resourcePath := filepath.Join(d.dir, collection, resource+".json")

	d.log.Debug("Reading record: %s from path: %s", resource, resourcePath)

	if _, err := util.Stat(resourcePath); err != nil {
		return fmt.Errorf("unable to find resource: %s (%s)", resourcePath, err)
	}

	bytes, err := os.ReadFile(resourcePath)
	if err != nil {
		return fmt.Errorf("error reading file: %s (%s)", resourcePath, err)
	}

	d.log.Debug("Read bytes from file: %s", string(bytes))

	if err := json.Unmarshal(bytes, &v); err != nil {
		return fmt.Errorf("error unmarshalling json: %s", err)
	}

	d.log.Debug("Unmarshalled record: %+v", v)

	return nil
}

// ReadAll retrieves all the records from the specified collection.
//
// Parameters:
// - collection: The name of the collection.
//
// Returns:
// - []string: The list of records.
// - error: An error if the operation fails.
func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection")
	}

	collectionPath := filepath.Join(d.dir, collection)

	if _, err := util.Stat(collectionPath); err != nil {
		return nil, fmt.Errorf("unable to find collection: %s (%s)", collectionPath, err)
	}

	entries, err := os.ReadDir(collectionPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read directory: %s (%s)", collectionPath, err)
	}

	var records []string

	for _, file := range entries {
		path := filepath.Join(collectionPath, file.Name())

		bytes, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("error reading file: %s (%s)", path, err)
		}

		records = append(records, string(bytes))
	}

	return records, err
}

// Delete removes a record from the database.
//
// Parameters:
// - collection: The name of the collection.
// - resource: The name of the resource to delete.
//
// Returns:
// - error: An error if the delete operation fails.
func (d *Driver) Delete(collection, resource string) error {

	if collection == "" {
		return fmt.Errorf("missing collection")
	}

	if resource == "" {
		return fmt.Errorf("missing resource")
	}

	mutex := d.getOrCreateMutex(collection)

	mutex.Lock()
	defer mutex.Unlock()

	resourcePath := filepath.Join(d.dir, collection, resource)

	if _, err := util.Stat(resourcePath); err != nil {
		return fmt.Errorf("unable to find resource: %s (%s)", resourcePath, err)
	}

	switch fi, err := util.Stat(resourcePath); {

	case fi == nil, err != nil:
		return fmt.Errorf("unable to find resource: %s (%s)", resourcePath, err)

	case fi.Mode().IsDir():
		return os.RemoveAll(resource)

	case fi.Mode().IsRegular():
		return os.RemoveAll(resource + ".json")

	}

	return nil

}

// Update updates a record in the database.

// Update updates a record in the database.
//
// Parameters:
// - collection: The name of the collection to update.
// - resource: The name of the resource to update.
// - v: The data to update.
//
// Returns:
// - error: An error if the update operation fails.
func (d *Driver) Update(collection, resource string, v interface{}) error {
	if collection == "" {
		d.log.Debug("Collection is empty")
		return fmt.Errorf("missing collection")
	}

	if resource == "" {
		d.log.Debug("Resource is empty")
		return fmt.Errorf("missing resource")
	}

	mutex := d.getOrCreateMutex(collection)

	mutex.Lock()
	defer mutex.Unlock()

	resourcePath := filepath.Join(d.dir, collection, resource+".json")

	if _, err := util.Stat(resourcePath); err != nil {
		d.log.Debug("Resource does not exist at %s (%s)", resourcePath, err)
		return fmt.Errorf("unable to find resource: %s (%s)", resourcePath, err)
	}

	bytes, err := os.ReadFile(resourcePath)
	if err != nil {
		d.log.Debug("Error reading file: %s (%s)", resourcePath, err)
		return fmt.Errorf("error reading file: %s (%s)", resourcePath, err)
	}

	var existing map[string]interface{}
	if err := json.Unmarshal(bytes, &existing); err != nil {
		d.log.Debug("Error unmarshalling json: %s", err)
		return fmt.Errorf("error unmarshalling json: %s", err)
	}

	newData, err := util.ToMap(v)
	if err != nil {
		d.log.Debug("Error converting data to map: %s", err)
		return fmt.Errorf("error converting data to map: %s", err)
	}

	util.UpdateMap(newData, existing)

	if err := os.Remove(resourcePath); err != nil {
		d.log.Debug("Error removing file: %s (%s)", resourcePath, err)
		return fmt.Errorf("error removing file: %s (%s)", resourcePath, err)
	}

	bytes, err = json.MarshalIndent(existing, "", "\t")
	if err != nil {
		d.log.Debug("Error marshalling json: %s", err)
		return fmt.Errorf("error marshalling json: %s", err)
	}

	if err := os.WriteFile(resourcePath, bytes, 0644); err != nil {
		d.log.Debug("Error writing to file: %s (%s)", resourcePath, err)
		return err
	}

	return nil
}
