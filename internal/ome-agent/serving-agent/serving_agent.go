package serving_agent

import (
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"sigs.k8s.io/ome/pkg/logging"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
	"sigs.k8s.io/ome/pkg/zipper"
)

const (
	BigFileSizeInMB              = 200
	DefaultDownloadChunkSizeInMB = 20
	DefaultDownloadThreads       = 12

	maxRetries = 3
	retryDelay = 5 * time.Second
)

type ServingSidecar struct {
	logger logging.Interface
	Config Config
}

// NewServingSidecar constructs a new replica agent from the given configuration.
func NewServingSidecar(config *Config) (*ServingSidecar, error) {
	return &ServingSidecar{
		logger: config.AnotherLogger,
		Config: *config,
	}, nil
}

func (s *ServingSidecar) Start() error {
	s.logger.Info("Starting Serving Sidecar")

	// Initialize the finetuned model directory when app starts
	s.applyFinetunedModelChanges()

	// Create file change watcher and the channel to watch file changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	// ConfigMap never update the file. It creates as a tmp file and delete the old file, and then being renamed.
	// Use the mount dir to work around
	fileChangeDetected := s.watchFileChanges(watcher, filepath.Dir(s.Config.FineTunedWeightInfoFilePath))

	// Create the channel to receive termination signals
	terminationSignalCh := make(chan os.Signal, 1)
	signal.Notify(terminationSignalCh, os.Interrupt, syscall.SIGTERM)

OuterLoop:
	for {
		select {
		case <-terminationSignalCh:
			close(fileChangeDetected)
			s.logger.Infof("Termination signal received, exiting serving sidecar ...")
			break OuterLoop
		case <-fileChangeDetected:
			// apply the finetuned models changes
			s.applyFinetunedModelChanges()
		}
	}

	return nil
}

// applyFinetunedModelChanges makes the model directories match the fine-tuned
// weight list: it downloads and unpacks the models the list names that are not
// installed yet, and removes the ones the list no longer names. A failure to
// read the list or to install a model stops the sidecar.
func (s *ServingSidecar) applyFinetunedModelChanges() {
	objectURIs, ftModelNames, err := readObjectURIsFromFile(s.Config.FineTunedWeightInfoFilePath)
	if err != nil {
		s.logger.Infof("Error reading %s: %s", s.Config.FineTunedWeightInfoFilePath, err)
		panic(err)
	}

	existingNames, err := getExistingFtModelNamesFromDir(s.Config.ZippedFineTunedWeightDirectory)
	if err != nil {
		panic(err)
	}
	modelsToAdd, modelsToDelete := findModelNameDifferences(ftModelNames, existingNames)

	for _, uri := range objectURIs {
		if !modelsToAdd[uri.ObjectName] {
			continue
		}
		if err := s.addModel(uri); err != nil {
			panic(err)
		}
	}

	for name := range modelsToDelete {
		s.removeModel(name)
	}
}

// addModel downloads one zipped model and unpacks it. The zip file stays in the
// zipped directory, which is how the next pass sees the model as installed.
func (s *ServingSidecar) addModel(uri ociobjectstore.ObjectURI) error {
	s.logger.Infof("Model '%s' to be downloaded\n", uri.ObjectName)
	if err := s.downloadWithRetries(uri); err != nil {
		return err
	}
	s.logger.Infof("Model '%s' downloaded\n", uri.ObjectName)
	if err := s.unzipModel(uri); err != nil {
		return err
	}
	s.logger.Infof("Model '%s' unzipped\n", uri.ObjectName)
	return nil
}

// downloadWithRetries fetches one zipped model into the zipped directory,
// trying up to maxRetries times with retryDelay between attempts, and returns
// the last attempt's error.
func (s *ServingSidecar) downloadWithRetries(uri ociobjectstore.ObjectURI) error {
	if err := os.MkdirAll(s.Config.ZippedFineTunedWeightDirectory, os.ModePerm); err != nil {
		return err
	}
	var err error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = s.Config.ObjectStorageDataStore.DownloadWithStrategy(
			uri,
			s.Config.ZippedFineTunedWeightDirectory,
			ociobjectstore.WithBaseNameOnly(true),
			ociobjectstore.WithChunkSize(DefaultDownloadChunkSizeInMB),
			ociobjectstore.WithThreads(DefaultDownloadThreads),
			ociobjectstore.WithSizeThreshold(BigFileSizeInMB),
		)
		if err == nil {
			return nil
		}
		s.logger.Infof("Error when downloading '%s'\n", uri.ObjectName)
		if attempt < maxRetries {
			s.logger.Infof("Retrying download '%s'\n", uri.ObjectName)
			time.Sleep(retryDelay)
		}
	}
	return err
}

// unzipModel unpacks a downloaded model from the zipped directory into the
// unzipped directory.
func (s *ServingSidecar) unzipModel(uri ociobjectstore.ObjectURI) error {
	zippedPath := filepath.Join(s.Config.ZippedFineTunedWeightDirectory, ociobjectstore.ObjectBaseName(uri.ObjectName))
	if err := zipper.Unzip(zippedPath, s.Config.UnzippedFineTunedWeightDirectory); err != nil {
		s.logger.Errorf("Error when unzip %s to %s. %v\n", zippedPath, s.Config.UnzippedFineTunedWeightDirectory, err)
		return err
	}
	return nil
}

// removeModel deletes a model's unpacked files and its zip file. A failed
// delete is logged and does not stop the sync.
func (s *ServingSidecar) removeModel(name string) {
	s.logger.Infof("Deleting fintuned model: %s\n", name)
	if err := deleteFilesWithMatchingString(s.Config.UnzippedFineTunedWeightDirectory, name); err != nil {
		s.logger.Errorf("Error when deleting unzipped model '%s' related files in %s, %v\n", name, s.Config.UnzippedFineTunedWeightDirectory, err)
	}
	if err := os.Remove(filepath.Join(s.Config.ZippedFineTunedWeightDirectory, name)); err != nil {
		s.logger.Errorf("Error when deleting zipped model '%s' related files in %s, %v\n", name, s.Config.ZippedFineTunedWeightDirectory, err)
	}
}

// Detects if a file changes
// Reference: https://medium.com/@skdomino/watch-this-file-watching-in-go-5b5a247cf71f
func (s *ServingSidecar) watchFileChanges(watcher *fsnotify.Watcher, filePath string) chan bool {
	changeDetected := make(chan bool)

	go func() {
		for {
			select {
			// watch for file change events
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}

				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
					// Add some delay, so we don't read the partially updated file
					time.Sleep(1 * time.Second)

					changeDetected <- true
					s.logger.Infof("File Watcher EVENT! %#v\n", event)
				}
			// watch for errors
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				s.logger.Errorf("File Watcher ERROR: %v", err)
			}
		}
	}()

	// out of the box fsnotify can watch a single file, or a single directory
	if err := watcher.Add(filePath); err != nil {
		panic(err)
	}

	return changeDetected
}

// Reads a JSON file and returns a slice of ObjectURIs and a slice of object names
func readObjectURIsFromFile(filePath string) ([]ociobjectstore.ObjectURI, []string, error) {
	// Read the content of the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// Read the entire file content as a byte slice
	fileContent, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	// Unmarshal the JSON content into a slice of ObjectURI
	var objectURIs []ociobjectstore.ObjectURI
	err = json.Unmarshal(fileContent, &objectURIs)
	if err != nil {
		return nil, nil, err
	}

	// Extract file names from ObjectURIs
	var fileNames []string
	for _, uri := range objectURIs {
		fileNames = append(fileNames, uri.ObjectName)
	}

	return objectURIs, fileNames, nil
}

// Reads all ft model ids based on existing zip files in the target directory
func getExistingFtModelNamesFromDir(directoryPath string) ([]string, error) {
	ftModelNameSet := make(map[string]struct{})

	err := filepath.Walk(directoryPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// the downloaded zipped object has no file extension
		if !info.IsDir() && filepath.Ext(info.Name()) == "" {
			fileName := info.Name()
			ftModelNameSet[fileName] = struct{}{}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	var list []string
	for key := range ftModelNameSet {
		list = append(list, key)
	}

	return list, nil
}

// Finds the differences between two string arrays
func findModelNameDifferences(ftModelNames, unzippedFtModelNames []string) (map[string]bool, map[string]bool) {
	// Create a map to store the presence of model names in ftModelNames
	ftModelMap := make(map[string]bool)
	for _, modelName := range ftModelNames {
		ftModelMap[modelName] = true
	}

	// Create a map to store the presence of model names in unzippedFtModelNames
	unzippedMap := make(map[string]bool)
	for _, modelName := range unzippedFtModelNames {
		unzippedMap[modelName] = true
	}

	// Find model names in ftModelNames but not in unzippedFtModelNames
	modelsToAdd := make(map[string]bool)
	for _, modelName := range ftModelNames {
		if _, exists := unzippedMap[modelName]; !exists {
			modelsToAdd[modelName] = true
		}
	}

	// Find model names in unzippedFtModelNames but not in ftModelNames
	modelsToDeleteMap := make(map[string]bool)
	for _, modelName := range unzippedFtModelNames {
		if _, exists := ftModelMap[modelName]; !exists {
			modelsToDeleteMap[modelName] = true
		}
	}
	return modelsToAdd, modelsToDeleteMap
}

// Deletes all files in a given directory and its subdirectories if the file name contains a matching string
func deleteFilesWithMatchingString(dirPath, matchString string) error {
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.Contains(info.Name(), matchString) {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
