package configutils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

// ImportKey is the config value we look for that denotes a file to import when
// resolving the configuration.
var ImportKey = "imports"

// ResolveAndMergeFile will read the configuration file provided, resolve all
// imports from that configuration file, and then merge the resulting configs
// into the provided viper.
func ResolveAndMergeFile(v *viper.Viper, filePath string) error {
	// make sure the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return err
	}

	// see if Viper supports the extension
	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == "" {
		return errors.New("configuration file has no extension")
	}

	extSupported := false
	for _, e := range viper.SupportedExts {
		if ext[1:] == e { // we compare ignoring the leading dot
			extSupported = true
			break
		}
	}
	if !extSupported {
		return fmt.Errorf("unsupported configuration file extension: %s", ext)
	}
	// tell Viper what kind of file this will be
	v.SetConfigType(ext[1:])

	// specify the filepath
	v.SetConfigFile(filePath)

	// read the config
	err := v.ReadInConfig()
	if err != nil {
		return err
	}

	// resolve our imports
	if err := resolveAllImports(v); err != nil {
		return fmt.Errorf("could not resolve configuration imports: %v", err)
	}

	return nil
}

// resolveImports performs a DFS on the config imports mentioned by the viper
// config. The visited set is added to in pre-order traversal in order to
// prevent circular imports. The configs slice is appended to via post-order
// traversal to ensure we import the children first.
func resolveImports(v *viper.Viper, configs *[]string, visited *map[string]struct{}) error {
	imports := v.GetStringSlice(ImportKey)

	// bail early if we have no imports
	if len(imports) == 0 {
		return nil
	}

	for _, i := range imports {
		// skip empty imports (e.g., imports: or imports: -)
		if len(i) == 0 {
			continue
		}

		var path string
		if i[0] == os.PathSeparator {
			// assume the import is absolute and just clean it
			path = filepath.Clean(i)
		} else {
			// otherwise assume the import is relative to the current viper
			dir := filepath.Dir(v.ConfigFileUsed())
			path = filepath.Join(dir, i)
		}

		// ensure the file exists
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return err
		}

		// only visit new nodes
		if _, ok := (*visited)[path]; !ok {
			(*visited)[path] = struct{}{}

			// create a child viper that refers to this config
			child := viper.New()
			child.SetConfigFile(path)
			if err := child.ReadInConfig(); err != nil {
				return err
			}

			// descend
			if err := resolveImports(child, configs, visited); err != nil {
				return err
			}

			// append this config after all of our children to preserve the
			// import order
			*configs = append(*configs, path)
		}
	}

	return nil
}

// resolveAllImports will attempt to use the global viper instance to resolve
// and merge all imports into the global config.
func resolveAllImports(v *viper.Viper) error {
	configs := []string{}
	visited := make(map[string]struct{})

	if err := resolveImports(v, &configs, &visited); err != nil {
		return err
	}

	// add the root config to the end
	configs = append(configs, v.ConfigFileUsed())
	for _, configFilePath := range configs {
		if err := mergeConfigFile(v, configFilePath); err != nil {
			return fmt.Errorf("merging config %s: %w", configFilePath, err)
		}
	}

	return nil
}

func mergeConfigFile(v *viper.Viper, filePath string) error {
	r, err := os.Open(filePath)
	if err != nil {
		return err
	}

	defer func() { _ = r.Close() }()
	return v.MergeConfig(r)
}

func BindEnvsRecursive(v *viper.Viper, iface interface{}, path string) error {
	val := reflect.ValueOf(iface).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("mapstructure")

		// Skip fields without a mapstructure tag
		if tag == "" {
			continue
		}

		// Construct the full path for the current field
		fullPath := tag
		if path != "" {
			fullPath = path + "." + tag
		}

		// Get the field value, handle pointers without dereferencing nil pointers
		field := val.Field(i)

		if field.Kind() == reflect.Ptr {
			if field.IsNil() && field.Type().Elem().Kind() == reflect.Struct {
				// Initialize the pointer to a new struct if it's nil
				field.Set(reflect.New(field.Type().Elem()))
			}
			// Update field to the dereferenced struct (either existing or newly created)
			field = field.Elem()
		}

		// If the field is a struct (or pointer to a struct), recurse into it to construct nested paths
		if field.Kind() == reflect.Struct {
			// If it's a nil pointer, just recurse with fullPath
			if err := BindEnvsRecursive(v, field.Addr().Interface(), fullPath); err != nil {
				return err
			}
			// Binding the parent lets Viper shadow nested environment keys
			// during Unmarshal, so bind only the leaves of a struct.
			continue
		}

		if err := v.BindEnv(fullPath); err != nil {
			return fmt.Errorf("failed to bind environment variable: %w", err)
		}
	}

	return nil
}
