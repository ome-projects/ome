// Package agentconfig reads an agent module's settings from the process
// environment and the configuration file the agent was started with.
package agentconfig

import (
	"fmt"
	"reflect"

	"github.com/spf13/viper"
)

// BindEnvs binds every setting of c, a pointer to a struct, to its
// environment variable, so v.Unmarshal reads the environment for a setting
// the configuration file leaves out. A setting's key is its mapstructure tag,
// nested under the tags of the structs that hold it. Only settings are bound,
// never the key of a struct that holds settings: viper drops a setting whose
// holding key is bound too, in an order that changes from one start to the
// next, so a setting given through the environment alone would be read on
// some starts and missed on others. A nil pointer to a struct is allocated so
// the settings inside it can be bound.
func BindEnvs(v *viper.Viper, c any) error {
	_, err := bindSettings(v, reflect.ValueOf(c).Elem(), "")
	return err
}

// bindSettings binds the settings of the struct value under prefix and
// reports how many it bound. A struct that holds no setting is one setting
// under its own key.
func bindSettings(v *viper.Viper, value reflect.Value, prefix string) (int, error) {
	bound := 0
	for i := 0; i < value.NumField(); i++ {
		tag := value.Type().Field(i).Tag.Get("mapstructure")
		if tag == "" {
			continue
		}
		key := tag
		if prefix != "" {
			key = prefix + "." + tag
		}
		field := value.Field(i)
		if field.Kind() == reflect.Pointer && field.Type().Elem().Kind() == reflect.Struct {
			if field.IsNil() {
				field.Set(reflect.New(field.Type().Elem()))
			}
			field = field.Elem()
		}
		if field.Kind() == reflect.Struct {
			inside, err := bindSettings(v, field, key)
			if err != nil {
				return bound, err
			}
			if inside > 0 {
				bound += inside
				continue
			}
		}
		if err := v.BindEnv(key); err != nil {
			return bound, fmt.Errorf("bind %s to its environment variable: %w", key, err)
		}
		bound++
	}
	return bound, nil
}
