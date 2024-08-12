package util

import (
	"encoding/json"
	"math/rand"
	"os"
	"reflect"
)

func Stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return fi, err
}

func ToMap(v interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func UpdateMap(newMap, existingMap map[string]interface{}) {
	for k, v := range newMap {
		_, ok := existingMap[k]
		if !ok || existingMap[k] == nil {
			existingMap[k] = v
			continue
		}
		if v != nil && IsValid(v) && v != existingMap[k] {
			existingMap[k] = v
			continue
		}
		if reflect.TypeOf(v).Kind() == reflect.Slice && !reflect.DeepEqual(v, existingMap[k]) {
			existingMap[k] = v
			continue
		}
		if v1, ok := v.(map[string]interface{}); ok {
			if v2, ok := existingMap[k].(map[string]interface{}); ok {
				UpdateMap(v1, v2)
			}
		}
	}
}

func IsValid(value interface{}) bool {
	switch v := value.(type) {
	case int:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	case bool:
		return !v
	case nil:
		return true
	default:
		return false
	}
}

func GenerateObjectId() string {
	charSet := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, 26)
	for i := range b {
		b[i] = charSet[rand.Intn(len(charSet))]
	}
	return string(b)
}
