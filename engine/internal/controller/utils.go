package controller

import (
	"encoding/json"
	"github.com/google/uuid"
)

func InterfaceToString(p interface{}) (s string, err error) {
	if p == nil {
		return "", nil
	}
	// let see
	if b, err := json.Marshal(p); err == nil {
		return string(b), err
	}
	return
}
func StringToInterface(s string, p interface{}) (err error) {
	if len(s) == 0 || p == nil {
		return nil
	}
	return json.Unmarshal([]byte(s), p)
}
func ToString(c interface{}) string {
	if c == nil {
		return ""
	}
	s, _ := json.MarshalIndent(c, "", "\t")
	return string(s)
}
func ToPlainString(c interface{}) string {
	if c == nil {
		return ""
	}
	s, _ := json.Marshal(c)
	return string(s)
}
func GenerateUuid() string {
	return uuid.New().String()
}
func FromStringToMapStringInterface(s string) (map[string]interface{}, error) {
	if len(s) == 0 {
		return nil, nil
	}
	res := make(map[string]interface{})
	err := StringToInterface(s, &res)
	return res, err
}
