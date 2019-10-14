package controller

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"os/exec"
	"strings"
	"time"
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

// pick up value from env or generate a GUID for it
// TODO error handling if env is not defined.
func getEnvOrGenGuid(envName string, defaultValue string, required bool) (res string) {
	res = os.Getenv("ENGINE_ID")
	if required {
		return res // regardless of default value, must return, error check later
	}
	if res == "" {
		if defaultValue != "" {
			res = defaultValue
		} else {
			res = GenerateUuid()
		}
	}
	return res
}

/**
TODO
get the container id by
cat /proc/self/cgroup
13:name=systemd:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
12:pids:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
11:hugetlb:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
10:net_prio:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
9:perf_event:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
8:net_cls:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
7:freezer:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
6:devices:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
5:memory:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
4:blkio:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
3:cpuacct:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
2:cpu:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc
1:cpuset:/docker/d6b75a15a6dc5486e7c40473cff138e0c250f23350f2fd056000edd67a84d8dc

the rest -- just fab
*/
func getInitialContainerStatus() (containerId string, timestamp int64) {
	timestamp = time.Now().Unix()
	containerId = "dockerid:" + GenerateUuid()
	cmd := exec.Command("cat", "/proc/self/cgroup")
	out, err := cmd.CombinedOutput()
	if err == nil {
		ss := strings.Split(string(out), "\n")
		if len(ss) > 0 {
			// look for /docker
			keyword := "/docker/"
			if dockerStart := strings.Index(ss[0], keyword); dockerStart >= 0 {
				startAt := dockerStart + len(keyword)
				containerId = ss[0][startAt : startAt+16]
			}
		}
	}
	return
}

// get the request Work for engine ids
// this is a bit tricky in that if we want to have adapters and SI, we need say ffmpeg, streamlink, python etc
//

func discoverEngines() []string {
	// the first one is the ENGINE_ID env variable
	res := make([]string, 0)
	if mainEngineId := os.Getenv("ENGINE_ID"); mainEngineId != "" {
		res = append(res, mainEngineId)
	}
	// TODO -- need to really check for ffmpeg, streamlink as required by adapters, si
	// or now we'll just blindly think that it's there
	res = append(res, engineIdTVRA)
	res = append(res, engineIdWSA)
	res = append(res, engineIdSI2)
	return res
}
