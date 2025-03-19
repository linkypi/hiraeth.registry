package common

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"io"
	"regexp"
	"runtime"
	"strconv"
	"time"
)

func JsonUnmarshalForAny(source any, any interface{}) error {
	bytes, err := json.Marshal(source)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, any)
}

func ConvertEntityByJson(source any, target any) error {
	// Marshal source entity to JSON
	data, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("failed to marshal source: %v", err)
	}

	// Unmarshal JSON into target entity
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("failed to unmarshal into target: %v", err)
	}
	return nil
}

// isPureNumber checks if a string contains only numeric characters
func isPureNumber(input string) (int, bool) {
	num, err := strconv.Atoi(input)
	return num, err == nil
}

// ParseDuration parses a duration string into a time.Duration
// 支持 "y", "M", "d", "h", "m", "s" 格式如 "2y3M","10d3h", "3h", "2h30m10s", etc.
// 若输入的是纯数字则默认返回对应的秒数
func ParseDuration(input string) (time.Duration, error) {
	// Check if the input is a pure integer (default to seconds)
	if secs, ok := isPureNumber(input); ok {
		return time.Duration(secs) * time.Second, nil
	}

	// Define time unit to duration conversion
	unitToDuration := map[string]time.Duration{
		"y": time.Hour * 24 * 365, // 1 year = 365 days
		"M": time.Hour * 24 * 30,  // 1 month = 30 days
		"d": time.Hour * 24,       // 1 day
		"h": time.Hour,            // 1 hour
		"m": time.Minute,          // 1 minute
		"s": time.Second,          // 1 second
	}

	// Regular expression to match duration units (e.g., "10d", "3h", "2h30m10s", etc.)
	re := regexp.MustCompile(`(\d+)([yMdhms])`)
	matches := re.FindAllStringSubmatch(input, -1)

	if matches == nil {
		return 0, fmt.Errorf("invalid time format: %s", input)
	}

	var totalDuration time.Duration

	// Iterate over matches and calculate total duration
	for _, match := range matches {
		value, err := strconv.Atoi(match[1]) // Extract numeric value
		if err != nil {
			return 0, fmt.Errorf("invalid number: %s", match[1])
		}

		unit := match[2] // Extract unit (e.g., "y", "d", etc.)
		if conversion, ok := unitToDuration[unit]; ok {
			totalDuration += time.Duration(value) * conversion
		} else {
			return 0, fmt.Errorf("unknown time unit: %s", unit)
		}
	}

	return totalDuration, nil
}

// RecordTimeCostIfExceeded 若方法执行超出指定限制则记录方法耗时
// exceedMs 超出指定时间后才会在日志中记录方法耗时
// flag 特殊标记，方便日志过滤。无特殊需要可直接设置空字符串，因为该方法本身已在日志记录调用方的函数名
// 使用方式:
//
//		func MyFunc(){
//		   defer TimeCost(1000,"myFunc")() // MyFunc方法执行超出1秒则在日志记录实际耗时
//	       ...
//		}
func RecordTimeCostIfExceeded(exceedMs int64, flag string) func() {
	start := time.Now()
	pc, _, line, ok := runtime.Caller(1)
	if !ok {
		log.Errorf("runtime.Caller occur error", nil)
		return func() {}
	}

	funcName := runtime.FuncForPC(pc).Name()
	if len(flag) > 0 {
		flag = ", " + flag
	}
	return func() {
		cost := time.Since(start).Milliseconds()
		if cost > exceedMs {
			fmt.Printf(fmt.Sprintf("%s %s:%d, execute exceed %dms, cost %dms %s\n",
				TimeToMsString(time.Now()), funcName, line, exceedMs, cost, flag))
			msg := fmt.Sprintf("%s:%d, execute exceed %dms, cost %dms %s", funcName, line, exceedMs, cost, flag)
			log.Errorf(msg, nil)
		}
	}
}

func RecordTimeCost(flag string) func() {
	start := time.Now()
	pc, _, line, ok := runtime.Caller(1)
	if !ok {
		log.Errorf("runtime.Caller occur error", nil)
		return func() {}
	}

	funcName := runtime.FuncForPC(pc).Name()
	if len(flag) > 0 {
		flag = ", " + flag
	}
	return func() {
		cost := time.Since(start).Milliseconds()
		msg := fmt.Sprintf("%s %s:%d %s cost %dms ", TimeToMsString(time.Now()), funcName, line, flag, cost)
		log.Info(msg)
	}
}

func TimeToMsString(t time.Time) string {
	return t.Format("2006-01-02 15:04:05.000")
}
func TimeToSecString(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func TimestampToSecString(secs int64) string {
	t := time.Unix(secs, 0)
	return t.Format("2006-01-02 15:04:05")
}
func TimestampToMsString(secs int64) string {
	t := time.Unix(secs, 0)
	return t.Format("2006-01-02 15:04:05.000")
}
func TimestampToMinString(secs int64) string {
	t := time.Unix(secs, 0)
	return t.Format("2006-01-02 15:04")
}
func TimeToMinString(t time.Time) string {
	return t.Format("2006-01-02 15:04")
}
func Md5WithSalt(password, salt string) (string, error) {
	h := md5.New()
	_, err := io.WriteString(h, password+salt)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func PrintStackTrace() {
	if r := recover(); r != nil {
		pc := make([]uintptr, 10) // 至少需要 1 个元素的切片
		n := runtime.Callers(2, pc)
		frames := runtime.CallersFrames(pc[:n])

		stacks := make([]string, 0)
		for i := 3; i < n; i++ {
			frame, more := frames.Next()
			if !more {
				break
			}
			stack := fmt.Sprintf("%s:%d %s\r\n  ", frame.File, frame.Line, frame.Function)
			stacks = append(stacks, stack)
			//log.Errorf(stack)
		}

		// 打印 panic 信息
		log.Errorf("recovered from panic: %v\n  %s", r, strings.Join(stacks, ""))
	}
}
