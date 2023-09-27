package utils

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// 获取当前进程ID
func GetCurrentProcessID() string {
	return strconv.Itoa(os.Getpid())
}

// 获取当前协程ID
func GetCurrentGoroutineID() string {
	buf := make([]byte, 128)
	//runtime.Stack 函数是用于获取当前 Goroutine 的堆栈信息的函数
	buf = buf[:runtime.Stack(buf, false)]
	stackInfo := string(buf)
	//使用 "[running]" 字符串将堆栈信息分成两部分,然后再使用 "goroutine" 将第一部分分成两部分
	//取第二部分作为 Goroutine 的唯一标识符。
	return strings.TrimSpace(strings.Split(strings.Split(stackInfo, "[running]")[0], "goroutine")[1])
}

func GetProcessAndGoroutineIDStr() string {
	return fmt.Sprintf("%s_%s", GetCurrentProcessID(), GetCurrentGoroutineID())
}
