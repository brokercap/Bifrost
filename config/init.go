package config

import (
	"os"
	"path/filepath"
	"strings"
)

func init()  {
	execDir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	pathArr := strings.Split(execDir,"/")
	BifrostDirName := pathArr[len(pathArr)-1]
	switch BifrostDirName {
	case "bin","sbin":
		BifrostDir = filepath.Dir(execDir)+"/"
		break
	default:
		BifrostDir = execDir+"/"
		break
	}
}