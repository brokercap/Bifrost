package warning

var dirverMap map[string]WarningFunInterface

func init() {
	dirverMap = make(map[string]WarningFunInterface, 0)
}

func Register(name string, f WarningFunInterface) {
	dirverMap[name] = f
}

type WarningFunInterface interface {
	SendWarning(p map[string]interface{}, title string, body string) error
}
