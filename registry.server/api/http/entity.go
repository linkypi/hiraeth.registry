package http

type RegisterRequest struct {
	ServiceName string `json:"serviceName"`
	Ip          string `json:"ip"`
	Port        int    `json:"port"`
}

type RestResult struct {
	Success bool        `json:"success"`
	Code    string      `json:"code"`
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
}

func (r RestResult) WithData(data interface{}) RestResult {
	r.Data = data
	return r
}
func (r RestResult) WithCode(code string) RestResult {
	r.Code = code
	return r
}
func (r RestResult) WithMsg(msg string) RestResult {
	r.Message = msg
	return r
}
func SuccessResult() *RestResult {
	return &RestResult{Success: true}
}
func SuccessResultWithData(data interface{}) *RestResult {
	return &RestResult{Success: true, Data: data}
}

func FiledResult() *RestResult {
	return &RestResult{Success: false}
}
func FiledResultWithMsg(msg string) *RestResult {
	return &RestResult{Success: false, Message: msg}
}
