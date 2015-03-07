package common

type HealthCheck struct {
	WorkerId   string             `json:"worker_id"`
	Healthy    bool               `json:"healthy"`
	ResponseMS map[string]float64 `json:"response_ms"`
}

func (hc *HealthCheck) SetMS(name string, ms float64) {
	if hc.ResponseMS == nil {
		hc.ResponseMS = make(map[string]float64)
	}
	hc.ResponseMS[name] = ms
}
