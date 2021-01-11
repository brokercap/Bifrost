package elastic

type EsResponse struct {
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shards `json:"_shards"`
	ScrollId string `json:"_scroll_id"`
	Hits     Hits1  `json:"hits"`
}
type Shards struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}
type Total struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type Hits struct {
	Index  string      `json:"_index"`
	Type   string      `json:"_type"`
	ID     string      `json:"_id"`
	Score  float64     `json:"_score"`
	Source interface{} `json:"_source"`
}
type Hits1 struct {
	Total    Total   `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []Hits  `json:"hits"`
}


