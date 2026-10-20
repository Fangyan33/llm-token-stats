package gatewayconfig

type Route struct {
	RouteName string   `json:"route_name"`
	Enabled   bool     `json:"enabled"`
	Methods   []string `json:"methods"`
	Paths     []string `json:"paths"`
	ServiceID string   `json:"service_id"`
	AdapterID string   `json:"adapter_id"`
}
type Config struct {
	SchemaVersion           int64    `json:"schema_version"`
	Enabled                 bool     `json:"enabled"`
	ConfigVersion           string   `json:"config_version"`
	GatewayInstanceID       string   `json:"gateway_instance_id"`
	ObserverCluster         string   `json:"observer_cluster"`
	AcceptedRequestHosts    []string `json:"accepted_request_hosts"`
	Routes                  []Route  `json:"routes"`
	ChunkBytes              int64    `json:"chunk_bytes"`
	CallbackMaxBytes        int64    `json:"callback_max_bytes"`
	CallbackMaxChunks       int64    `json:"callback_max_chunks"`
	RequestBodyMaxBytes     int64    `json:"request_body_max_bytes"`
	RewriteTimeoutMS        int64    `json:"rewrite_timeout_ms"`
	RewriteReservationBytes int64    `json:"rewrite_reservation_bytes"`
	RootRewriteBudgetBytes  int64    `json:"root_rewrite_budget_bytes"`
	RootMaxRewrites         int64    `json:"root_max_rewrites"`
	ResponseBodyMaxBytes    int64    `json:"response_body_max_bytes"`
	StreamInflightBytes     int64    `json:"stream_inflight_bytes"`
	StreamInflightMessages  int64    `json:"stream_inflight_messages"`
	RootMaxStreams          int64    `json:"root_max_streams"`
	RootInflightBytes       int64    `json:"root_inflight_bytes"`
	RootInflightMessages    int64    `json:"root_inflight_messages"`
	AckTimeoutMS            int64    `json:"ack_timeout_ms"`
	EndAckTimeoutMS         int64    `json:"end_ack_timeout_ms"`
	ObservationMaxMS        int64    `json:"observation_max_ms"`
	TickMS                  int64    `json:"tick_ms"`
	MetadataMaxBytes        int64    `json:"metadata_max_bytes"`
	AckMaxBytes             int64    `json:"ack_max_bytes"`
}
