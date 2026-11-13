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
	PodMaxObservations        int64    `json:"pod_max_observations"`
	PodInflightBytes          int64    `json:"pod_inflight_bytes"`
	PodInflightMessages       int64    `json:"pod_inflight_messages"`
	TransportStreamsPerWorker int64    `json:"transport_streams_per_worker"`
	TransportBatchMaxBytes    int64    `json:"transport_batch_max_bytes"`
	TransportBatchMaxFrames   int64    `json:"transport_batch_max_frames"`
	UserIDHeader              []string `json:"user_id_header"`
	ExtraRequestHeaders       []string `json:"extra_request_headers"`
	SchemaVersion             int64    `json:"schema_version"`
	Enabled                   bool     `json:"enabled"`
	MetricsEnabled            bool     `json:"metrics_enabled"`
	RequestBodyCaptureEnabled bool     `json:"request_body_capture_enabled"`
	ConfigVersion             string   `json:"config_version"`
	ObserverCluster           string   `json:"observer_cluster"`
	AcceptedRequestHosts      []string `json:"accepted_request_hosts"`
	LogicalRequestIDHeaders   []string `json:"logical_request_id_headers"`
	Routes                    []Route  `json:"routes"`
	ChunkBytes                int64    `json:"chunk_bytes"`
	ResponseBodyMaxBytes      int64    `json:"response_body_max_bytes"`
	AckTimeoutMS              int64    `json:"ack_timeout_ms"`
	EndAckTimeoutMS           int64    `json:"end_ack_timeout_ms"`
	ObservationMaxMS          int64    `json:"observation_max_ms"`
	TickMS                    int64    `json:"tick_ms"`
	MetadataMaxBytes          int64    `json:"metadata_max_bytes"`
	AckMaxBytes               int64    `json:"ack_max_bytes"`
}
