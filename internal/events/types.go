package events

type Token struct {
	Value  *int64 `json:"value"`
	Source string `json:"source"`
}
type Usage struct {
	InputTokens         Token `json:"input_tokens"`
	OutputTokens        Token `json:"output_tokens"`
	CachedReadTokens    Token `json:"cached_read_tokens"`
	CachedWriteTokens   Token `json:"cached_write_tokens"`
	ReasoningTokens     Token `json:"reasoning_tokens"`
	ReportedTotalTokens Token `json:"reported_total_tokens"`
}
type Event struct {
	SchemaVersion          int      `json:"schema_version"`
	EventID                string   `json:"event_id"`
	EventType              string   `json:"event_type"`
	LogicalRequestID       string   `json:"logical_request_id"`
	XRequestID             *string  `json:"x_request_id"`
	ObservationID          string   `json:"observation_id"`
	ObservationScope       string   `json:"observation_scope"`
	ProducerID             string   `json:"producer_id"`
	GatewayInstanceID      string   `json:"gateway_instance_id"`
	ConfigVersion          string   `json:"config_version"`
	AdapterID              string   `json:"adapter_id"`
	RouteName              string   `json:"route_name"`
	RequestHost            *string  `json:"request_host"`
	ClusterName            string   `json:"cluster_name"`
	DeploymentEnvironment  string   `json:"deployment_environment"`
	ServiceID              *string  `json:"service_id"`
	ServiceName            *string  `json:"service_name"`
	ProviderID             *string  `json:"provider_id"`
	ProviderName           *string  `json:"provider_name"`
	MappingVersion         *string  `json:"mapping_version"`
	MappingStatus          string   `json:"mapping_status"`
	ApiKeyID               *string  `json:"api_key_id"`
	CustomerID             *string  `json:"customer_id"`
	CustomerName           *string  `json:"customer_name"`
	CustomerMappingVersion *string  `json:"customer_mapping_version"`
	CustomerMappingStatus  string   `json:"customer_mapping_status"`
	IdentityStatus         string   `json:"identity_status"`
	RequestStartedAtMs     int64    `json:"request_started_at_ms"`
	ObservationEndedAtMs   *int64   `json:"observation_ended_at_ms"`
	RequestFinishedAtMs    *int64   `json:"request_finished_at_ms"`
	EventCreatedAtMs       int64    `json:"event_created_at_ms"`
	RequestModel           *string  `json:"request_model"`
	ResponseModel          *string  `json:"response_model"`
	HTTPStatus             *int64   `json:"http_status"`
	UpstreamAttemptCount   *int64   `json:"upstream_attempt_count"`
	ResponseFlags          *string  `json:"response_flags"`
	ResponseCodeDetails    *string  `json:"response_code_details"`
	BusinessOutcome        *string  `json:"business_outcome"`
	TerminalObserved       bool     `json:"terminal_observed"`
	ObservationStatus      *string  `json:"observation_status"`
	UsageStatus            string   `json:"usage_status"`
	UsageEligible          bool     `json:"usage_eligible"`
	Usage                  *Usage   `json:"usage"`
	RawUsageJSON           string   `json:"raw_usage_json"`
	RawUsageTruncated      bool     `json:"raw_usage_truncated"`
	AnomalyCodes           []string `json:"anomaly_codes"`
}
