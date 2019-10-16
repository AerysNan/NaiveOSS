package osserror

var (
	MissingParameter = map[string]string{
		"bucket": "Missing bucket parameter",
		"key":    "Missing key parameter",
	}
	EmptyParameter = map[string]string{
		"bucket": "Empty bucket parameter",
		"key":    "Empty key parameter",
	}
)
