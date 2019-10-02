package metrics

const (
	fivexx  = "5xx"
	fourxx  = "4xx"
	threexx = "3xx"
	twoxx   = "2xx"
	onexx   = "1xx"
	unknown = "unkown"
)

// GetBucketForHTTPStatus categorizes HTTP errors into buckets below.
// 100's = 1xx
// 200's = 2xx
// 300's = 3xx,
// 400's = 4xx
// 500's = 5xx
func GetBucketForHTTPStatus(httpStatus int) string {

	//we could use a switch here, but there are an enormous amount of status codes to test
	if httpStatus >= 500 {
		return fivexx
	}

	if httpStatus >= 400 {
		return fourxx
	}

	if httpStatus >= 300 {
		return threexx
	}

	if httpStatus >= 200 {
		return twoxx
	}

	if httpStatus >= 100 {
		return onexx
	}

	return unknown
}
