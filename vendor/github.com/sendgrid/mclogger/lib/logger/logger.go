package logger

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	logrus.ErrorKey = ErrorMessageKey
	logger = logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	setLogLevel(logrus.InfoLevel.String())
}

// Field name keys
const (
	// Default field names
	AppKey        = "app"
	AppVersionKey = "app_version"
	EventKey      = "event"
	ServerKey     = "server"

	// Fields defined by the event schema:
	// https://wiki.sendgrid.net/display/DALX/Event+Schema+Standards
	UserIDKey        = "userid"
	ProcessedKey     = "processed"
	SchemaVersionKey = "schema_version"
	ErrorMessageKey  = "message"

	// Field names for HTTP request details
	HTTPMethodKey = "http_method"
	URLPathKey    = "url_path"
	ClientIPKey   = "client_ip"

	// Other common keys
	HandlerKey        = "handler"
	ResponseStatusKey = "resp_status"
)

const (
	// contextLogEntryKey is used as the key for the log entry stored on the context
	contextLogEntryKey = contextKey("logEntryKey")

	placeholder   = "should be set in Setup()"
	schemaVersion = 1

	xForwardedForHeader = "X-Forwarded-For"
)

var (
	logger        *logrus.Logger
	defaultFields = DefaultFields{
		AppName: placeholder,
		Event:   placeholder,
		Server:  placeholder,
		Version: placeholder,
	}
)

// contextKey is created to prevent collisions from other code embedding values in the context and causing collisions
type contextKey string

func (c contextKey) String() string {
	return "mclogger " + string(c)
}

// DefaultFields contains application-wide values for the default log fields that will be set on every log entry
type DefaultFields struct {
	AppName string
	Event   string
	Server  string
	Version string
}

// Entry represents a log entry which should eventually be written out to the logs
type Entry struct {
	le *logrus.Entry
}

// Setup is called to set up the logger and set common fields for all log entries from a given service.
// Only needs to be called once per service/lambda initialization.
func Setup(level string, df DefaultFields) {
	setLogLevel(level)
	setDefaultFields(df)
}

// setDefaultFields sets the default fields to the supplied values if they are not empty string
func setDefaultFields(df DefaultFields) {
	if df.Event != "" {
		defaultFields.Event = df.Event
	}

	if df.AppName != "" {
		defaultFields.AppName = df.AppName
	}

	if df.Server != "" {
		defaultFields.Server = df.Server
	}

	if df.Version != "" {
		defaultFields.Version = df.Version
	}
}

// setLogLevel will set the log level on the logger to the value in the config and default to Info if parsing the level fails
func setLogLevel(level string) {
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		logger.Errorf("Log level '%s' could not be parsed: %v", level, err)
		logLevel = logrus.InfoLevel
	}

	logger.SetLevel(logLevel)

	// Include file name and line number of where the function call to write the log event out is made
	if logLevel == logrus.DebugLevel {
		logger.SetReportCaller(true)
	}
}

// NewEntry creates a log entry with all the standard expected fields
func NewEntry() *Entry {
	entry := logrus.NewEntry(logger)
	defaultFields := logrus.Fields{
		// These fields should be on every log event and adhere to the event schema standards documented here:
		// https://wiki.sendgrid.net/display/DALX/Event+Schema+Standards
		AppKey:           defaultFields.AppName,
		AppVersionKey:    defaultFields.Version,
		EventKey:         defaultFields.Event,
		ServerKey:        defaultFields.Server,
		SchemaVersionKey: schemaVersion,
		ProcessedKey:     time.Now().Unix(),
	}

	return &Entry{le: entry.WithFields(defaultFields)}
}

// NewHTTPEntry creates a log entry from an HTTP request with all the standard expected fields that are available at the start of a request
func NewHTTPEntry(r *http.Request) *Entry {
	log := NewEntry()

	// Set HTTP Request fields
	log.SetField(HTTPMethodKey, r.Method)
	log.SetField(URLPathKey, r.URL.Path)
	log.SetField(ClientIPKey, r.Header.Get(xForwardedForHeader))

	return log
}

// ContextWithEntry creates a new context with the entry set as a value using the supplied context as the parent context
func ContextWithEntry(ctx context.Context, entry *Entry) context.Context {
	return context.WithValue(ctx, contextLogEntryKey, entry)
}

// MustGetEntryFromContext retrieves the log entry from the context or panics if not set
func MustGetEntryFromContext(ctx context.Context) *Entry {
	e, err := EntryFromContext(ctx)
	if err != nil {
		panic(err)
	}
	return e
}

// EntryFromContext retrieves the log entry from the context or returns an error if not set
func EntryFromContext(ctx context.Context) (*Entry, error) {
	val := ctx.Value(contextLogEntryKey)
	if val == nil {
		return nil, errors.New("log entry not set on context")
	}

	entry, ok := val.(*Entry)
	if !ok {
		return nil, errors.New("invalid log entry set on context")
	}

	return entry, nil
}

// SetField sets the field with the supplied key and value. Multiple calls for the same key will overwrite the value.
func (e *Entry) SetField(key string, value interface{}) *Entry {
	e.le = e.le.WithField(key, value)
	return e
}

// SetError sets the error message field with the error string from the supplied error
func (e *Entry) SetError(err error) *Entry {
	e.le = e.le.WithError(err)
	return e
}

// SetUserID logs the user ID to the log entry using the correct user ID field name
func (e *Entry) SetUserID(userID int64) *Entry {
	e.SetField(UserIDKey, userID)
	return e
}

// SetResponseStatusCode sets the HTTP response status code returned to the client
func (e *Entry) SetResponseStatusCode(statusCode int) *Entry {
	e.SetField(ResponseStatusKey, statusCode)
	return e
}

// SetHandler sets the name of the handler function on the log entry
func (e *Entry) SetHandler(handlerName string) *Entry {
	e.SetField(HandlerKey, handlerName)
	return e
}

// Debug writes the log entry out to DEBUG level
func (e *Entry) Debug(args ...interface{}) {
	e.le.Debug(args...)
}

// Info writes the log entry out to INFO level
func (e *Entry) Info(args ...interface{}) {
	e.le.Info(args...)
}

// Warn writes the log entry out to WARN level
func (e *Entry) Warn(args ...interface{}) {
	e.le.Warn(args...)
}

// Error writes the log entry out to ERROR level
func (e *Entry) Error(args ...interface{}) {
	e.le.Error(args...)
}

// Fatal writes the log entry out to Fatal level
func (e *Entry) Fatal(args ...interface{}) {
	e.le.Fatal(args...)
}

// Debugf writes the log entry out to DEBUG level
func (e *Entry) Debugf(format string, args ...interface{}) {
	e.le.Debugf(format, args...)
}

// Infof writes the log entry out to INFO level
func (e *Entry) Infof(format string, args ...interface{}) {
	e.le.Infof(format, args...)
}

// Warnf writes the log entry out to WARN level
func (e *Entry) Warnf(format string, args ...interface{}) {
	e.le.Warnf(format, args...)
}

// Errorf writes the log entry out to ERROR level
func (e *Entry) Errorf(format string, args ...interface{}) {
	e.le.Errorf(format, args...)
}

// Fatalf writes the log entry out to Fatal level
func (e *Entry) Fatalf(format string, args ...interface{}) {
	e.le.Fatalf(format, args...)
}
