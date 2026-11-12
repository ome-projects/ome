package testing

import (
	"github.com/stretchr/testify/mock"

	"sigs.k8s.io/ome/pkg/logging"
)

// MockLogger implements logging.Interface for testing
type MockLogger struct {
	mock.Mock
}

func (m *MockLogger) Debug(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Info(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Warn(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Error(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) Fatal(msg string) {
	m.Called(msg)
}

func (m *MockLogger) Fatalf(format string, args ...interface{}) {
	m.Called(format, args)
}

func (m *MockLogger) WithField(key string, value interface{}) logging.Interface {
	args := m.Called(key, value)
	return args.Get(0).(logging.Interface)
}

func (m *MockLogger) WithError(err error) logging.Interface {
	args := m.Called(err)
	return args.Get(0).(logging.Interface)
}

// Helper functions for creating test data

// SetupMockLogger creates a mock logger that returns itself for chaining methods
func SetupMockLogger() *MockLogger {
	mockLogger := &MockLogger{}

	// Setup common expectations for logger chaining
	mockLogger.On("WithField", mock.Anything, mock.Anything).Return(mockLogger).Maybe()
	mockLogger.On("WithError", mock.Anything).Return(mockLogger).Maybe()

	// Setup common logging calls to not fail tests
	mockLogger.On("Debug", mock.Anything).Maybe()
	mockLogger.On("Debugf", mock.Anything, mock.Anything).Maybe()
	mockLogger.On("Info", mock.Anything).Maybe()
	mockLogger.On("Infof", mock.Anything, mock.Anything).Maybe()
	mockLogger.On("Warn", mock.Anything).Maybe()
	mockLogger.On("Warnf", mock.Anything, mock.Anything).Maybe()
	mockLogger.On("Error", mock.Anything).Maybe()
	mockLogger.On("Errorf", mock.Anything, mock.Anything).Maybe()

	return mockLogger
}
