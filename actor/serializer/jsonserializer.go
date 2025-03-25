package serializer

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// Note: This serializer uses reflection which is not the best option for performance.
// Try custom serializer for better performance
type JSONSerializer struct {
	stateType reflect.Type
}

func NewJSONSerializer(stateType any) *JSONSerializer {
	return &JSONSerializer{
		stateType: reflect.TypeOf(stateType),
	}
}

func (s *JSONSerializer) Serialize(state any) ([]byte, error) {
	return json.Marshal(state)
}

func (s *JSONSerializer) Deserialize(data []byte) (any, error) {
	state := reflect.New(s.stateType).Interface()

	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("failed to deserialize state: %w", err)
	}

	// Return the value (not the pointer) to prevent shared memory issues
	return reflect.ValueOf(state).Elem().Interface(), nil
}
