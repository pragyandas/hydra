package serializer

import "encoding/json"

type JSONSerializer struct{}

func (s *JSONSerializer) Serialize(state any) ([]byte, error) {
	return json.Marshal(state)
}

func (s *JSONSerializer) Deserialize(data []byte) (any, error) {
	var state any
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return state, nil
}

func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}
