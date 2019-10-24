package processing

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"sync"
)

// jsonEncoder encodes JSON.
type JsonEncoder struct {
	v    interface{}
	once sync.Once
	b    []byte
	err  error
}

func NewJSONEncoder(v interface{}) sarama.Encoder {
	return &JsonEncoder{v: v}
}

func (j *JsonEncoder) encode() {
	j.once.Do(func() {
		j.b, j.err = json.Marshal(j.v)
	})
}

func (j *JsonEncoder) Encode() ([]byte, error) {
	j.encode()
	return j.b, j.err
}

func (j *JsonEncoder) Length() int {
	j.encode()
	return len(j.b)
}
