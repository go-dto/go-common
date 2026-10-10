package redis

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/go-dto/go-common/broker"
)

const (
	fieldData       = "data"
	fieldAttributes = "attributes"
)

func (d *Driver) encode(msg *broker.Message) ([]any, error) {
	if msg == nil {
		return nil, errors.New("broker/redis: nil message")
	}
	if msg.OrderingKey != "" {
		return nil, errors.New("broker/redis: ordering keys are not supported")
	}
	if len(msg.Attributes) > d.cfg.MaxAttributes {
		return nil, errors.New("broker/redis: too many message attributes")
	}
	attributeBytes := 0
	for key, value := range msg.Attributes {
		if key == "" || len(key) > 128 || !utf8.ValidString(key) || !utf8.ValidString(value) || strings.ContainsAny(key, "\r\n\x00") {
			return nil, errors.New("broker/redis: invalid message attribute")
		}
		attributeBytes += len(key) + len(value)
		if attributeBytes > d.cfg.MaxAttributeBytes {
			return nil, errors.New("broker/redis: message attributes are too large")
		}
	}
	attrs, err := json.Marshal(msg.Attributes)
	if err != nil {
		return nil, errors.New("broker/redis: encode message attributes")
	}
	if len(msg.Data)+len(attrs) > d.cfg.MaxMessageBytes {
		return nil, errors.New("broker/redis: message is too large")
	}
	return []any{fieldData, msg.Data, fieldAttributes, attrs}, nil
}

func (d *Driver) decode(raw goredis.XMessage, attempt int) (*broker.Message, error) {
	data, ok := fieldBytes(raw.Values[fieldData])
	if !ok {
		return nil, errors.New("broker/redis: malformed message data")
	}
	attrsBytes, ok := fieldBytes(raw.Values[fieldAttributes])
	if !ok {
		return nil, errors.New("broker/redis: malformed message attributes")
	}
	if !utf8.Valid(attrsBytes) {
		return nil, errors.New("broker/redis: malformed message attributes")
	}
	if len(data)+len(attrsBytes) > d.cfg.MaxMessageBytes {
		return nil, errors.New("broker/redis: message is too large")
	}
	attrs := make(map[string]string)
	if len(attrsBytes) > 0 {
		if err := json.Unmarshal(attrsBytes, &attrs); err != nil {
			return nil, errors.New("broker/redis: malformed message attributes")
		}
	}
	if len(attrs) > d.cfg.MaxAttributes {
		return nil, errors.New("broker/redis: too many message attributes")
	}
	attributeBytes := 0
	for key, value := range attrs {
		attributeBytes += len(key) + len(value)
		if key == "" || len(key) > 128 || !utf8.ValidString(key) || !utf8.ValidString(value) || strings.ContainsAny(key, "\r\n\x00") || attributeBytes > d.cfg.MaxAttributeBytes {
			return nil, errors.New("broker/redis: invalid message attributes")
		}
	}
	return &broker.Message{
		ID: raw.ID, Data: data, Attributes: attrs,
		PublishTime: streamTime(raw.ID), DeliveryAttempt: attempt,
	}, nil
}

func fieldBytes(value any) ([]byte, bool) {
	switch value := value.(type) {
	case string:
		return []byte(value), true
	case []byte:
		return append([]byte(nil), value...), true
	default:
		return nil, false
	}
}

func streamTime(id string) time.Time {
	millis, _, ok := strings.Cut(id, "-")
	if !ok {
		return time.Time{}
	}
	value, err := strconv.ParseInt(millis, 10, 64)
	if err != nil || value < 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}
