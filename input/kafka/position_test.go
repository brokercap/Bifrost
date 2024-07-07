package kafka

import "testing"

func TestInputKafka_SetTopicPartitionOffsetAndReturnGTID(t *testing.T) {
	c := NewInputKafka()
	gtid := c.SetTopicPartitionOffsetAndReturnGTID(nil)

	if gtid != "" {
		t.Fatalf("gtid(%s) not != ''", gtid)
	}
}
