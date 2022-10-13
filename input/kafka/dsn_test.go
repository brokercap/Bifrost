package kafka

import "testing"

func TestParseDSN(t *testing.T) {
	var url string

	url = "127.0.0.1:9092"
	p := ParseDSN(url)
	if len(p) != 1 {
		t.Fatalf("len(p) != 1")
	}
	if p["addr"] != url {
		t.Fatalf("addr(%s) != %s", p["addr"], url)
	}

	url = "127.0.0.1:9092,10.10.10.10"
	p = ParseDSN(url)
	if len(p) != 1 {
		t.Fatalf("len(p) != 1")
	}
	if p["addr"] != url {
		t.Fatalf("addr(%s) != %s", p["addr"], url)
	}

	url = "127.0.0.1:9092,10.10.10.10?from.beginning=false"
	p = ParseDSN(url)
	if len(p) != 1 {
		t.Fatalf("len(p) != 1")
	}

	url = "127.0.0.1:9092,192.168.1.10/topic1,topic2?from.beginning=false"
	p = ParseDSN(url)
	if len(p) != 3 {
		t.Fatalf("len(p) != 3")
	}
	if p["addr"] != "127.0.0.1:9092,192.168.1.10" {
		t.Fatalf("addr(%s) != %s", p["addr"], "127.0.0.1:9092,192.168.1.10")
	}
	if p["topics"] != "topic1,topic2" {
		t.Fatalf("topics != %s", "topic1,topic2")
	}
	if p["from.beginning"] != "false" {
		t.Fatalf("from.beginning != %s", "false")
	}
}
