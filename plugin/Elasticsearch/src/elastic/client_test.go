package elastic

import (
	"flag"
	"fmt"
	"reflect"
	"testing"
)

var host = flag.String("host", "127.0.0.1", "Elasticsearch host")
var port = flag.Int("port", 9200, "Elasticsearch port")

type elasticTester struct {
	c *Client
	t *testing.T
}

func (s *elasticTester) SetUpSuite(t *testing.T) {
	cfg := new(ClientConfig)
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.User = ""
	cfg.Password = ""
	s.c = NewClient(cfg)
	s.t = t
}

func makeTestData(arg1 string, arg2 string) map[string]interface{} {
	m := make(map[string]interface{})
	m["name"] = arg1
	m["content"] = arg2

	return m
}
func (s *elasticTester) assertEaqual(got, want interface{}) {
	if !reflect.DeepEqual(got, want) {
		s.t.Errorf("got = %v, want %v", got, want)
	}
}

func (s *elasticTester) TestSimple() {
	index := "dummy"

	items := make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}

	resp, err := s.c.IndexBulk(index, items)
	s.assertEaqual(err, nil)
	err = s.c.Update(index, "1", makeTestData("abc", "hello world"))

	exists, err := s.c.Exists(index, "1")
	s.assertEaqual(err, nil)

	s.assertEaqual(exists, true)

	r, err := s.c.Get(index, "1")
	s.assertEaqual(err, nil)

	s.assertEaqual(r.Code, 200)
	s.assertEaqual(r.ID, "1")

	err = s.c.Delete(index, "1")
	s.assertEaqual(err, nil)

	exists, err = s.c.Exists(index, "1")
	s.assertEaqual(err, nil)

	s.assertEaqual(exists, false)

	items = make([]*BulkRequest, 10)

	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionIndex
		req.ID = id
		req.Data = makeTestData(fmt.Sprintf("abc %d", i), fmt.Sprintf("hello world %d", i))
		items[i] = req
	}

	resp, err = s.c.IndexBulk(index, items)
	s.assertEaqual(err, nil)

	s.assertEaqual(resp.Code, 200)
	s.assertEaqual(resp.Errors, false)

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%d", i)
		req := new(BulkRequest)
		req.Action = ActionDelete
		req.ID = id
		items[i] = req
	}

	resp, err = s.c.IndexBulk(index, items)
	s.assertEaqual(err, nil)

	s.assertEaqual(resp.Code, 200)
	s.assertEaqual(resp.Errors, false)
}

func (s *elasticTester) TestMapping() {
	index := "dummyy"

	resp, err := s.c.GetMapping(index)
	s.assertEaqual(err, nil)
	// log.Println("GetMapping resp:", g.Export(resp))

	err = s.c.CreateMapping(index)
	s.assertEaqual(err, nil)

	resp, err = s.c.GetMapping(index)
	s.assertEaqual(err, nil)
	s.assertEaqual(resp.Mapping[index].Mappings.DateDetection, true)
	resp, err = s.c.GetMapping(index)

	if res, ok:=resp.Mapping[index];ok && res.Mappings.DateDetection {

	}
	// log.Println("GetMapping resp:", g.Export(resp))

}

func TestSimple(t *testing.T) {
	tester := &elasticTester{}
	tester.SetUpSuite(t)
	tester.TestSimple()
}

func TestMapping(t *testing.T) {
	tester := &elasticTester{}
	tester.SetUpSuite(t)
	tester.TestMapping()
}
