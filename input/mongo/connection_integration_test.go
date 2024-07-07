//go:build integration
// +build integration

package mongo

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCreateMongoClient_Integration(t *testing.T) {
	Convey("normal", t, func() {
		_, err := CreateMongoClient(mongoUri, nil)
		So(err, ShouldBeNil)
	})
}
