/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/Shopify/sarama"
)

type ConsumerConfig struct {
	BrokerServerList []string
	GroupID          string
	config           *sarama.Config
}

type NetConfig struct {
	TimeoutMs   int `json:"timeout.ms,string"`
	KeepAliveMs int `keepalive.ms,string`
}

type TLSConfig struct {
	Cert                string `json:"cert"`
	Key                 string `json:"key"`
	CA                  string `json:"ca"`
	InsescureSkipVerify bool   `json:"insecure.skip.verify,bool"`
	ServerName          string `json:"servername"`
}

type SaslConfig struct {
	SaslMechanism string `json:"sasl.mechanism"`
	SaslUser      string `json:"sasl.user"`
	SaslPassword  string `json:"sasl.password"`
}

type KafkaConfig struct {
	NetConfig
	*SaslConfig
	ClientID             string `json:"client.id"`
	GroupID              string `json:"group.id"`
	RetryBackOffMS       int    `json:"retry.backoff.ms,int"`
	MetadataMaxAgeMS     int    `json:"metadata.max.age.ms,int"`
	SessionTimeoutMS     int32  `json:"session.timeout.ms,int32"`
	FetchMaxWaitMS       int32  `json:"fetch.max.wait.ms,int32"`
	FetchMaxBytes        int32  `json:"fetch.max.bytes,int32"`
	FetchMinBytes        int32  `json:"fetch.min.bytes,int32"`
	FromBeginning        bool   `json:"from.beginning,bool"`
	AutoCommit           bool   `json:"auto.commit,bool"`
	AutoCommitIntervalMS int    `json:"auto.commit.interval.ms,int"`

	TLSEnabled bool       `json:"tls.enabled,bool"`
	TLS        *TLSConfig `json:"tls"`
}

func defaultConsumerConfig() *KafkaConfig {
	c := &KafkaConfig{
		GroupID:       defaultKafkaGroupId,
		FromBeginning: false,
		AutoCommit:    false,
	}
	return c
}

func createTLSConfig(certFile, keyFile, caFile string, verify bool) (tlsConfig *tls.Config, err error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("cert not found")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tlsConfig, err
	}
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return tlsConfig, err
	}
	rootsCAs := x509.NewCertPool()
	ok := rootsCAs.AppendCertsFromPEM(caCert)
	if !ok {
		err = fmt.Errorf("rootsCAs.AppendCertsFromPEM result != true")
		return
	}
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            rootsCAs,
		InsecureSkipVerify: verify,
	}
	return
}

func getConsumerConfig(version string, config map[string]interface{}) (kafkaConnectConfig *sarama.Config, err error) {
	b, err := json.Marshal(config)
	if err != nil {
		return
	}
	rawConfig := defaultConsumerConfig()
	err = json.Unmarshal(b, rawConfig)
	if err != nil {
		return
	}
	cfg := sarama.NewConfig()
	if cfg.Version, err = sarama.ParseKafkaVersion(version); err != nil {
		return
	}
	if rawConfig.ClientID != "" {
		cfg.ClientID = rawConfig.ClientID
	}
	if rawConfig.MetadataMaxAgeMS > 0 {
		cfg.Metadata.Timeout = time.Duration(rawConfig.MetadataMaxAgeMS) * time.Millisecond
	}
	if rawConfig.RetryBackOffMS > 0 {
		cfg.Consumer.Retry.Backoff = time.Duration(rawConfig.RetryBackOffMS) * time.Millisecond
	}

	if rawConfig.FromBeginning {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if rawConfig.SaslConfig != nil {
		cfg.Net.SASL.User = rawConfig.SaslUser
		cfg.Net.SASL.Password = rawConfig.SaslPassword
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(rawConfig.SaslMechanism)
	}
	if rawConfig.TLS != nil && rawConfig.TLSEnabled {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config, err = createTLSConfig(rawConfig.TLS.Cert, rawConfig.TLS.Key, rawConfig.TLS.CA, rawConfig.TLS.InsescureSkipVerify)
	}

	return cfg, err

}
