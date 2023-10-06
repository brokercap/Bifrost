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
	"github.com/Shopify/sarama"
	"io/ioutil"
	"strconv"
	"strings"
)

type ParamMap map[string]string

type Config struct {
	BrokerServerList []string
	GroupId          string
	Topics           []string
	ParamConfig      *sarama.Config
	SkipSerializeErr bool
	ParamMap         ParamMap
	CosumerCount     int // 处理从kafka连接出来之后的处理数据的协程数量,默认 1
}

type TLSConfig struct {
	TLSEnabled             bool   `json:"net.tls.enabled,string"`
	TLSCert                string `json:"net.tls.cert"`
	TLSKey                 string `json:"net.tls.key"`
	TLSCA                  string `json:"net.tls.ca"`
	TLSInsescureSkipVerify bool   `json:"net.tls.insecure.skip.verify,string"`
	TLSServerName          string `json:"net.tls.servername"`
}

type SaslConfig struct {
	SaslMechanism string `json:"net.sasl.mechanism"`
	SaslUser      string `json:"net.sasl.user"`
	SaslPassword  string `json:"net.sasl.password"`
	SCRAMAuthzID  string `json:"net.sasl.SCRAMAuthzID"`
}

type ConnectorParamConfig struct {
	*SaslConfig
	ClientID string `json:"client.id"`
	GroupID  string `json:"group.id"`
	*TLSConfig
	FromBeginning bool `json:"from.beginning,string"`
}

func defaultConnectorParamConfig() *ConnectorParamConfig {
	c := &ConnectorParamConfig{}
	return c
}

func createTLSConfig(certFile, keyFile, caFile string, verify bool, serverName string) (tlsConfig *tls.Config, err error) {
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
		ServerName:         serverName,
	}
	return
}

func getKafkaConnectConfig(config map[string]string) (kafkaConnectConfig *Config, err error) {
	kafkaConnectConfig = &Config{}
	b, err := json.Marshal(config)
	if err != nil {
		return
	}
	var version string
	if _, ok := config["version"]; !ok {
		version = defaultKafkaVersion
	} else {
		version = fmt.Sprint(config["version"])
	}
	rawConfig := defaultConnectorParamConfig()
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

	if rawConfig.FromBeginning {
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if rawConfig.SaslConfig != nil {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = rawConfig.SaslUser
		cfg.Net.SASL.Password = rawConfig.SaslPassword
		cfg.Net.SASL.Mechanism = sarama.SASLMechanism(rawConfig.SaslMechanism)
		cfg.Net.SASL.SCRAMAuthzID = rawConfig.SCRAMAuthzID
		switch cfg.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA256} }
		case sarama.SASLTypeSCRAMSHA512:
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &SCRAMClient{HashGeneratorFcn: SHA512} }
		}
	}
	if rawConfig.TLSConfig != nil && rawConfig.TLSEnabled {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config, err = createTLSConfig(rawConfig.TLSCert, rawConfig.TLSKey, rawConfig.TLSCA, rawConfig.TLSInsescureSkipVerify, rawConfig.TLSServerName)
	}
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	kafkaConnectConfig.ParamConfig = cfg

	if rawConfig.GroupID != "" {
		kafkaConnectConfig.GroupId = rawConfig.GroupID
	}

	if _, ok := config["topics"]; ok {
		if config["topics"] != "" {
			kafkaConnectConfig.Topics = strings.Split(fmt.Sprint(config["topics"]), ",")
		}
	}
	if _, ok := config["addr"]; ok {
		kafkaConnectConfig.BrokerServerList = strings.Split(fmt.Sprint(config["addr"]), ",")
	}
	if _, ok := config["skip.serialize.err"]; ok {
		kafkaConnectConfig.SkipSerializeErr = true
	}
	if _, ok := config["consumer.count"]; ok {
		kafkaConnectConfig.CosumerCount, _ = strconv.Atoi(config["consumer.count"])
	}
	if kafkaConnectConfig.CosumerCount <= 0 {
		kafkaConnectConfig.CosumerCount = DefaultConsumerCount
	}

	return kafkaConnectConfig, err
}
