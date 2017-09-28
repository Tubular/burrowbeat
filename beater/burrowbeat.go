package beater

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/goomzee/burrowbeat/config"
)

type Burrowbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client

	host    string
	port    string
	cluster string
	groups  []string
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Burrowbeat{
		done:   make(chan struct{}),
		config: config,
	}
	return bt, nil
}

func (bt *Burrowbeat) getEndpoint(endpoint string) (content map[string]interface{}, err error) {
	timeout := time.Duration(5 * time.Second)
	client := &http.Client{
		Timeout: timeout,
	}

	url := "http://" + bt.host + ":" + bt.port + "/v2/kafka/" + endpoint
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return content, err
	}

	req.Host = bt.host
	resp, err := client.Do(req)
	if err != nil {
		return content, err
	}

	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	err = json.Unmarshal(body, &content)

	return content, err
}

func (bt *Burrowbeat) Run(b *beat.Beat) error {
	logp.Info("burrowbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	bt.host = bt.config.Host
	bt.port = bt.config.Port
	bt.cluster = bt.config.Cluster
	bt.groups = bt.config.Groups[:]
	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		logp.Debug("main", "Running tick")
		groups := bt.groups
		if len(groups) == 0 {
			burrow_groups, err := bt.getEndpoint(bt.cluster)
			if err != nil {
				fmt.Errorf("Got an error: %v", err)
			} else {
				for _, group := range burrow_groups["consumers"].([]interface{}) {
					groups = append(groups, group.(string))
				}
			}
		}

		for _, group := range groups {
			bt.getConsumerGroupStatus(group)
		}
	}
}

func (bt *Burrowbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Burrowbeat) getConsumerGroupTopics(group string) (topics []string, err error) {
	response, err := bt.getEndpoint(bt.cluster + "/consumer/" + group + "/topic")

	if err != nil {
		return topics, err
	}

	for _, topic := range response["topics"].([]interface{}) {
		topics = append(topics, topic.(string))
	}

	return topics, err
}

func (bt *Burrowbeat) getConsumerGroupOffets(group string, topic string) (offsets []int64, err error) {
	response, err := bt.getEndpoint(bt.cluster + "/consumer/" + group + "/topic/" + topic)

	if err != nil {
		return offsets, err
	}

	for _, offset := range response["offsets"].([]interface{}) {
		offsets = append(offsets, int64(offset.(float64)))
	}

	return offsets, err
}

func (bt *Burrowbeat) getTopicOffets(topic string) (offsets []int64, err error) {
	response, err := bt.getEndpoint(bt.cluster + "/topic/" + topic)

	if err != nil {
		return offsets, err
	}

	for _, offset := range response["offsets"].([]interface{}) {
		offsets = append(offsets, int64(offset.(float64)))
	}

	return offsets, err
}

func (bt *Burrowbeat) getConsumerGroupStatus(group string) {
	topics, err := bt.getConsumerGroupTopics(group)

	if err != nil {
		logp.Err("Failed to get topics for %v consumer group.\n Error: %v", group, err)
		return
	}

	for _, topic := range topics {
		logp.Debug("Getting info for topic %v", topic)
		topic_offsets, err := bt.getTopicOffets(topic)
		if err != nil {
			logp.Err("Failed to get offsets for %v topic:\n %v", topic, err)
			continue
		}

		consumer_offsets, err := bt.getConsumerGroupOffets(group, topic)
		if err != nil {
			logp.Err("Failed to get offsets for %v consumer_group and %v topic:\n %v", group, topic, err)
			continue
		}

		topic_partitions := len(topic_offsets)
		var topic_lag, topic_size int64 = 0, 0

		for i, _ := range topic_offsets {
			partition_lag := topic_offsets[i] - consumer_offsets[i]

			if partition_lag < 0 {
				partition_lag = 0
			}

			consumer_group := common.MapStr{
				"name":      group,
				"topic":     topic,
				"partition": i,
				"offset":    consumer_offsets[i],
				"lag":       partition_lag,
			}
			event := common.MapStr{
				"@timestamp":     common.Time(time.Now()),
				"type":           "consumer_group",
				"cluster":        bt.cluster,
				"consumer_group": consumer_group,
			}
			bt.client.PublishEvent(event)

			topic_lag += partition_lag
			topic_size += topic_offsets[i]
		}

		topic_detail := common.MapStr{
			"name":       topic,
			"size":       topic_size,
			"partitions": topic_partitions,
			"lag":        topic_lag,
		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "topic",
			"cluster":    bt.cluster,
			"group":      group,
			"topic":      topic_detail,
		}
		bt.client.PublishEvent(event)
		logp.Info("Topic event sent")

	}
}
