package beater

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
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
			consumer_group, err := bt.getEndpoint(bt.cluster + "/consumer/" + group + "/lag")
			if err != nil {
				fmt.Errorf("Got an error: %v", err)
			} else {
				bt.getConsumerGroupStatus(consumer_group)
				bt.getTopicStatuses(consumer_group)
			}
		}
	}
}

func (bt *Burrowbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Burrowbeat) getConsumerGroupStatus(burrow map[string]interface{}) {
	status := burrow["status"].(map[string]interface{})
	group := status["group"].(string)
	partitions := status["partitions"].([]interface{})
	for _, partition := range partitions {
		partition := partition.(map[string]interface{})
		offset := partition["end"].(map[string]interface{})

		consumer_group := common.MapStr{
			"name":      group,
			"topic":     partition["topic"].(string),
			"partition": int(partition["partition"].(float64)),
			"offset":    int64(offset["offset"].(float64)),
			"lag":       int64(offset["lag"].(float64)),
		}

		event := common.MapStr{
			"@timestamp":     common.Time(time.Now()),
			"type":           "consumer_group",
			"cluster":        bt.cluster,
			"consumer_group": consumer_group,
		}

		bt.client.PublishEvent(event)
	}
	logp.Info("Consumer group events sent")
}

func (bt *Burrowbeat) getTopicStatuses(burrow map[string]interface{}) {
	status := burrow["status"].(map[string]interface{})
	group := status["group"].(string)
	partitions := status["partitions"].([]interface{})

	var topic_names []string
	var topic_sizes, topic_partitions, topic_lags []int
	current_topic := 0

	for i, _ := range partitions {
		partition := partitions[i].(map[string]interface{})
		end := partition["end"].(map[string]interface{})
		tmp_name := partition["topic"].(string)
		tmp_offset := int(end["offset"].(float64))
		tmp_lag := int(end["lag"].(float64))

		if i == 0 {
			topic_names = append(topic_names, tmp_name)
			topic_sizes = append(topic_sizes, tmp_offset)
			topic_partitions = append(topic_partitions, 1)
			topic_lags = append(topic_lags, tmp_lag)
		} else {
			if strings.Compare(tmp_name, topic_names[len(topic_names)-1]) != 0 {
				topic_names = append(topic_names, tmp_name)
				topic_sizes = append(topic_sizes, tmp_offset)
				topic_partitions = append(topic_partitions, 1)
				topic_lags = append(topic_lags, tmp_lag)
				current_topic++
			} else {
				topic_sizes[current_topic] += tmp_offset
				topic_partitions[current_topic] += 1
				topic_lags[current_topic] += tmp_lag
			}
		}
	}

	for i, name := range topic_names {
		topic := common.MapStr{
			"name":       name,
			"size":       topic_sizes[i],
			"partitions": topic_partitions[i],
			"lag":        topic_lags[i],
		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "topic",
			"cluster":    bt.cluster,
			"group":      group,
			"topic":      topic,
		}
		bt.client.PublishEvent(event)
		logp.Info("Topic event sent")
	}
}
