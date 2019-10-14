package messaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"math"
	"strconv"
	"time"
)

type kafkaTestSuite struct {
	suite.Suite
	hosts                       []string
	topic                       string
	numPartitions               int32
	capturedMessagesByPartition map[int32][]*sarama.ConsumerMessage
	capturedMessagesLast        map[string]*sarama.ConsumerMessage // Last message captured with a given key

	groupId             string
	groupConsumerClient *cluster.Client
	groupConsumer       *cluster.Consumer
}

func (t *kafkaTestSuite) SetupSuite() {
	t.hosts = []string{"kafka1:9092", "kafka2:9092"}
	t.topic = "webstream_adapter_test_topic"
	t.numPartitions = 5
	var err error
	err = t.createTopic(t.hosts, t.topic, t.numPartitions)
	if err != nil {
		assert.Contains(t.T(), err.Error(), "Topic with this name already exists")
	}

	t.capturedMessagesByPartition = make(map[int32][]*sarama.ConsumerMessage)
	t.capturedMessagesLast = make(map[string]*sarama.ConsumerMessage)
	_, clusterConfig := GetDefaultConfig()
	t.groupConsumerClient, err = cluster.NewClient(t.hosts, clusterConfig)
	assert.Nil(t.T(), err)
	t.groupId = "cg_" + t.topic + strconv.Itoa(time.Now().Second())
	t.groupConsumer, err = cluster.NewConsumerFromClient(t.groupConsumerClient, t.groupId, []string{t.topic})
	assert.Nil(t.T(), err)
	t.runKafkaMessageCapture()
}

func (t *kafkaTestSuite) SetupTest() {
	t.capturedMessagesByPartition = make(map[int32][]*sarama.ConsumerMessage)
	t.capturedMessagesLast = make(map[string]*sarama.ConsumerMessage)
}

func (t *kafkaTestSuite) TearDownSuite() {
	t.groupConsumer.Close()
	t.groupConsumerClient.Close()
}

func (t *kafkaTestSuite) TestNewKafkaClient() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	defer client.Close()
	assert.Nil(t.T(), err)
	assert.IsType(t.T(), new(kafkaClient), client)
}

type keyValue struct {
	key   string
	value []byte
}

func (t *kafkaTestSuite) TestProduceRandom_MessagesWithSameKeyFoundInVariousPartitions() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	defer client.Close()
	assert.Nil(t.T(), err)
	var keyValues []keyValue
	// 300 messages with 4 different keys (key_0, key_1, key_2, key_3)
	const numMessages = 300
	const numKeys = 4
	const numMessagesPerKey = numMessages / numKeys // 50 messages
	// For 5 partitions ideally 10 messages for each key would end one in each partition.
	//expectedNumMessagesPerKeyPerPartition := numMessagesPerKey / t.numPartitions
	//maxVariance := expectedNumMessagesPerKeyPerPartition / 2
	for k := 0; k < numKeys; k++ {
		key := "key_" + strconv.Itoa(k)
		for m := 0; m < numMessagesPerKey; m++ {
			value := []byte("value_" + strconv.Itoa(m))
			keyValues = append(keyValues, keyValue{key, value})
			err, _, _ := client.ProduceRandom(context.Background(), t.topic, key, value)
			assert.Nil(t.T(), err)
			//printf("Produced message: %s", key)
		}
	}
	time.Sleep(time.Millisecond * 500)

	obtainedDistribution := make(map[string]map[int32]int32) // key -> partition -> count
	for partition, messages := range t.capturedMessagesByPartition {
		for _, message := range messages {
			_, ok := obtainedDistribution[string(message.Key)]
			if !ok {
				obtainedDistribution[string(message.Key)] = make(map[int32]int32)
			}
			obtainedDistribution[string(message.Key)][partition]++
		}
	}

	for key, partitions := range obtainedDistribution {
		partitionDistribution := []float64{}
		for partitionIdx, count := range partitions {
			printf("Key: %s, Partition: %d, Count: %d", key, partitionIdx, count)
			partitionDistribution = append(partitionDistribution, float64(count))
		}
		distribution := normalize(partitionDistribution)
		entropy := normalizedEntropy(distribution)
		printf("Key: %s, Entropy: %f", key, entropy)
		assert.True(t.T(), entropy > 0.8)
	}
}

func (t *kafkaTestSuite) TestProduce_MessagesWithSameKeyFoundInSamePartition() {
	config := KafkaClientConfig{
		Brokers:            []string{"kafka1:9092", "kafka2:9092"},
		ClientIDPrefix:     "clientIdPrefix",
		ProducerTimeout:    "2s",
		ProducerMaxRetries: 3,
		Debug:              true,
	}
	client, err := NewKafkaClient(config)
	defer client.Close()
	assert.Nil(t.T(), err)
	var keyValues []keyValue
	// 20 messages with 4 different keys (key_0, key_1, key_2, key_3)
	const numMessages = 20
	const numKeys = 4
	const numMessagesPerKey = numMessages / numKeys
	for i := 0; i < numMessages; i++ {
		keyValues = append(keyValues, keyValue{"key_" + strconv.Itoa(i%4), []byte("value_" + strconv.Itoa(i))})
	}
	for _, pair := range keyValues {
		err, _, _ := client.ProduceHash(context.Background(), t.topic, pair.key, pair.value)
		assert.Nil(t.T(), err)
		printf("Produced message: %s", pair.key)
	}
	time.Sleep(time.Millisecond * 500)

	for key, message := range t.capturedMessagesLast {
		partitionMessages := t.capturedMessagesByPartition[message.Partition]
		count := 0
		for _, partitionMessage := range partitionMessages {
			if string(partitionMessage.Key) == key {
				count++
			}
		}
		assert.Equal(t.T(), numMessagesPerKey, count, fmt.Sprintf("There should be 4 messages with key %s in partition %d", key, message.Partition))
	}
}

func (k *kafkaTestSuite) runKafkaMessageCapture() {
	go func() {
		defer k.groupConsumer.Close()
		for {
			select {
			case msg, ok := <-k.groupConsumer.Messages():
				if ok {
					//printf("Received message: %s", msg.Key)
					k.capturedMessagesByPartition[msg.Partition] = append(k.capturedMessagesByPartition[msg.Partition], msg)
					k.capturedMessagesLast[string(msg.Key)] = msg
				} else {
					return
				}
			case err, ok := <-k.groupConsumer.Errors():
				if ok {
					printf("Kafka error: %s", err.Error())
				} else {
					return
				}
				return
			case note, ok := <-k.groupConsumer.Notifications():
				if ok {
					printf("%v: Kafka notification: %+v", time.Now().Format(time.StampMilli), note)
				} else {
					return
				}
			}
		}
	}()
	time.Sleep(time.Second * 5) // Wait for partition acquisition
}

func (m *kafkaTestSuite) createTopic(hosts []string, topic string, numPartitions int32) error {
	var err error
	c, _ := GetDefaultConfig()
	client, err := sarama.NewClient(hosts, c)
	if err != nil {
		return err
	}
	controllerBroker, err := client.Controller()
	if err != nil {
		return err
	}
	t := &sarama.CreateTopicsRequest{}
	t.Timeout = time.Second * 5
	t.Version = 2
	t.TopicDetails = make(map[string]*sarama.TopicDetail)
	t.TopicDetails[topic] = &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	}
	res, err := controllerBroker.CreateTopics(t)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, err := range res.TopicErrors {
		if err.Err != sarama.ErrNoError {
			buf.WriteString(err.Err.Error() + ",")
		}
	}
	if buf.Len() > 0 {
		err = errors.New(buf.String())
		return err
	}
	return nil
}

func GetDefaultConfig() (*sarama.Config, *cluster.Config) {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_0_0
	conf.ClientID = "webstream_adapter"
	conf.Metadata.Retry.Max = 5
	conf.Metadata.Retry.Backoff = 1 * time.Second
	conf.Consumer.Return.Errors = true
	conf.Consumer.Retry.Backoff = 1 * time.Second
	conf.Consumer.Offsets.Retry.Max = 5
	conf.Admin.Timeout = 30 * time.Second // Not used
	clusConf := cluster.NewConfig()
	clusConf.Group.Return.Notifications = true
	clusConf.Group.Mode = cluster.ConsumerModeMultiplex
	clusConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusConf.Config = *conf
	return conf, clusConf
}

func printf(format string, a ...interface{}) {
	str := fmt.Sprintf(format, a...)
	fmt.Printf("%s: %s\n", time.Now().Format(time.StampMilli), str)

}

func normalize(d []float64) []float64 {
	sum := 0.0
	for _, v := range d {
		sum += v
	}
	r := make([]float64, len(d))
	for i, v := range d {
		r[i] = v / sum
	}
	return r
}

func normalizedEntropy(d []float64) float64 {
	var r float64
	for _, v := range d {
		if v != 0 {
			r -= v * math.Log(v)
		}
	}
	logN := math.Log(float64(len(d)))
	return r / logN
}
