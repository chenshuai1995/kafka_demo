package com.jim.demo;

import kafka.coordinator.group.GroupOverview;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.*;
import java.util.stream.Collectors;
//import scala.collection.Iterator;
//import scala.collection.immutable.List;

/**
 * 统计topic在磁盘上占用大小
 * kafka版本>1.0
 *
 * @author Jim Chen
 * @date 2021-10-13
 */
public class TopicDiskSizeSummaryAll {

    private static AdminClient admin;
    private static kafka.admin.AdminClient kafkaAdminClient;

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            System.out.println("请指定broker server list, usage: java -jar xxx.jar localhost:6667");
        }
        // topic -> replica,retention(h),logSize(gb),messageCount
        Map<String, String> result = new HashMap<>();

//        String brokers = "wx12-test-hadoop001:6667";
        String brokers = args[0];
        initialize(brokers);

        try {
            List<Integer> nodeIds = getClusterNodeIds();
            List<String> topics = getTopics();

            for (String topic : topics) {
                int replicas = getReplicas(topic);
                String retention = getConfig(topic, "retention.ms");
                long hour = Long.valueOf(retention) / 1000 / 3600;
                long size = getTopicDiskSizeForAllBroker(topic, nodeIds);
                long gb = size / 1024 / 1024 / 1024;

                long messageCount = getMessageCount(topic, brokers);

                Set<String> groups = getConsumers(topic);

                String value = replicas + "," + hour + "," + gb + "," + messageCount + "," + groups.size();
                result.put(topic, value);
            }
            System.out.println("topic -> replica,retention(h),logSize(gb),messageCount,groupSize,");
            System.out.println("========");
            System.out.println(result);
            System.out.println("========");
        } finally {
            shutdown();
        }
    }

    private static void getLastupdateTime(String topic) {

    }

    /**
     * 获取消费者组
     * @param topic
     */
    private static Set<String> getConsumers(String topic) {
        Set<String> groups = new HashSet<>();
        List<GroupOverview> listAllGroupsFlattened = scala.collection.JavaConversions.seqAsJavaList(kafkaAdminClient.listAllGroupsFlattened().toSeq());
        if (listAllGroupsFlattened != null && listAllGroupsFlattened.size() > 0) {
            for (GroupOverview overview : listAllGroupsFlattened) {
                String groupID = overview.groupId();
                Map<TopicPartition, Object> offsets = scala.collection.JavaConversions.mapAsJavaMap(kafkaAdminClient.listGroupOffsets(groupID));
                Set<TopicPartition> partitions = offsets.keySet();
                for (TopicPartition tp: partitions) {
                    if (tp.topic().equals(topic)) {
                        groups.add(groupID);
                    }
                }
            }
        }
        return groups;
    }

    /**
     * 获取topic中有多少记录数
     * @param topic
     * @param brokerList
     * @return
     */
    private static long getMessageCount(String topic, String brokerList) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", "test-group");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> tps = Optional.ofNullable(consumer.partitionsFor(topic))
                    .orElse(Collections.emptyList())
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());
            Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

            return tps.stream().mapToLong(tp -> endOffsets.get(tp) - beginOffsets.get(tp)).sum();
        }
    }

    /**
     * 获取topic的副本数
     * @param topic
     * @return
     * @throws Exception
     */
    private static int getReplicas(String topic) throws Exception {
        int replicas = -1;
        DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singletonList(topic));
        KafkaFuture<Map<String, TopicDescription>> all = describeTopicsResult.all();
        Map<String, TopicDescription> topicDescriptionMap = all.get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            TopicDescription topicDescription = entry.getValue();
            replicas = topicDescription.partitions().get(0).replicas().size();
        }
        return replicas;
    }

    /**
     * 获取topic级别的配置
     * @param topic topic名称
     * @param configName config配置名称
     * @return
     * @throws Exception
     */
    private static String getConfig(String topic, String configName) throws Exception {
        String result = null;

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singletonList(configResource));
        KafkaFuture<Map<ConfigResource, Config>> all = describeConfigsResult.all();
        Map<ConfigResource, Config> configResourceConfigMap = all.get();
        for (Map.Entry<ConfigResource, Config> entry : configResourceConfigMap.entrySet()) {
            Config config = entry.getValue();
            ConfigEntry configEntry = config.get(configName);
            result = configEntry.value();
        }
        return result;

    }

    /**
     * 获取所有的topic
     * @return
     * @throws Exception
     */
    private static List<String> getTopics() throws Exception {
        List<String> topics = new ArrayList<String>();
        ListTopicsResult listTopicsResult = admin.listTopics();
        KafkaFuture<Collection<TopicListing>> listings = listTopicsResult.listings();
        Collection<TopicListing> topicListings = listings.get();
        for (TopicListing topicListing : topicListings) {
            topics.add(topicListing.name());
        }
        return topics;
    }

    /**
     * 获取集群节点
     * @return
     * @throws Exception
     */
    private static Collection<Node> getClusterNodes() throws Exception {
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        return describeClusterResult.nodes().get();
    }

    /**
     * 获取集群节点ID
     * @return
     * @throws Exception
     */
    private static List<Integer> getClusterNodeIds() throws Exception {
        List<Integer> nodeIds = new ArrayList<Integer>();
        DescribeClusterResult describeClusterResult = admin.describeCluster();
        Collection<Node> nodes = describeClusterResult.nodes().get();
        for (Node node : nodes) {
            nodeIds.add(node.id());
        }

        return nodeIds;
    }

    /**
     * 获取topic在broker集群中的bytes数量
     * @param topic
     * @param brokerIDs
     * @return
     * @throws Exception
     */
    public static long getTopicDiskSizeForAllBroker(String topic, List<Integer> brokerIDs) throws Exception {
        long sum = 0;
        for (Integer brokerID : brokerIDs) {
            sum = sum + getTopicDiskSizeForSomeBroker(topic, brokerID);
        }
        return sum;
    }

    /**
     * 获取topic在某个broker节点上的bytes数量
     * @param topic
     * @param brokerID
     * @return
     * @throws Exception
     */
    public static long getTopicDiskSizeForSomeBroker(String topic, int brokerID)
            throws Exception {
        long sum = 0;
        DescribeLogDirsResult ret = admin.describeLogDirs(Collections.singletonList(brokerID));
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> tmp = ret.all().get();
        for (Map.Entry<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> entry : tmp.entrySet()) {
            Map<String, DescribeLogDirsResponse.LogDirInfo> tmp1 = entry.getValue();
            for (Map.Entry<String, DescribeLogDirsResponse.LogDirInfo> entry1 : tmp1.entrySet()) {
                DescribeLogDirsResponse.LogDirInfo info = entry1.getValue();
                Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicaInfoMap = info.replicaInfos;
                for (Map.Entry<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> replicas : replicaInfoMap.entrySet()) {
                    if (topic.equals(replicas.getKey().topic())) {
                        sum += replicas.getValue().size;
                    }
                }
            }
        }
        return sum;
    }

    /**
     * 初始化adminClient
     * @param bootstrapServers
     */
    private static void initialize(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        admin = AdminClient.create(props);
        kafkaAdminClient = kafka.admin.AdminClient.createSimplePlaintext(bootstrapServers);
    }

    /**
     * 关闭连接
     */
    private static void shutdown() {
        if (admin != null) {
            admin.close();
        }
    }

}
