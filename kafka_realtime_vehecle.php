<?php
const BROKER_LIST = '9.1.187.186:9092,9.1.187.187:9092,9.1.187.188:9092,9.1.187.189:9092,9.1.187.190:9092';
const GROUP_ID = 'khw_kfk_2';
const USER_NAME = 'user3700';
const PASSWORD = 'user3700_aBwHCUnb';
const TOPIC_NAME = 'user3700NewData';
const SESS_TIME_OUT = '60000';
const BLOCK_TIME    = 2000;

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', BROKER_LIST);
$conf->set('group.id', GROUP_ID);
$conf->set('enable.partition.eof', 'true');
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'largest');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'SCRAM-SHA-256');
$conf->set('sasl.username', USER_NAME);
$conf->set('sasl.password', PASSWORD);
$conf->set('session.timeout.ms', SESS_TIME_OUT);

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(array(TOPIC_NAME));

while (true) {
    $message = $consumer->consume(BLOCK_TIME);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            var_dump($message);
            $consumer->commit($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
           break;
        default:
            $consumer->unsubscribe(array(TOPIC_NAME));
            $consumer->close();
            throw new \Exception($message->errstr(), $message->err);
    }
}
