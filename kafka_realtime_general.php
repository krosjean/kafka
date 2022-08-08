<?php
const BROKER_LIST = '9.1.187.186:9092,9.1.187.187:9092,9.1.187.188:9092,9.1.187.189:9092,9.1.187.190:9092';
const GROUP_ID = 'khw_kfk_1';
const USER_NAME = 'user3700';
const PASSWORD = 'user3700_aBwHCUnb';
const SESS_TIME_OUT = '10000';

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', BROKER_LIST);
$conf->set('group.id', GROUP_ID);
$conf->set('enable.partition.eof', 'true');
$conf->set('enable.auto.commit', 'false');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'SCRAM-SHA-256');
$conf->set('sasl.username', USER_NAME);
$conf->set('sasl.password', PASSWORD);
$conf->set('session.timeout.ms', SESS_TIME_OUT);
$rk = new RdKafka\Consumer($conf);
$rk->addBrokers(BROKER_LIST);

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.offset.reset', 'latest');
$topic = $rk->newTopic(USER_NAME, $topicConf);
$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

$message = $topic->consume(0, 200);
var_dump( $message );

$topic->consumeStop(0);











