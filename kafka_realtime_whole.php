<?php
const BROKER_LIST   = '9.1.187.186:9092,9.1.187.187:9092,9.1.187.188:9092,9.1.187.189:9092,9.1.187.190:9092';
const GROUP_ID      = 'khw_kfk_3';
const KFK_USERNAME  = 'user3700';
const KFK_PASSWORD  = 'user3700_aBwHCUnb';
const TOPIC_LIST    = ['user3700NewData'];
const SESS_TIME_OUT = '60000';
const BLOCK_TIME    = 5000;

const DEFAULTCHARSET    = 'UTF8';
const DATABASE          = 'SDORACLE19C';
const DBUSERNAME        = 'C##ORCL200_CUSER';
const DBPASSWORD        = 'BLZ2lIzv3z';
const KFKRAWTABLE       = 'REALTIME_KFK_RAW';
const MAXPAYLOADLEN     = 2000;

$conn = oci_pconnect(DBUSERNAME, DBPASSWORD, DATABASE, DEFAULTCHARSET);
$stmt = 'INSERT INTO ' . KFKRAWTABLE . ' (MESSAGE) VALUES (:p)';
$stid = oci_parse($conn, $stmt);
$payload = '';
if (!oci_bind_by_name($stid, ':p', $payload, MAXPAYLOADLEN)) {
    oci_free_statement($stid);
    oci_close($conn);
    echo "Bind error.\n";
    exit(0);
}

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', BROKER_LIST);
$conf->set('group.id', GROUP_ID);
$conf->set('enable.partition.eof', 'true');
$conf->set('enable.auto.commit', 'false');
$conf->set('auto.offset.reset', 'largest');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'SCRAM-SHA-256');
$conf->set('sasl.username', KFK_USERNAME);
$conf->set('sasl.password', KFK_PASSWORD);
$conf->set('session.timeout.ms', SESS_TIME_OUT);

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(TOPIC_LIST);
$i = 0;
while (true) {
    $message = $consumer->consume(BLOCK_TIME);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $payload = $message->payload;
            if (!oci_execute($stid)) {
                echo "insert error\n";
                break 2;
            }
            $consumer->commit($message);
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            break;
        default:
            echo "Rd error:{$message->err}\n{$message->errstr()}\n";
            break 2;
    }
    if (++$i == 5) {
        break;
    }
}

oci_free_statement($stid);
oci_close($conn);
$consumer->unsubscribe();
$consumer->close();
