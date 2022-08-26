<?php
const ARGV_LIST = ['endtime:'];
const DEFAULT_ENDTIME = 2330;

/** Kafka parameters */
const BROKER_LIST   = '9.1.187.186:9092,9.1.187.187:9092,9.1.187.188:9092,9.1.187.189:9092,9.1.187.190:9092';
const GROUP_ID      = 'khw_kfk_4';
const KFK_USERNAME  = 'user3700';
const KFK_PASSWORD  = 'user3700_aBwHCUnb';
const TOPIC_LIST    = ['user3700'];
const SESS_TIME_OUT = '60000';
const BLOCK_TIME    = 5000;

/** Database parameters */
const DEFAULTCHARSET    = 'UTF8';
const DATABASE          = 'SDORACLE19C';
const DBUSERNAME        = 'C##ORCL200_CUSER';
const DBPASSWORD        = 'BLZ2lIzv3z';
const MSG_TABLE         = 'REALTIME_KAFKA';
const ORACLE_BASETIME   = 28800;

$arr_argv = getopt('', ARGV_LIST);

$datetimezone   = new DateTimeZone('Asia/Shanghai');
try {
    $datetimeObj = new DateTime('now', $datetimezone);
} catch (Exception $e) {
    echo $e->getMessage() . PHP_EOL;
    exit();
}
$endtime = array_key_exists('endtime', $arr_argv) ? min(2350, max(10, intval($arr_argv['endtime']))) : DEFAULT_ENDTIME;
if (intval($datetimeObj->format('Hi')) > $endtime) {
    echo 'It is break time now.' . PHP_EOL;
    exit();
}

/* Connect to database */
$conn = oci_pconnect(DBUSERNAME, DBPASSWORD, DATABASE, DEFAULTCHARSET);
$stmt = 'INSERT INTO ' . MSG_TABLE . ' VALUES (:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17)';
$stid1 = oci_parse($conn, $stmt);

$b_systemcode           = '';
$b_sendtime             = '';
$b_msgcode              = '';
$b_businessno           = '';
$b_comcode              = '';
$b_riskcode             = '';
$b_newchnltype          = '';
$b_underwriteedndate    = '';
$b_startdate            = '';
$b_enddate              = '';
$b_exchangerate         = '';
$b_coinsrate            = '';
$b_sumamount            = '';
$b_sumpremium           = '';
$b_shareagentfee        = '';
$b_handlercode          = '';
$b_handlername          = '';

oci_bind_by_name($stid1, ':p1',  $b_systemcode, 10);
oci_bind_by_name($stid1, ':p2',  $b_sendtime,23);
oci_bind_by_name($stid1, ':p3',  $b_msgcode, 20);
oci_bind_by_name($stid1, ':p4',  $b_businessno, 50);
oci_bind_by_name($stid1, ':p5',  $b_comcode, 10);
oci_bind_by_name($stid1, ':p6',  $b_riskcode,12);
oci_bind_by_name($stid1, ':p7',  $b_newchnltype, 16);
oci_bind_by_name($stid1, ':p8',  $b_underwriteedndate, 20);
oci_bind_by_name($stid1, ':p9',  $b_startdate, 20);
oci_bind_by_name($stid1, ':p10', $b_enddate, 20);
oci_bind_by_name($stid1, ':p11', $b_exchangerate, 11);
oci_bind_by_name($stid1, ':p12', $b_coinsrate, 7);
oci_bind_by_name($stid1, ':p13', $b_sumamount, 21);
oci_bind_by_name($stid1, ':p14', $b_sumpremium, 21);
oci_bind_by_name($stid1, ':p15', $b_shareagentfee, 21);
oci_bind_by_name($stid1, ':p16', $b_handlercode, 54);
oci_bind_by_name($stid1, ':p17', $b_handlername, 192);

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

$consumer       = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(TOPIC_LIST);

$arr_payload    = array();
$timestamp      = '';
$seconds        = '';
$milliseconds   = '';
while (true) {
    $message = $consumer->consume(BLOCK_TIME);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $timestamp              = strval($message->timestamp);
            $seconds                = substr($timestamp, 0, 10);
            $milliseconds           = substr($timestamp, -3, 3);
            $b_sendtime             = date('Y-m-d H:i:s', ORACLE_BASETIME + intval($seconds)) . ".$milliseconds";

            $arr_payload            = json_decode($message->payload, true);
            $b_systemcode           = $arr_payload['systemCode'];
            $b_msgcode              = $arr_payload['msgCode'];

            /* Payload data */
            $arr_payload            = $arr_payload['data'][0];

            $b_businessno           = $arr_payload['businessno'];
            $b_comcode              = $arr_payload['comcode'];
            $b_riskcode             = $arr_payload['RiskCode'];
            $b_newchnltype          = $arr_payload['newChnlType'];
            $b_underwriteedndate    = $arr_payload['UnderWriteEndDate'];
            $b_exchangerate         = $arr_payload['ExchangeRate'];
            $b_coinsrate            = $arr_payload['CoinsRate'];
            $b_handlercode          = $arr_payload['handlercode'];
            $b_handlername          = $arr_payload['handlername'];

            if ($b_msgcode != 'endorse') {
                $b_startdate        = $arr_payload['StartDate'];
                $b_enddate          = $arr_payload['EndDate'];
                $b_sumamount        = $arr_payload['SumAmount'];
                $b_sumpremium       = $arr_payload['SumPremium'];
                $b_shareagentfee    = $arr_payload['shareAgentFee'];
            } else {
                $b_startdate        = $arr_payload['ValidDate'];
                $b_enddate          = NULL;
                $b_sumamount        = $arr_payload['ChgAmount'];
                $b_sumpremium       = $arr_payload['ChgPremium'];
                $b_shareagentfee    = $arr_payload['chgShareAgentFee'];
            }

            if (!oci_execute($stid1)) {
                echo "$message->topic_name|$message->partition|$message->offset|$b_msgcode|$b_sendtime|$b_businessno  ... push fails \n";
                break 2;
            }

            $consumer->commit($message);
            echo "$message->topic_name|$message->partition|$message->offset|$b_msgcode|$b_sendtime|$b_businessno ... pushed \n";
            break;

        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            break;
        default:
            echo "Rd error:$message->err\n{$message->errstr()}\n";
            break 2;
    }

    /* Exit if it is time for a break */
    $datetimeObj->setTimestamp(time());
    if (intval($datetimeObj->format('Hi')) > $endtime) {
        echo 'Time for a break.' . PHP_EOL;
        break;
    }
}

oci_free_statement($stid1);
oci_close($conn);

$consumer->unsubscribe();
$consumer->close();
