<?php
/** KAFKA 实时消息处理-全险种承保   build 202209  khwarezmi    last changed  20220904 14:20 
 ** based on edenhill/librdkafka 1.6.2
**/

const ARGV_LIST = ['endtime:'];
const DEFAULT_ENDTIME = 2330;

/** Kafka parameters */
const BROKER_LIST   = '9.1.187.186:9092,9.1.187.187:9092,9.1.187.188:9092,9.1.187.189:9092,9.1.187.190:9092';
const GROUP_ID      = 'khw_kfk_12';
const KFK_USERNAME  = 'user3700';
const KFK_PASSWORD  = 'user3700_aBwHCUnb';
const TOPIC_LIST    = ['user3700NewData'];
const SESS_TIME_OUT = '60000';
const BLOCK_TIME    = 60000;
const MAX_POLL_INTERVAL_MS = 3600 * 2 * 1000;


/** Database parameters */
const DEFAULTCHARSET    = 'UTF8';
const DATABASE          = 'SDORACLE19C';
const DBUSERNAME        = 'C##ORCL200_CUSER';
const DBPASSWORD        = 'BLZ2lIzv3z';
const ORACLE_BASETIME   = 28800;
$msg_table        		= 'REALTIME_KAFKA_CAR';

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

$logfile    = $_SERVER['TEMP'] . '\\' . basename(__FILE__, '.php') . '.log';
$filehandle = fopen($logfile, 'a');
fwrite($filehandle, $datetimeObj->format('Y-m-d H:i:s') . ' started' . PHP_EOL, 50);
fclose($filehandle);

/* Connect to database */
$conn = oci_pconnect(DBUSERNAME, DBPASSWORD, DATABASE, DEFAULTCHARSET);
$stmt = "INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX($msg_table, realtimekafkacar1) APPEND_VALUES */ INTO  $msg_table VALUES (SEQ_KAFKA_CAR.nextval,:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17,:p18,:p19,:p20,:p21,:p22,:p23,:p24,:p25,:p26,:p27,:p28, current_timestamp)";
$stid1 = oci_parse($conn, $stmt);

$b_systemcode           = '';
$b_sendtime             = '';
$b_msgcode              = '';
$b_policyno             = '';
$b_riskcode             = '';
$b_policysort           = '';
$b_underwriteenddate    = '';
$b_newchnltype          = '';
$b_agentcode            = '';
$b_agentname            = '';
$b_startdate            = '';
$b_enddate              = '';
$b_businessnature       = '';
$b_carkindcode          = '';
$b_usenaturecode        = '';
$b_licenseno            = '';
$b_modelcode            = '';
$b_frameno              = '';
$b_comcode              = '';
$b_comname              = '';
$b_appliname            = '';
$b_insuredname          = '';
$b_newpolicyflag        = '';
$b_autotransrenewflag   = '';
$b_transferpolicyflag   = '';
$b_sumpremium           = '';
$b_agentnetfee          = '';
$b_artifselfpricesrat   = '';

oci_bind_by_name($stid1, ':p1',  $b_systemcode, 10);
oci_bind_by_name($stid1, ':p2',  $b_sendtime,23);
oci_bind_by_name($stid1, ':p3',  $b_msgcode, 20);
oci_bind_by_name($stid1, ':p4',  $b_policyno, 50);
oci_bind_by_name($stid1, ':p5',  $b_riskcode,50);
oci_bind_by_name($stid1, ':p6',  $b_policysort,10);
oci_bind_by_name($stid1, ':p7',  $b_underwriteenddate, 20);
oci_bind_by_name($stid1, ':p8',  $b_newchnltype, 16);
oci_bind_by_name($stid1, ':p9',  $b_agentcode, 30);
oci_bind_by_name($stid1, ':p10', $b_agentname, 210);
oci_bind_by_name($stid1, ':p11', $b_startdate, 20);
oci_bind_by_name($stid1, ':p12', $b_enddate, 20);
oci_bind_by_name($stid1, ':p13', $b_businessnature, 10);
oci_bind_by_name($stid1, ':p14', $b_carkindcode, 4);
oci_bind_by_name($stid1, ':p15', $b_usenaturecode, 2);
oci_bind_by_name($stid1, ':p16', $b_licenseno, 32);
oci_bind_by_name($stid1, ':p17', $b_modelcode, 20);
oci_bind_by_name($stid1, ':p18', $b_frameno, 32);
oci_bind_by_name($stid1, ':p19', $b_comcode, 10);
oci_bind_by_name($stid1, ':p20', $b_comname, 384);
oci_bind_by_name($stid1, ':p21', $b_appliname, 384);
oci_bind_by_name($stid1, ':p22', $b_insuredname, 384);
oci_bind_by_name($stid1, ':p23', $b_newpolicyflag, 1);
oci_bind_by_name($stid1, ':p24', $b_autotransrenewflag, 1);
oci_bind_by_name($stid1, ':p25', $b_transferpolicyflag, 1);
oci_bind_by_name($stid1, ':p26', $b_sumpremium, 21);
oci_bind_by_name($stid1, ':p27', $b_agentnetfee, 21);
oci_bind_by_name($stid1, ':p28', $b_artifselfpricesrat, 5);

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
$conf->set('max.poll.interval.ms', MAX_POLL_INTERVAL_MS);

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

            $b_policyno             = $arr_payload['policyNo'];
            $b_policysort           = $arr_payload['policySort'];
            $b_riskcode             = $arr_payload['riskCode'];

            $seconds                = substr($arr_payload['underWriteEndDate'], 0, 10);
            $b_underwriteedndate    = date('Y-m-d H:i:s', ORACLE_BASETIME + intval($seconds));

            $b_newchnltype          = $arr_payload['newChnlType'];
            $b_agentcode            = $arr_payload['agentComCode'];
            $b_agentname            = $arr_payload['agentComName'];

            $seconds                = substr($arr_payload['startDate'], 0, 10);
            $b_startdate            = date('Y-m-d H:i:s', ORACLE_BASETIME + intval($seconds));

            $seconds                = substr($arr_payload['endDate'], 0, 10);
            $b_enddate              = date('Y-m-d H:i:s', ORACLE_BASETIME + intval($seconds));

            $b_businessnature       = $arr_payload['businessNature'];
            $b_carkindcode          = $arr_payload['carKindCode'];
            $b_usenaturecode        = array_key_exists('useNatureCode', $arr_payload) ? $arr_payload['useNatureCode'] : NULL;
            $b_licenseno            = $arr_payload['licenseNo'];
            $b_modelcode            = $arr_payload['modelCode'];
            $b_frameno              = $arr_payload['frameNo'];
            $b_comcode              = $arr_payload['comCode'];
            $b_comname              = $arr_payload['comName'];
            $b_appliname            = $arr_payload['appliName'];
            $b_insuredname          = $arr_payload['insuredName'];
            $b_newpolicyflag        = $arr_payload['newPolicyFlag'];
            $b_autotransrenewflag   = $arr_payload['autoTransreNewFlag'];
            $b_transferpolicyflag   = $arr_payload['transferPolicyFlag'];
            $b_sumpremium           = $arr_payload['sumPremium'];
            $b_agentnetfee          = $arr_payload['agentNetFee'];
            $b_artifselfpricesrat   = $arr_payload['artifSelfPricesRat'];

            if (!oci_execute($stid1)) {
                echo "$message->topic_name|$message->partition|$message->offset|$b_msgcode|$b_sendtime|$b_policyno  ... push fails \n";
                break 2;
            }

            $consumer->commit($message);
            echo "$message->topic_name|$message->partition|$message->offset|$b_msgcode|$b_sendtime|$b_policyno ... pushed \n";
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

$filehandle = fopen($logfile, 'a');
fwrite($filehandle, $datetimeObj->format('Y-m-d H:i:s') . ' terminated' . PHP_EOL, 50);
fclose($filehandle);
