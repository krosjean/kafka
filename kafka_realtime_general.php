<?php
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

$conn = oci_pconnect(DBUSERNAME, DBPASSWORD, DATABASE, DEFAULTCHARSET);

$stmt = 'INSERT INTO ' . MSG_TABLE . ' VALUES (:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16)';
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

oci_bind_by_name($stid1, ':p1',  $b_systemcode, 10);
oci_bind_by_name($stid1, ':p2',  $b_sendtime,20);
oci_bind_by_name($stid1, ':p3',  $b_msgcode, 20);
oci_bind_by_name($stid1, ':p4',  $b_businessno, 50);
oci_bind_by_name($stid1, ':p5',  $b_comcode, 10);
oci_bind_by_name($stid1, ':p6',  $b_riskcode,12);
oci_bind_by_name($stid1, ':p8',  $b_newchnltype, 192);
oci_bind_by_name($stid1, ':p9',  $b_underwriteedndate, 20);
oci_bind_by_name($stid1, ':p10', $b_startdate, 20);
oci_bind_by_name($stid1, ':p11', $b_enddate, 20);
oci_bind_by_name($stid1, ':p12', $b_exchangerate, 11);
oci_bind_by_name($stid1, ':p13', $b_coinsrate, 7);
oci_bind_by_name($stid1, ':p14', $b_sumamount, 21);
oci_bind_by_name($stid1, ':p15', $b_sumpremium, 21);
oci_bind_by_name($stid1, ':p16', $b_shareagentfee, 21);
oci_bind_by_name($stid1, ':p17', $b_handlercode, 5);

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
$interval       = 0;
$i = 0;
while (true) {
    $message = $consumer->consume(BLOCK_TIME);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $interval               = (int) ($message->timestamp/1000);
            $b_sendtime             = date('Y-m-d H:i:s', ORACLE_BASETIME + $interval);

            $arr_payload            = json_decode($message->payload, true);
            $b_systemcode           = $arr_payload['systemCode'];
            $b_msgcode              = $arr_payload['msgCode'];

            /* Payload data */
            $arr_payload            = $arr_payload['data'][0];

            $b_policyno             = $arr_payload['policyNo'];
            $b_policysort           = $arr_payload['policySort'];
            $b_riskcode             = $arr_payload['riskCode'];

            $interval               = (int) ((int) $arr_payload['underWriteEndDate'] / 1000);
            $b_underwriteedndate    = date('Y-m-d', ORACLE_BASETIME + $interval);

            $b_newchnltype          = $arr_payload['newChnlType'];
            $b_agentcode            = $arr_payload['agentComCode'];
            $b_agentname            = $arr_payload['agentComName'];

            $interval               = (int) ((int) $arr_payload['startDate'] / 1000);
            $b_startdate            = date('Y-m-d', ORACLE_BASETIME + $interval);

            $interval               = (int) ((int) $arr_payload['endDate'] / 1000);
            $b_enddate              = date('Y-m-d', ORACLE_BASETIME + $interval);

            $b_businessnature       = $arr_payload['businessNature'];
            $b_carkindcode          = $arr_payload['carKindCode'];
            $b_usenaturecode        = $arr_payload['useNatureCode'];
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
                echo $b_sendtime.PHP_EOL.$b_startdate.PHP_EOL.$b_enddate.PHP_EOL;
                echo "insert error\n";
                break 2;
            }
            $consumer->commit($message);
            echo "$b_msgcode | $b_sendtime | $b_policyno ... pushed \n";
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

oci_free_statement($stid1);
oci_close($conn);
$consumer->unsubscribe();
$consumer->close();
