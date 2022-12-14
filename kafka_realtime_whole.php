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
const MSG_TABLE_MAP     = ['carpolicy'=>'REALTIME_KAFKA_CAR'];
const ORACLE_BASETIME   = 28800;

$conn = oci_pconnect(DBUSERNAME, DBPASSWORD, DATABASE, DEFAULTCHARSET);

$stmt = 'INSERT INTO REALTIME_KAFKA_CAR VALUES (:p1,:p2,:p3,:p4,:p5,:p6,:p7,:p8,:p9,:p10,:p11,:p12,:p13,:p14,:p15,:p16,:p17,:p18,:p19,:p20,:p21,:p22,:p23,:p24,:p25,:p26,:p27,:p28)';
$stid1 = oci_parse($conn, $stmt);

$b_systemcode       = '';
$b_sendtime         = '';
$b_msgcode          = '';
$b_policyno         = '';
$b_riskcode         = '';
$b_policysort       = '';
$b_underwriteedndate = '';
$b_newchnltype      = '';
$b_agentcode        = '';
$b_agentname        = '';
$b_startdate        = '';
$b_enddate          = '';
$b_businessnature   = '';
$b_carkindcode      = '';
$b_usenaturecode    = '';
$b_licenseno        = '';
$b_modelcode        = '';
$b_frameno          = '';
$b_comcode          = '';
$b_comname          = '';
$b_appliname        = '';
$b_insuredname      = '';
$b_newpolicyflag    = '';
$b_autotransrenewflag = '';
$b_transferpolicyflag = '';
$b_sumpremium       = 0.00;
$b_agentnetfee      = 0.00;
$b_artifselfpricesrat = 0.00;

$bind_result = true;
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p1',  $b_systemcode, 10);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p2',  $b_sendtime,20);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p3',  $b_msgcode, 20);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p4',  $b_policyno, 150);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p5',  $b_riskcode,150);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p5',  $b_policysort,150);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p6',  $b_underwriteedndate, 20);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p7',  $b_newchnltype, 192);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p8',  $b_agentcode, 30);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p9',  $b_agentname, 210);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p10', $b_startdate, 20);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p11', $b_enddate, 20);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p12', $b_businessnature, 10);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p13', $b_carkindcode, 4);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p14', $b_usenaturecode, 2);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p15', $b_licenseno, 96);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p16', $b_modelcode, 60);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p17', $b_frameno, 96);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p18', $b_comcode, 30);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p19', $b_comname, 384);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p20', $b_appliname, 384);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p21', $b_insuredname, 384);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p22', $b_newpolicyflag, 1);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p23', $b_autotransrenewflag, 1);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p24', $b_transferpolicyflag, 1);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p25', $b_sumpremium, 21);
$bind_result = $bind_result && oci_bind_by_name($stid1, ':p26', $b_artifselfpricesrat, 5);
if(!$bind_result) {
    oci_free_statement($stid1);
    oci_close($conn);
    echo "bind error\n";
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

$consumer       = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe(TOPIC_LIST);
$arr_payload    = array('');
$interval      = 0.000;
$i = 0;
while (true) {
    $message = $consumer->consume(BLOCK_TIME);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $arr_payload    = json_decode($message, true);

            $interval      = (int)$arr_payload['timestamp']/1000;
            $b_sendtime     = date('Y:m:d H:i:s', ORACLE_BASETIME+$interval);

            $b_systemcode   = $arr_payload['payload']['systemCode'];
            $b_msgcode      = $arr_payload['payload']['msgCode'];
            $b_policyno     = $arr_payload['payload']['policyNo'];
            $b_riskcode     = $arr_payload['payload']['riskCode'];
            $b_underwriteedndate = $arr_payload['payload']['underWriteEndDate'];
            $b_newchnltype  = $arr_payload['payload']['newChnlType'];
            $b_agentcode    = $arr_payload['payload']['agentComCode'];
            $b_agentname    = $arr_payload['payload']['agentComName'];

            $interval      = (int)$arr_payload['payload']['startDate']/1000;
            $b_startdate    = date('Y:m:d H:i:s', ORACLE_BASETIME+$interval);

            $interval      = (int)$arr_payload['payload']['endDate']/1000;
            $b_enddate      = date('Y:m:d H:i:s', ORACLE_BASETIME+$interval);

            $b_businessnature       = $arr_payload['payload']['businessNature'];
            $b_carkindcode          = $arr_payload['payload']['carKindCode'];
            $b_usenaturecode        = $arr_payload['payload']['useNatureCode'];
            $b_licenseno            = $arr_payload['payload']['licenseNo'];
            $b_modelcode            = $arr_payload['payload']['modelCode'];
            $b_frameno              = $arr_payload['payload']['frameNo'];
            $b_comcode              = $arr_payload['payload']['comCode'];
            $b_comname              = $arr_payload['payload']['comName'];
            $b_appliname            = $arr_payload['payload']['appliName'];
            $b_insuredname          = $arr_payload['payload']['insuredName'];
            $b_newpolicyflag        = $arr_payload['payload']['newPolicyFlag'];
            $b_autotransrenewflag   = $arr_payload['payload']['autoTransreNewFlag'];
            $b_transferpolicyflag   = $arr_payload['payload']['transferPolicyFlag'];
            $b_sumpremium           = $arr_payload['payload']['sumPremium'];
            $b_artifselfpricesrat   = $arr_payload['payload']['artifSelfPricesRat'];




            if (!oci_execute($stid1)) {
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

oci_free_statement($stid1);
oci_close($conn);
$consumer->unsubscribe();
$consumer->close();
