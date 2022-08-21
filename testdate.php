<?php
//$dtz = new DateTimeZone('Asia/Shanghai');
//$dti = DateTimeImmutable::createFromFormat('Y-m-d H:i:s', '1970-01-01 08:00:00', $dtz);

$basetime = strtotime('1970-01-01 08:00:00');
$intervals = 1660214552501;
//$newtime = strtotime("+{$intervals} ms", $basetime);
$newtime = (int) ($basetime + $intervals/1000);
echo $basetime.PHP_EOL.$intervals.PHP_EOL.date('Y:m:d H:i:s', $newtime).PHP_EOL;

//echo $dti->format('Y-m-d H:i:s').PHP_EOL;