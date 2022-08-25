<?php
$datetimezone = new DateTimeZone('Asia/Shanghai');
$datetimeObj = new datetime('now', $datetimezone);
echo $datetimeObj->format('Hi').PHP_EOL;

print_r(getopt('', ['endtime:']));
