<?php
$datetimezone = new DateTimeZone('Asia/Shanghai');
$datetimeObj = new datetime('now', $datetimezone);
echo $datetimeObj->format('Hi').PHP_EOL;

print_r($argv);

if ($argc > 1 && in_array('/f', $argv)) {
    echo 'option /f found' . PHP_EOL;
}