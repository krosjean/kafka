<?php
$dti = new DateTime();
$dtz = new DateTimeZone('Asia/Shanghai');
$dti->setTimezone($dtz);
$dti->setTimestamp(1643385696541);

echo $dti->format('Y-m-d H:i:s').PHP_EOL;