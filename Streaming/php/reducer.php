<?php
$wordcount = array();
while(($myfile = fgets(STDIN)) != null)
{
	$file = trim($myfile);
	if(array_key_exists($file, $wordcount))
	{
		$count = $wordcount[$file];
		$count++;
		$wordcount[$file] = $count;
	}
	else
	{
		$wordcount[$file] = 1;
	}
}
foreach($wordcount as $key => $value)
{
	echo $key . "\t". $value . "\n";
}
?>
