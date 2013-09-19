<?php
while(($file = fgets(STDIN)) != null)
{
	$words = explode(" ", $file);
	foreach($words as $word)
	{
		echo $word . "\n";
	}
}
?>
