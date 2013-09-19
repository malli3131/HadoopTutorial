#!/usr/bin/perl

@words = <STDIN>;
%wordcount = {};

foreach $word(@words)
{
chomp($word);
	if (exists $wordcount{$word})
	{
		$count = $wordcount{$word};
		$count++;
		$wordcount{$word} = $count;
	}
	else
	{
		$wordcount{$word} = 1;
	}
}
while(($key, $value) = each %wordcount)
{
	print $key, "\t", $value, "\n";
}
