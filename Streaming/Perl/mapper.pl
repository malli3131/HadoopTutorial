#!/usr/bin/perl

@lines = <STDIN>;

foreach $line(@lines)
{
	@words = split(/\s/, $line);
	foreach $word(@words)
	{
		print $word, "\n";
	}
}
