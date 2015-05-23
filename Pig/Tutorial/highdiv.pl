#!/usr/bin/perl

use strict;

while (<>) {
	my @fields = split;
	if ($fields[3] > 1.0) {
		print join("\t", @fields) . "\n"
	}
}
