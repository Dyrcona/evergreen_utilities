#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2012 Merrimack Valley Library Consortium
# Jason Stephenson <jstephenson@mvlc.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# ---------------------------------------------------------------

# This program will read a JSON query from a file passed in on the
# command line and output the results of the query as CSV data to
# standard output.  If you wish to capture the output, redirect it to
# a file.

use strict;
use warnings;

use OpenILS::Utils::Cronscript;
use Encode;
use JSON::XS;
use Text::CSV;

my $script = OpenILS::Utils::Cronscript->new({nolockfile=>1});

my $editor = $script->editor();

my $query = read_query($ARGV[0]) or die ("No query");

my $r = $editor->json_query($query);

if ($r && @$r) {
    my $count = 0;
    my $csv = Text::CSV->new({always_quote=>1});
    my @keys;
    foreach my $row (@$r) {
        if ($count == 0) {
            @keys = keys(%{$row});
            $csv->combine(@keys);
            print $csv->string . "\n";
        }
        my @vals = ();
        foreach my $key (@keys) {
            $row->{$key} =~ s/"?(.+?)"?$/$1/ if (defined($row->{$key}));
            push(@vals, $row->{$key});
        }
        $csv->combine(@vals);
        print $csv->string . "\n";
        $count++;
    }
} elsif (!defined($r)) {
    print_err($editor->event);
} else {
    print(STDERR "Query returned no results\n");
}

$editor->finish;

sub read_query {
    my $filename = shift;
    if (open(FILE, '<:utf8', $filename)) {
        my $content;
        while (my $line = <FILE>) {
            $content .= $line;
        }
        close(FILE);
        return decode_json($content);
    }
    return undef;
}

sub print_err {
    my $evt = shift;
    my @keys = keys(%{$evt});
    print(STDERR "An error occured:\n");
    foreach my $key (@keys) {
        printf(STDERR "\t%s => %s\n", $key, $evt->{$key});
    }
}
