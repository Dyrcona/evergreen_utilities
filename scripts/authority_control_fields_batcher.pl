#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2012 Merrimack Valley Library Consortium
# Jason Stephenson <jstephenson@mvlc.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# ---------------------------------------------------------------

# Outputs command lines for running authority_control_fields.pl over
# $batch_size numbers of bibs.  The output is printed to standard
# output and can be redirected to a file.  It is suitable for use with
# disbatcher.pl (available elsewhere).

# The default batch size is 10,000 records.  You can change this value
# by giving this program a numeric argument.  For instance if you want
# the batch size to be 1,000 records, you would run:
#
# authority_control_fields_batcher.pl 1000

use strict;
use warnings;
use DBI;
use JSONPrefs;

my $batch_size = 10000;

if ($ARGV[0] && int($ARGV[0])) {
    $batch_size = $ARGV[0];
}

my $egdbi = JSONPrefs->load($ENV{'HOME'} . "/myprefs.d/egdbi.json");

my $dsn = "dbi:Pg:database=" . $egdbi->database;

if ($egdbi->host) {
    $dsn .= ";host=" . $egdbi->host;
}

if ($egdbi->port) {
    $dsn .= ";port=" . $egdbi->port;
}

my $dbh = DBI->connect($dsn,$egdbi->user,$egdbi->password);

my $q = <<END_OF_Q;
SELECT id
FROM biblio.record_entry
WHERE deleted = 'f'
AND id > 0
AND (source IS NULL
     OR source IN (1,2))
ORDER BY id ASC
END_OF_Q

my $ids = $dbh->selectall_arrayref($q);
my ($start, $end, $count) = (0, 0, 0);
foreach (@$ids) {
    $count++;
    $end = $_->[0];
    if ($count == 1) {
        $start = $_->[0];
    }
    if ($count == $batch_size) {
        print_it($start, $end);
        $count = 0;
    }
}
if ($count) {
    print_it($start, $end);
}

sub print_it {
    my ($start, $end) = @_;
    print("/openils/bin/authority_control_fields.pl ");
    if ($start == $end) {
        printf("--record=%d\n", $start);
    } else {
        printf("--start_id=%d --end_id=%d\n", $start, $end);
    }
}
