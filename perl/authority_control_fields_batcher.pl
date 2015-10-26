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
# by specifying the --batch-size (-b) option with a numeric argument.
# For instance, to run batches of 1,000 records you could use:
#
# authority_control_fields_batcher.pl -b 1000

# You can specify a lower bound.  This is an integer value that the
# bib retrieval will start at, so any batches will start at biblio
# record entries with an id greater than this value.  You specify this
# with the --lower-bound (-l) option:
#
# authority_control_fields_batcher.pl --lower-bound 1380695
#
# This option is useful if you ran some batches previously and want to
# pick up any bibs added since the last batch.  To do this, you'd
# specify the --end_id from the last line of your previous batch as
# the lower bound.
#
# The default lower bound is 0 to run over all of your regular biblio
# record entries.

# Naturally, the options can be combined.



use strict;
use warnings;
use DBI;
use Getopt::Long;

my $batch_size = 10000;
my $lower_bound = 0;

my $result = GetOptions("lower-bound=i" => \$lower_bound,
                        "batch-size=i" => \$batch_size);

my $dbh = DBI->connect('DBI:Pg:');

my $q = <<END_OF_Q;
SELECT id
FROM biblio.record_entry
WHERE deleted = 'f'
AND id > $lower_bound
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
# Catch the leftovers.
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
