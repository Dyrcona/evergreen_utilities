# ---------------------------------------------------------------
# Copyright © 2013 Merrimack Valley Library Consortium
# Jason Stephenson <jstephenson@mvlc.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# ---------------------------------------------------------------

# This guy parallelizes a reingest.
use strict;
use warnings;
use DBI;

# You will want to adjust the next two based on your database size,
# i.e. number of bib records as well as the number of cores on your
# database server.  Using roughly number of cores/2 doesn't seem to
# have much impact in off peak times.
use constant {
    BATCHSIZE => 10000,
    MAXCHILD => 8
};

# "Gimme the keys!  I'll drive!"
my $q = <<END_OF_Q;
SELECT id
FROM biblio.record_entry
WHERE deleted = 'f'
AND id > 0
ORDER BY id ASC
END_OF_Q

# Stuffs needed for looping, tracking how many lists of records we
# have, storing the actual list of records, and the list of the lists
# of records.
my ($count, $lists, $records) = (0,0,[]);
my @lol = ();
# To do the browse-only ingest:
my @blist = ();

# All of the DBI->connect() calls in this file assume that you have
# configured the PGHOST, PGPORT, PGDATABASE, PGUSER, and PGPASSWORD
# variables in your execution environment.  If you have not, you have
# two options:
#
# 1) configure them
#
# 2) edit the DBI->connec() calls in this program so that it can
# connect to your database.
my $dbh = DBI->connect('DBI:Pg:');

my $results = $dbh->selectall_arrayref($q);
foreach my $r (@$results) {
    my $record = $r->[0];
    push(@blist, $record); # separate list of browse-only ingest
    push(@$records, $record);
    if (++$count == BATCHSIZE) {
        $lol[$lists++] = $records;
        $count = 0;
        $records = [];
    }
}
$lol[$lists++] = $records if ($count); # Last batch is likely to be
                                       # small.
$dbh->disconnect();

# We're going to reuse $count to keep track of the total number of
# batches processed.
$count = 0;

# @running keeps track of the running child processes.
my @running = ();

# We start the browse-only ingest before starting the other ingests.
browse_ingest(@blist);

# We loop until we have processed all of the batches stored in @lol:
while ($count < $lists) {
    if (scalar(@lol) && scalar(@running) < MAXCHILD) {
        # Reuse $records for the lulz.
        $records = shift(@lol);
        reingest($records);
    } else {
        my $pid = wait();
        if (grep {$_ == $pid} @running) {
            @running = grep {$_ != $pid} @running;
            $count++;
            print "$count of $lists processed\n";
        }
    }
}

# Fork a child process to make the database calls on a list of
# records.
sub reingest {
    my $list = shift;
    my $pid = fork();
    if (!defined($pid)) {
        die "Failed to spawn a child";
    } elsif ($pid > 0) {
        push(@running, $pid);
    } elsif ($pid == 0) {
        my $dbh = DBI->connect('DBI:Pg:');
        my $sth = $dbh->prepare("SELECT metabib.reingest_metabib_field_entries(?, FALSE, TRUE, FALSE)");
        foreach (@$list) {
            if ($sth->execute($_)) {
                my $crap = $sth->fetchall_arrayref();
            } else {
                warn ("Select statement failed for record $_");
            }
        }
        $dbh->disconnect();
        exit(0);
    }
}

# This subroutine forks a process to do the browse-only ingest on the
# @blist above.  It cannot be parallelized, but can run in parrallel
# to the other ingests.
sub browse_ingest {
    my @list = @_;
    my $pid = fork();
    if (!defined($pid)) {
        die "failed to spawn child";
    } elsif ($pid > 0) {
        # Add our browser to the list of running children.
        push(@running, $pid);
        # Increment the number of lists, because this list was not
        # previously counted.
        $lists++;
    } elsif ($pid == 0) {
        my $dbh = DBI->connect('DBI:Pg:');
        my $sth = $dbh->prepare("SELECT metabib.reingest_metabib_field_entries(?, TRUE, FALSE, TRUE)");
        foreach (@list) {
            if ($sth->execute($_)) {
                my $crap = $sth->fetchall_arrayref();
            } else {
                warn ("Browse ingest failed for record $_");
            }
        }
        $dbh->disconnect();
        exit(0);
    }
}
