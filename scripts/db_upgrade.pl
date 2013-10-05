#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2013 Merrimack Valley Library Consortium
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

# This script can be used to run the database upgrade scripts for an
# Evergreen system running the master branch from git.  It will lookup
# the highest numbered upgrade script that has been run against your
# database and will run all scripts of a higher version number.  You
# can optionally specify to run all scripts that begin with XXXX by
# using an option of -X or --XXXX.  Any other option will be
# interpreted as a directory where your git checkout of Evergreen
# lives.  If you do not specify this option, then ~/Evergreen/ will be
# assumed.

# This script also assumes that you have set the PG* environment
# variables for running PostgreSQL programs against your target
# database.  Unlike many of the other scripts in this repository, it
# does not rely on the JSONPrefs module.

use strict;
use feature 'switch';

my $XXXX = 0;
my $time = 0;
my $count = 0;
my $src_dir = $ENV{HOME} . "/Evergreen/";

foreach my $arg (@ARGV) {
    given ($arg) {
        when (/^-{1,2}X{1,4}$/) {$XXXX = 1;}
        when (/^-{1,2}t(?:ime)?$/) {$time = 1;}
        default {$src_dir = $arg;}
    }
}
$src_dir .= "/Open-ILS/src/sql/Pg/upgrade/";

my $eg_version = get_eg_version();

opendir my ($dh), $src_dir or die("Can't open $src_dir");
my @files = readdir $dh;
closedir($dh);
foreach my $file (sort @files) {
    my $vers = substr($file, 0, index($file, "."));
    if ($vers gt $eg_version) {
        # 20130508: --XXXX expanded to include YYYY scripts as well.
        if (($vers ne 'XXXX' && $vers ne 'YYYY') || $XXXX) {
            $count++;
            print '-' x 72 . "\n";
            my $script = $src_dir . $file;
            my $start = time();
            system("psql -veg_version=NULL -f $script");
            my $end = time() - $start;
            print("$end seconds elapsed\n") if ($time);
        }
    }
}
print '-' x 72 . "\n" if ($count);

sub get_eg_version {
    use DBI;
    my $result;
    my $db = DBI->connect("DBI:Pg:");

    if ($db) {
        $result = $db->selectrow_arrayref("select max(version) from config.upgrade_log")->[0];
        $db->disconnect;
    } else {
        die("Failed to connect to database");
    }
    return $result;
}
