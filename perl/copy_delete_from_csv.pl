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

# Delete copies from a CSV input where the first column is the copy
# barcode.

use strict;
use warnings;

use OpenILS::Utils::Cronscript;
use OpenILS::Application::Cat::AssetCommon;

use Text::CSV;
use JSONPrefs;

my $apputils = 'OpenILS::Application::AppUtils';
my $assetcom = 'OpenILS::Application::Cat::AssetCommon';

my $script = OpenILS::Utils::Cronscript->new({nolockfile=>1});

my $login = JSONPrefs->load($ENV{'HOME'} . "/myprefs.d/evergreen.json");

my $authtoken = $script->authenticate($login->TO_JSON);

die "failed to authenticate" unless($authtoken);

END {
    $script->logout() if ($authtoken);
}

my $editor = $script->editor(authtoken=>$authtoken,xact=>1);
die "Checkauth!" unless $editor->checkauth();

# let's retrieve the copy statuses that restrict deletion.
my $bad_statuses = $editor->search_config_copy_status(
    {
        restrict_copy_delete => 't'
    }
);

# Pipe the CSV data into standard input.
my $csv = Text::CSV->new();
my $fh = *STDIN;

# Load the copy data into an array:
my @copies = ();
while (my $row = $csv->getline($fh)) {
    my $r = $editor->search_asset_copy(
        [
            {
                barcode => $row->[0], deleted => 'f'},
            {
                flesh => 1,
                flesh_fields => {
                    acp => [ 'call_number' ]
                }
            }
        ]
    );

    if (ref($r) eq 'ARRAY') {
        if (@$r) {
            foreach (@$r) {
                if (my $stat = status_is_bad($_)) {
                    printf("skipping %s, is %s\n", $_->barcode, $stat->name);
                } else {
                    push(@copies, $_);
                }
            }
        } else {
            printf("skipping %s, already deleted\n", $row->[0]);
        }
    }
}

# Delete the copies.
my $retarget_holds = [];
foreach my $copy (@copies) {
    my $r = $assetcom->delete_copy(
        $editor,
        1,
        $copy->call_number,
        $copy,
        $retarget_holds,
        1,
        0
    );
    if ($r && defined($r->{textcode})) {
        printf("error deleting %s: %s\n", $copy->barcode, $r->{textcode});
    } else {
        printf("deleted %s\n", $copy->barcode);
    }
}
$editor->finish;

# Retarget the holds, if any.
$apputils->simplereq(
    'open-ils.circ',
    'open-ils.circ.hold.reset.batch',
    $authtoken,
    $retarget_holds
) if (@$retarget_holds);

sub status_is_bad {
    my $acp = shift;
    foreach my $status (@$bad_statuses) {
        if ($status->id == $acp->status) {
            return $status;
        }
    }
    return undef;
}
