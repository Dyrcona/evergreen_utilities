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

# Sets the xact_finish on open circulation transactions. This is
# intended to be used to close hanging transactions when a lost copy
# is checked in and the transaction is not closed as a result of this
# bug: https://bugs.launchpad.net/evergreen/+bug/758982

# It currently takes two arguments: the patron barcode and the copy
# barcode. It looks them up in an attempt to determine which is
# which. It will close any open transactions for the identified patron
# and copy.
use strict;
use warnings;

use OpenILS::Utils::Cronscript;
use JSONPrefs;
use UNIVERSAL;

my $U = 'OpenILS::Application::AppUtils';

my $script = OpenILS::Utils::Cronscript->new({nolockfile=>1});

my $login = JSONPrefs->load($ENV{'HOME'} . "/myprefs.d/evergreen.json");

my $authtoken = $script->authenticate($login->TO_JSON);

die ("Failed to authenticate!") unless($authtoken);

END {
    $script->logout() if ($authtoken);
}

die ("Need exactly two barcodes!") unless ($#ARGV == 1);

my $user = "";
my $copy = "";

foreach my $barcode (@ARGV) {
    # check if we have a user barcode.
    my $obj = $U->simplereq(
        'open-ils.actor',
        'open-ils.actor.user.fleshed.retrieve_by_barcode',
        $authtoken,
        $barcode
    );
    if (UNIVERSAL::isa($obj, 'Fieldmapper::actor::user')) {
        unless ($user) {
            $user = $obj;
        } else {
            die("You gave me two patron barcodes!");
        }
    } else {
        # Check if we have a copy barcode.
        $obj = $U->simplereq(
            'open-ils.search',
            'open-ils.search.asset.copy.find_by_barcode',
            $barcode
        );
        if (UNIVERSAL::isa($obj, 'Fieldmapper::asset::copy')) {
            unless ($copy) {
                $copy = $obj;
            } else {
                die("You gave me two copy barcodes");
            }
        } else {
            die("$barcode is not a valid copy or patron barcode");
        }
    }
}

my $editor = $script->editor(authtoken=>$authtoken);
die("Checkauth!") unless ($editor->checkauth());

# Search for any "open" circulations for this copy of this patron:
my $circs = $editor->search_action_circulation(
    {
        usr => $user->id,
        target_copy => $copy->id,
        xact_finish => undef
    }
);

if ($circs && ref($circs) eq 'ARRAY') {
    foreach my $circ (@{$circs}) {
        printf("Updating circulation %d\n", $circ->id);
        $editor->xact_begin;
        $circ->xact_finish('now');
        $editor->update_action_circulation($circ);
        $editor->commit;
    }
} else {
    die("An error occurred looking up the circulations");
}
