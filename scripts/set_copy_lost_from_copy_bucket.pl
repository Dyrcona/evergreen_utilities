#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2013 Merrimack Valley Library Consortium
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

# This script set copies and cirulations to lost that are found in copy
# buckets. It takes the copy bucket id #s on the command line. It will
# then set all copies to lost that are in the buckets.
#
# It does NOT empty the copies from the bucket.

use strict;
use warnings;

use OpenILS::Utils::Cronscript;
use OpenILS::Application::Cat::AssetCommon;
use JSONPrefs;

my $apputils = 'OpenILS::Application::AppUtils';
my $assetcommon = 'OpenILS::Application::Cat::AssetCommon';

my $script = OpenILS::Utils::Cronscript->new({nolockfile=>1});

# Load login credentials from a JSON file using my JSONPrefs object.
# (See README for more details.)
my $login = JSONPrefs->load($ENV{'HOME'} . "/myprefs.d/evergreen.json");

my $authtoken = $script->authenticate($login->TO_JSON);

die "failed to authenticate" unless($authtoken);

END {
    $script->logout() if ($authtoken);
}

my $editor = $script->editor(authtoken=>$authtoken);
die "Checkauth!" unless $editor->checkauth();

# Now, we get the bucket id(s) from the command line!
while (my $bucket_id = shift @ARGV) {
    print("Started $bucket_id\n");
    my $bucket = $apputils->simplereq(
        'open-ils.actor',
        'open-ils.actor.container.flesh',
        $authtoken,
        'copy',
        $bucket_id
    );
    $editor->xact_begin();
    foreach my $item (@{$bucket->items}) {
        my $acp = $editor->retrieve_asset_copy($item->target_copy);
        # skip deleted copies
        if ($apputils->is_true($acp->deleted)) {
            printf("skipping copy id %d, deleted\n", $acp->id);
        } else {
            my $r = $assetcommon->set_item_lost(
                $editor,
                $acp->id()
            );
            if ($r && $apputils->event_code($r)) {
                printf("failed to lose copy id %d, %s\n", $acp->id,
                       $r->{textcode});
            } else {
                printf("lost copy id %d\n", $acp->id);
            }
        }
    }
    $editor->xact_commit();
}

$editor->finish;
