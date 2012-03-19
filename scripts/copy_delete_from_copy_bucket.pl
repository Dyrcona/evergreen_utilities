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

# This guy deletes copies from the database that are found in copy
# buckets. It takes the copy bucket id #s on the command line. It will
# then delete all copies from the database that are in the bucket,
# except for those that are in a bad status for deletion.
#
# It is handy to run when staff have filled a copy bucket with so many
# copies that they get a network error when doing delete from catalog.
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
    $script->logout();
}

my $editor = $script->editor(authtoken=>$authtoken);
die "Checkauth!" unless $editor->checkauth();

# let's retrieve the copy statuses that should not be deleted.
my $bad_statuses = $editor->search_config_copy_status(
    {
        restrict_copy_delete => 't'
    }
);

# Now, we get the bucket id(s) from the command line!
while (my $bucket_id = shift @ARGV) {
    print("Started $bucket_id\n");
    my $holds = [];
    $editor->xact_begin;
    my $bucket = $apputils->simplereq(
        'open-ils.actor',
        'open-ils.actor.container.flesh',
        $authtoken,
        'copy',
        $bucket_id
    );
    foreach my $item (@{$bucket->items}) {
        my $acp = $editor->retrieve_asset_copy(
            [
                $item->target_copy,
                {
                    flesh=>1,
                    flesh_fields => {
                        acp => [ 'call_number' ]
                    }
                }
            ]
        );
        # skip already deleted copies
        if ($apputils->is_true($acp->deleted)) {
            printf("skipping copy id %d, already deleted\n", $acp->id);
        }
        elsif (my $status = status_is_bad($acp)) {
            printf("skipping copy id %d, %s\n", $acp->id, $status->name);
        } else {
            my $r = $assetcommon->delete_copy(
                $editor,
                1,
                $acp->call_number,
                $acp,
                $holds,
                1,
                0
            );
            if ($r && $apputils->event_code($r)) {
                printf("failed to delete copy id %d, %s\n", $acp->id,
                       $r->{textcode});
            } else {
                printf("deleted copy id %d\n", $acp->id);
            }
        }
    }
    $editor->commit;
    $apputils->simplereq(
        'open-ils.circ',
        'open-ils.circ.hold.reset.batch',
        $authtoken,
        $holds
    ) if (@$holds);
    print("Finished $bucket_id\n");
}

$editor->finish;

sub status_is_bad {
    my $acp = shift;
    foreach my $status (@$bad_statuses) {
        if ($status->id == $acp->status) {
            return $status;
        }
    }
    return undef;
}
