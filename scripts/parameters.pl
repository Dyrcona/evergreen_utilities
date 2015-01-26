#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2012, 2013 Merrimack Valley Library Consortium
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

# Program creates a XLS workbook with several sheets to contain
# information about circulation and hold parameters.

use strict;
use warnings;

use DBI;
use Excel::Writer::XLSX;


my $xlsFile = $ARGV[0] || 'parameters.xlsx';

my $dbh = DBI->connect('DBI:Pg:');

if ($dbh) {
    my $wb = Excel::Writer::XLSX->new($xlsFile);
    if ($wb) {
        do_circ_modifier($dbh, $wb);
        do_copy_location($dbh, $wb);
        do_circ_matrix_matchpoint($dbh, $wb);
        do_circ_matrix_limit_test_map($dbh, $wb);
        do_grp_penalty_threshold($dbh, $wb);
        do_hold_matrix_matchpoint($dbh, $wb);
        $wb->close();
    } else {
        print("Failed to create $xlsFile\n");
    }
    $dbh->disconnect;
} else {
    die("He's dead, Jim!");
}

sub do_circ_modifier {
    my ($dbh, $wb) = @_;

    my $query =<<EOQ;
SELECT code, name, description, sip2_media_type, magnetic_media, avg_wait_time
FROM config.circ_modifier
ORDER BY code ASC
EOQ

    my $columns = ['code', 'description', 'sip2_media_type', 'magnetic_media'];
    my $results = $dbh->selectall_arrayref($query, { Slice => {}});
    my $ws = $wb->add_worksheet('circ_modifier');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub do_circ_matrix_matchpoint {
    my ($dbh, $wb) = @_;

    my $query =<<EOQ;
SELECT ccmp.id, aou.shortname, pgt.name as group, ccmp.circ_modifier,
ccmp.marc_type, ccmp.marc_bib_level, ccmp.marc_vr_format,
cou.shortname as copy_lib, uou.shortname as usr_lib, ccmp.ref_flag,
ccmp.circulate, crcd.name as duration_rule, crrf.name as recurring_fine_rule,
crmf.name as max_fine_rule, ccmp.renewals, ccmp.grace_period
FROM config.circ_matrix_matchpoint ccmp
left join actor.org_unit cou on ccmp.copy_circ_lib = cou.id
left join actor.org_unit uou on ccmp.user_home_ou = uou.id
join actor.org_unit aou on ccmp.org_unit = aou.id
join permission.grp_tree pgt on ccmp.grp = pgt.id
left join config.rule_circ_duration crcd on ccmp.duration_rule = crcd.id
left join config.rule_recurring_fine crrf on ccmp.recurring_fine_rule = crrf.id
left join config.rule_max_fine crmf on ccmp.max_fine_rule = crmf.id
WHERE ccmp.active = 't'
ORDER BY ccmp.org_unit
EOQ

    my $columns = ['id', 'shortname', 'copy_lib', 'usr_lib', 'group',
                   'circ_modifier', 'marc_type','marc_bib_level',
                   'marc_vr_format', 'ref_flag', 'circulate', 'duration_rule',
                   'recurring_fine_rule', 'max_fine_rule', 'renewals',
                   'grace_period'];
    my $results = $dbh->selectall_arrayref($query, { Slice => {}});
    my $ws = $wb->add_worksheet('circ_matrix_matchpoint');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub do_hold_matrix_matchpoint {
    my ($dbh, $wb) = @_;

    my $query =<<EOQ;
SELECT chmp.id, aou.shortname as circ_lib, uou.shortname as usr_lib,
pou.shortname as pickup_lib, pgt.name as group, chmp.circ_modifier,
chmp.marc_type, chmp.marc_bib_level, chmp.marc_vr_format, chmp.ref_flag,
chmp.holdable
FROM config.hold_matrix_matchpoint chmp
left join actor.org_unit aou on chmp.item_circ_ou = aou.id
left join actor.org_unit uou on chmp.user_home_ou = uou.id
left join actor.org_unit pou on chmp.pickup_ou = pou.id
left join permission.grp_tree pgt on chmp.usr_grp = pgt.id
WHERE chmp.active = 't'
ORDER BY coalesce(chmp.item_circ_ou, 1)
EOQ

    my $columns = ['id', 'circ_lib', 'usr_lib', 'pickup_lib', 'group',
                   'circ_modifier', 'marc_type','marc_bib_level',
                   'marc_vr_format', 'ref_flag', 'holdable'];
    my $results = $dbh->selectall_arrayref($query, { Slice => {}});
    my $ws = $wb->add_worksheet('hold_matrix_matchpoint');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub do_grp_penalty_threshold {
    my ($dbh, $wb) = @_;

    my $query =<<EOQ;
SELECT aou.shortname, pgt.name as group, pgpt.threshold, csp.name as penalty,
csp.label, csp.block_list, csp.org_depth as depth
FROM permission.grp_penalty_threshold pgpt
join actor.org_unit aou on pgpt.org_unit = aou.id
join permission.grp_tree pgt on pgpt.grp = pgt.id
join config.standing_penalty csp on pgpt.penalty = csp.id
ORDER BY pgpt.org_unit
EOQ

    my $columns = ['shortname', 'group', 'label', 'block_list', 'threshold'];
    my $results = $dbh->selectall_arrayref($query, { Slice => {}});
    my $ws = $wb->add_worksheet('standing_penalty_thresholds');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub do_circ_matrix_limit_test_map {
    my ($dbh, $wb) = @_;

my $query =<<EOQ;
select m.matchpoint, s.name, string_agg(c.circ_mod, ':') as circ_modifiers,
s.items_out, s.depth, s.global, m.fallthrough
from config.circ_matrix_limit_set_map m
join config.circ_limit_set s on m.limit_set = s.id
left join config.circ_limit_set_circ_mod_map c on s.id = c.limit_set
where m.active = 't'
group by m.matchpoint, s.name, s.items_out, s.depth, s.global, m.fallthrough
order by m.matchpoint asc
EOQ

    my $columns = ['matchpoint', 'name', 'circ_modifiers', 'items_out', 'depth',
                   'global', 'fallthrough'];
    my $results = $dbh->selectall_arrayref($query, {Slice =>{}});
    my $ws = $wb->add_worksheet('items_out');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub do_copy_location {
    my ($dbh, $wb) = @_;

    my $query =<<EOQ;
select l.name as location, o.shortname as owner, l.holdable, l.hold_verify,
l.opac_visible, l.circulate
from asset.copy_location l
join actor.org_unit o
on o.id = l.owning_lib
order by l.name, o.shortname
EOQ

    my $columns = ['location', 'owner', 'holdable', 'hold_verify',
                   'opac_visible', 'circulate'];
    my $results = $dbh->selectall_arrayref($query, {Slice =>{}});
    my $ws = $wb->add_worksheet('copy_locations');
    write_headers($ws, $columns, $wb->add_format(bold => 1));
    write_rows($ws, $columns, $results);
}

sub write_headers {
    my ($ws, $columns, $format) = @_;
    $ws->write_row(0, 0, $columns, $format);
}

sub write_rows {
    my ($ws, $columns, $results) = @_;
    my $row = 0;
    foreach my $result (@{$results}) {
        my $col = 0;
        $row++;
        foreach my $column (@{$columns}) {
            $ws->write($row, $col++, $result->{$column});
        }
    }
}

