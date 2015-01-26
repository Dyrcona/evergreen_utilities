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

    my $columns = ['id', 'shortname', 'copy_lib', 'usr_lib', 'group',
                   'circ_modifier', 'marc_type','marc_bib_level',
                   'marc_vr_format', 'ref_flag', 'circulate', 'duration_rule',
                   'recurring_fine_rule', 'max_fine_rule', 'renewals',
                   'grace_period'];

    my $weights = get_circ_matrix_weights($dbh);

    my $sth = $dbh->prepare(<<'EOQ'
SELECT ccmp.id, aou.shortname, pgt.name as group, ccmp.circ_modifier,
ccmp.marc_type, ccmp.marc_bib_level, ccmp.marc_vr_format,
cou.shortname as copy_lib, uou.shortname as usr_lib, ccmp.ref_flag,
ccmp.circulate, crcd.normal as duration_rule, crrf.normal as recurring_fine_rule,
crmf.amount as max_fine_rule, coalesce(ccmp.renewals, crcd.max_renewals) as renewals,
coalesce(ccmp.grace_period, crrf.grace_period) as grace_period,
oou.shortname as owning_lib
FROM config.circ_matrix_matchpoint ccmp
LEFT JOIN actor.org_unit cou ON ccmp.copy_circ_lib = cou.id
LEFT JOIN actor.org_unit uou ON ccmp.user_home_ou = uou.id
LEFT JOIN actor.org_unit oou ON ccmp.copy_owning_lib = oou.id
JOIN actor.org_unit aou ON ccmp.org_unit = aou.id
JOIN permission.grp_tree pgt ON ccmp.grp = pgt.id
LEFT JOIN config.rule_circ_duration crcd ON ccmp.duration_rule = crcd.id
LEFT JOIN config.rule_recurring_fine crrf ON ccmp.recurring_fine_rule = crrf.id
LEFT JOIN config.rule_max_fine crmf ON ccmp.max_fine_rule = crmf.id
LEFT JOIN permission.grp_descendants_distance(1) pgdd ON ccmp.grp = pgdd.id
LEFT JOIN actor.org_unit_descendants_distance(1) oud ON ccmp.org_unit = oud.id
LEFT JOIN actor.org_unit_descendants_distance(1) ccd ON ccmp.copy_circ_lib = ccd.id
LEFT JOIN actor.org_unit_descendants_distance(1) cod ON ccmp.copy_owning_lib = cod.id
LEFT JOIN actor.org_unit_descendants_distance(1) uhd ON ccmp.user_home_ou = uhd.id
WHERE ccmp.active = 't'
ORDER BY
CASE WHEN ccmp.org_unit IS NOT NULL THEN 2^(2.0*$17 - ((4-oud.distance)/4)) ELSE 0.0 END +
CASE WHEN ccmp.grp IS NOT NULL THEN 2^(2.0*$13 - ((4-pgdd.distance)/4)) ELSE 0.0 END +
CASE WHEN ccmp.copy_owning_lib IS NOT NULL THEN 2^(2.0*$15 - ((4-cod.distance)/4)) ELSE 0.0 END +
CASE WHEN ccmp.copy_circ_lib IS NOT NULL THEN 2^(2.0*$16 - ((4-ccd.distance)/4)) ELSE 0.0 END +
CASE WHEN ccmp.user_home_ou IS NOT NULL THEN 2^(2.0*$12 - ((4-uhd.distance)/4)) ELSE 0.0 END +
CASE WHEN ccmp.is_renewal IS NOT NULL THEN 4^$1 ELSE 0.0 END +
CASE WHEN ccmp.juvenile_flag IS NOT NULL THEN 4^$2 ELSE 0.0 END +
CASE WHEN ccmp.usr_age_lower_bound IS NOT NULL THEN 4^$3 ELSE 0.0 END +
CASE WHEN ccmp.usr_age_upper_bound IS NOT NULL THEN 4^$4 ELSE 0.0 END +
CASE WHEN ccmp.circ_modifier IS NOT NULL THEN 4^$5 ELSE 0.0 END +
CASE WHEN ccmp.copy_location IS NOT NULL THEN 4^$6 ELSE 0.0 END +
CASE WHEN ccmp.marc_type IS NOT NULL THEN 4^$7 ELSE 0.0 END +
CASE WHEN ccmp.marc_form IS NOT NULL THEN 4^$8 ELSE 0.0 END +
CASE WHEN ccmp.marc_bib_level IS NOT NULL THEN 4^$14 ELSE 0.0 END +
CASE WHEN ccmp.marc_vr_format IS NOT NULL THEN 4^$9 ELSE 0.0 END +
CASE WHEN ccmp.ref_flag IS NOT NULL THEN 4^$10 ELSE 0.0 END +
CASE WHEN ccmp.item_age IS NOT NULL THEN 4^$11 - 1 + 86400/EXTRACT(EPOCH FROM ccmp.item_age) ELSE 0.0 END DESC,
ccmp.id
EOQ
    );

    $sth->bind_param(1, $weights->{is_renewal});
    $sth->bind_param(2, $weights->{juvenile_flag});
    $sth->bind_param(3, $weights->{usr_age_lower_bound});
    $sth->bind_param(4, $weights->{usr_age_upper_bound});
    $sth->bind_param(5, $weights->{circ_modifier});
    $sth->bind_param(6, $weights->{copy_location});
    $sth->bind_param(7, $weights->{marc_type});
    $sth->bind_param(8, $weights->{marc_form});
    $sth->bind_param(9, $weights->{marc_vr_format});
    $sth->bind_param(10, $weights->{ref_flag});
    $sth->bind_param(11, $weights->{item_age});
    $sth->bind_param(12, $weights->{user_home_ou});
    $sth->bind_param(13, $weights->{grp});
    $sth->bind_param(14, $weights->{marc_bib_level});
    $sth->bind_param(15, $weights->{copy_owning_lib});
    $sth->bind_param(16, $weights->{copy_circ_lib});
    $sth->bind_param(17, $weights->{org_unit});

    unless ($sth->execute()) {
        die("do_circ_matrix_matchpoint");
    }
    my $results = $sth->fetchall_arrayref({});
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

sub get_circ_matrix_weights {
    my $dbh = shift;

    my $weights;

    my $sth = $dbh->prepare(<<WEIGHTS
select cw.*
from config.weight_assoc wa
join config.circ_matrix_weights cw on cw.id = wa.circ_weights
where wa.org_unit = 1
limit 1
WEIGHTS
    );

    if ($sth->execute()) {
        $weights = $sth->fetchrow_hashref();
    }

    # Just in case we don't have any weights defined.
    unless ($weights) {
        $weights = {};
        $weights->{grp} = 11.0;
        $weights->{org_unit} = 10.0;
        $weights->{circ_modifier} = 5.0;
        $weights->{copy_location} = 5.0;
        $weights->{marc_type} = 4.0;
        $weights->{marc_form} = 3.0;
        $weights->{marc_bib_level} = 2.0;
        $weights->{marc_vr_format} = 2.0;
        $weights->{copy_circ_lib} = 8.0;
        $weights->{copy_owning_lib} = 8.0;
        $weights->{user_home_ou} = 8.0;
        $weights->{ref_flag} = 1.0;
        $weights->{juvenile_flag} = 6.0;
        $weights->{is_renewal} = 7.0;
        $weights->{usr_age_lower_bound} = 0.0;
        $weights->{usr_age_upper_bound} = 0.0;
        $weights->{item_age} = 0.0;
    }

    return $weights;
}

