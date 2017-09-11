#!/usr/bin/perl
# ---------------------------------------------------------------
# Copyright © 2016 C/W MARS, Inc.
# Jason Stephenson <jstephenson@cwmars.org>
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

use strict;
use warnings;
use feature qw/state/;
use Getopt::Long;
use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'utf8');
use OpenILS::Utils::Normalize qw(clean_marc naco_normalize);
use IO::File;
use DateTime;
use DateTime::TimeZone;
use Time::HiRes qw/tv_interval gettimeofday/;
use DBI;

IO::File->input_record_separator("\x1E\x1D");
IO::File->output_record_separator("\n");

# options with defaults:
my $db_user = 'evergreen';
my $db_host = 'db1';
my $db_db = 'evergreen';
my $db_password = 'evergreen';
my $db_port = 5432;
my $source;
my $strict;
my $timing;

GetOptions("user=s" => \$db_user,
           "host=s" => \$db_host,
           "db=s" => \$db_db,
           "password=s" => \$db_password,
           "port=i" => \$db_port,
           "source=s" => \$source,
           timing => \$timing,
           strict => \$strict) or die("Error in command line options");

my $dbh = DBI->connect("dbi:Pg:database=$db_db;host=$db_host;port=$db_port;application_name=loaderecords",
                       $db_user, $db_password,
                       {PrintError => 0, RaiseError => 1, AutoCommit => 1})
    or die("No database connection.");

die("Must specify --source option.") unless ($source);

$source = lookup_source($source);

die("--source is not valid.") unless ($source);

my $mapper = MARCFixedFieldMapper->new();

my ($rej, $exc); # Variables for reject and exception file handles. We only open this if necessary.
my $error_count = 0; # Count of errors.

# Because this can produce lots of output, we're writing progress messages to a log file instead of standard output.
my $log = IO::File->new("> log.txt");

foreach my $input_file (@ARGV) {
    my $count = 0;
    my $fh = IO::File->new("< $input_file");
    my $str = date_str($input_file, 1);
    print("$str"); # For running from at, etc., so we have something in the email to let us know when it is done.
    $log->print($str);
    while (my $raw = <$fh>) {
        $count++;
        eval {
            my ($match_start, $match_end, $update_start, $update_end, $message);
            my $record = MARC::Record->new_from_usmarc($raw);
            my @warnings = $record->warnings();
            $match_start = [gettimeofday()];
            my $match = find_best_match($record);
            $match_end = [gettimeofday()];
            if ($match) {
                no warnings qw(uninitialized);
                my $update_needed = 0;
                $message = "$input_file $count matches " . $match->{id} . " with score " . $match->{score};
                $message .= " in " . tv_interval($match_start, $match_end) . " seconds" if ($timing);
                $log->print($message);
                foreach my $nfield ($record->field('856')) {
                    my $add = 1;
                    foreach my $ofield ($match->{marc}->field('856')) {
                        if ($nfield->subfield('9') eq $ofield->subfield('9') && $nfield->subfield('u')
                                eq $ofield->subfield('u')) {
                            $add = 0;
                            last;
                        }
                    }
                    if ($add) {
                        $match->{marc}->insert_fields_ordered($nfield);
                        $update_needed++;
                    }
                }
                if ($update_needed) {
                    $update_start = [gettimeofday()];
                    my $success = update_marc($match);
                    $update_end = [gettimeofday()];
                    if ($success == 0) { # man DBI and look for the execute statement handle description for why.
                        $message = "$input_file $count update of record " . $match->{id} . " failed";
                    } else {
                        $message = "$input_file $count added $update_needed URL(s) to record " . $match->{id};
                    }
                    $message .= " in " . tv_interval($update_start, $update_end) . " seconds" if ($timing);
                    $log->print($message);
                } else {
                    $log->print("$input_file $count URL tag exists in " . $match->{id});
                }
            } else {
                if ($timing) {
                    $log->print("$input_file $count did not match in " . tv_interval($match_start, $match_end) . " seconds");
                }
                if (@warnings) {
                    if ($strict) {
                        die("@warnings");
                    } else {
                        $log->print("$input_file $count @warnings");
                    }
                }
                $update_start = [gettimeofday()];
                my $id = insert_marc($source, $record);
                $update_end = [gettimeofday()];
                if ($id) {
                    $message = "$input_file $count inserted as bre.id $id";
                } else {
                    $message = "$input_file $count failed to insert";
                }
                $message .= " in " . tv_interval($update_start, $update_end) . " seconds" if ($timing);
                $log->print($message);
            }
        };
        if ($@) {
            my $error = $@;
            $error =~ s/\s+$//;
            $error_count++;
            unless ($rej) {
                $rej = IO::File->new("> skipped_bibs.mrc");
                $rej->binmode(':raw');
            }
            unless ($exc) {
                $exc = IO::File->new("> exceptions.txt");
            }
            { local $\; # Just makin' sure.
              $rej->print($raw); }
            { local $\ = "\cM\cJ";
              $exc->print("Record $error_count: $error"); }
            $log->print("$input_file $count $error");
        }
    }
    $fh->close();
    $str = date_str($input_file, 0);
    print("$str"); # For running from at, etc., so we have something in the email to let us know when it is done.
    $log->print($str);
}

END {
    $dbh->disconnect() if ($dbh);
    if ($log && $log->opened()) {
        $log->close();
    }
    if ($rej && $rej->opened()) {
        $rej->close();
    }
    if ($exc && $exc->opened()) {
        $exc->close();
    }
}

sub find_best_match {
    my $record = shift;

    my $id_matches = get_identifier_matches($record);
    my $isbn_matches = get_isbn_matches($record);

    if ($id_matches || $isbn_matches) {
        my %merged;
        if ($id_matches && $isbn_matches) {
            %merged = %$id_matches;
            foreach my $k (keys %$isbn_matches) {
                if ($merged{$k}) {
                    $merged{$k}->{score} += $isbn_matches->{$k}->{score};
                } else {
                    $merged{$k} = $isbn_matches->{$k};
                }
            }
        } elsif ($id_matches) {
            %merged = %$id_matches;
        } else {
            %merged = %$isbn_matches;
        }

        my @results = sort {$b->{score} <=> $a->{score}} sort {$b->{id} <=> $a->{id}} values %merged;
        my $data = $results[0];
        $data->{marc} = MARC::Record->new_from_xml($data->{marc}) if ($data && ref($data) eq 'HASH' && $data->{marc});
        return $data;
    }

    return undef;
}

sub get_identifier_matches {
    my $record = shift;

    state $sth = $dbh->prepare(<<'EOQ'
select bre.id, bre.marc, 2 as score
from biblio.record_entry bre
join metabib.record_attr_vector_list mravl on mravl.source = bre.id
join config.coded_value_map itype on idx(mravl.vlist, itype.id) > 0
and itype.ctype = 'item_type' and itype.code = $1
join config.coded_value_map iform on idx(mravl.vlist, iform.id) > 0
and iform.ctype = 'item_form' and iform.code = $2
join metabib.real_full_rec identifier on identifier.record = bre.id
and identifier.tag = '035'
and identifier.subfield = 'a'
and identifier.value = any($3)
where not bre.deleted
EOQ
    );

    $sth->bind_param(1, $mapper->type($record));
    $sth->bind_param(2, $mapper->form($record));
    $sth->bind_param(3, prepare_identifiers($record));
    if ($sth->execute()) {
        my $data = $sth->fetchall_hashref('id');
        if ($data && %$data) {
            return $data;
        }
    }

    return undef;
}

sub get_isbn_matches {
    my $record = shift;

    my $isbn_query = prepare_isbns($record);

    state $sth = $dbh->prepare(<<'EOQ'
select bre.id, bre.marc, 1 as score
from biblio.record_entry bre
join metabib.record_attr_vector_list mravl on mravl.source = bre.id
join config.coded_value_map itype on idx(mravl.vlist, itype.id) > 0
and itype.ctype = 'item_type' and itype.code = $1
join config.coded_value_map iform on idx(mravl.vlist, iform.id) > 0
and iform.ctype = 'item_form' and iform.code = $2
join metabib.real_full_rec isbn on isbn.record = bre.id
and isbn.tag = '020'
and isbn.subfield = 'a'
and index_vector @@ $3
where not bre.deleted
EOQ
    );

    if ($isbn_query) {
        $sth->bind_param(1, $mapper->type($record));
        $sth->bind_param(2, $mapper->form($record));
        $sth->bind_param(3, $isbn_query);
        if ($sth->execute()) {
            my $data = $sth->fetchall_hashref('id');
            if ($data && %$data) {
                return $data;
            }
        }
    }

    return undef;
}

sub prepare_identifiers {
    my $record = shift;
    my $out = [];

    my @fields = $record->field('035');
    foreach my $field (@fields) {
        my $str = $field->subfield('a');
        push(@$out, naco_normalize($str, 'a')) if ($str);
    }
    return $out;
}

sub prepare_isbns {
    my $record = shift;
    my @isbns = ();
    my @fields = $record->field('020');
    foreach my $field (@fields) {
        my $isbn = $field->subfield('a');
        next unless($isbn);
        $isbn = naco_normalize($isbn, 'a');
        my $idx = index($isbn, ' ');
        $isbn = substr($isbn, 0, $idx) if ($idx != -1);
        push(@isbns, $isbn) unless (grep {$_ eq $isbn} @isbns);
    }
    return join(' | ', @isbns);
}

sub lookup_source {
    my $source = shift;
    if ($source =~ /^\d+$/) {
        # check that this is a valid source id.
        my $data = $dbh->selectall_arrayref("select source from config.bib_source where id = $source");
        if ($data && @$data) {
            return $source;
        }
    } else {
        my $data = $dbh->selectall_arrayref('select id from config.bib_source where source ~* ?', {}, "^$source");
        if ($data && @$data) {
            return $data->[0]->[0];
        }
    }
    return undef;
}

sub update_marc {
    my $ref = shift;
    state $sth = $dbh->prepare('update biblio.record_entry set marc = $2 where id = $1');
    $sth->bind_param(1, $ref->{id});
    $sth->bind_param(2, clean_marc($ref->{marc}));
    return $sth->execute();
}

sub insert_marc {
    my ($source, $record) = @_;
    state $sth = $dbh->prepare(<<EOINSERT
insert into biblio.record_entry
(source, marc, last_xact_id)
values
(?, ?, pg_backend_pid() || '.' || extract(epoch from now()))
returning id
EOINSERT
    );
    $sth->bind_param(1, $source);
    $sth->bind_param(2, clean_marc($record));
    if ($sth->execute()) {
        my $data = $sth->fetchall_arrayref();
        if ($data && @$data) {
            return $data->[0]->[0];
        }
    }
    return undef;
}

sub date_str {
    my ($file, $open) = @_;
    my $dt = DateTime->now(time_zone => DateTime::TimeZone->new(name => 'local'));
    return (($open) ? 'Starting' : 'Closing') . " $file at " . $dt->strftime('%a, %d %b %Y %H:%M:%S %z.');
}

package MARCFixedFieldMapper;

use vars qw/$AUTOLOAD/;

sub new {
    my $proto = shift;
    my $class = ref $proto || $proto;
    my $self = {};
    my $instance = bless($self, $class);
    $instance->_init_rec_type_map();
    $instance->_init_fixed_field_map();
    return $instance;
}

sub _init_rec_type_map {
    my $self = shift;
    eval {
        $self->{marc21_rec_type_map} = $dbh->selectall_hashref('select * from config.marc21_rec_type_map', 'code');
    };
    if ($@) {
        die("Failed to initialize MARCFixedFieldMapper: $@");
    }
}

sub _init_fixed_field_map {
    my $self = shift;
    eval {
        $self->{marc21_ff_pos_map} = $dbh->selectall_hashref('select * from config.marc21_ff_pos_map',
                                                             ['fixed_field', 'rec_type', 'tag']);
    };
    if ($@) {
        die("Failed to initialize MARCFixedFieldMapper: $@");
    }
    $self->{field_map} = {};
    foreach my $ff (keys %{$self->{marc21_ff_pos_map}}) {
        my $f = lc($ff);
        $f =~ s|/||;
        $self->{field_map}->{$f} = $ff;
    }
}

sub item_type {
    my $self = shift;
    my $record = shift;
    my $ldr = $record->leader();
    return substr($ldr, 6, 1);
}

sub bib_level {
    my $self = shift;
    my $record = shift;
    my $ldr = $record->leader();
    return substr($ldr, 7, 1);
}

sub rec_type {
    my $self = shift;
    my $record = shift;

    my $href = $self->{marc21_rec_type_map};
    my $itype = $self->item_type($record);
    my $blvl = $self->bib_level($record);
    my ($rec_type) = grep {$href->{$_}->{type_val} =~ $itype && $href->{$_}->{blvl_val} =~ $blvl} keys %$href;
    return $rec_type;
}

sub AUTOLOAD {
    my $self = shift;
    my $record = shift;

    my $field = $AUTOLOAD;
    $field =~ s/.*:://;
    if ($self->{field_map}->{$field}) {
        my $ffield = $self->{field_map}->{$field};
        my $rec_type = $self->rec_type($record);
        my $map = $self->{marc21_ff_pos_map}->{$ffield}->{$rec_type};
        if ($map) {
            my $val;
            foreach (keys %$map) {
                my $start = $map->{$_}->{start_pos};
                my $length = $map->{$_}->{length};
                my $default_val = $map->{$_}->{default_val};
                my $str;
                if ($_ eq 'ldr') {
                    $str = $record->leader();
                } else {
                    my $mfield = $record->field($_);
                    if ($mfield && $mfield->is_control_field()) {
                        $str = $mfield->data();
                    }
                }
                if ($str && length($str) >= $start + $length) {
                    $val = substr($str, $start, $length);
                }
                last if ($val && $val ne $default_val);
                $val = $default_val unless ($val);
            }
            return $val;
        }
    }
    return undef;
}

1;
