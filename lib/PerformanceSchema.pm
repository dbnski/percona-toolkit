# This program is copyright 2008-2011 Baron Schwartz,
#                           2011 Percona Ireland Ltd.,
#                           2018 Maciej Dobrzanski
# Feedback and improvements are welcome.
#
# THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
# MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 2; OR the Perl Artistic License.  On UNIX and similar
# systems, you can issue `man perlgpl' or `man perlartistic' to read these
# licenses.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA  02111-1307  USA.
# ###########################################################################
# PerformanceSchema package
# ###########################################################################
{
# Package: PerformanceSchema
# Processlist makes events when used to poll events_statements.
package PerformanceSchema;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Time::HiRes qw(time usleep);
use List::Util qw(max);
use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

use constant PTDEBUG => $ENV{PTDEBUG} || 0;
use constant {
   THREAD_ID               => 0,
   EVENT_ID                => 1,
   END_EVENT_ID            => 2,
   EVENT_NAME              => 3,
   SOURCE                  => 4,
   TIMER_START             => 5,
   TIMER_END               => 6,
   TIMER_WAIT              => 7,
   LOCK_TIME               => 8,
   SQL_TEXT                => 9,
   DIGEST                  => 10,
   DIGEST_TEXT             => 11,
   CURRENT_SCHEMA          => 12,
   OBJECT_TYPE             => 13,
   OBJECT_SCHEMA           => 14,
   OBJECT_NAME             => 15,
   OBJECT_INSTANCE_BEGIN   => 16,
   MYSQL_ERRNO             => 17,
   RETURNED_SQLSTATE       => 18,
   MESSAGE_TEXT            => 19,
   ERRORS                  => 20,
   WARNINGS                => 21,
   ROWS_AFFECTED           => 22,
   ROWS_SENT               => 23,
   ROWS_EXAMINED           => 24,
   CREATED_TMP_DISK_TABLES => 25,
   CREATED_TMP_TABLES      => 26,
   SELECT_FULL_JOIN        => 27,
   SELECT_FULL_RANGE_JOIN  => 28,
   SELECT_RANGE            => 29,
   SELECT_RANGE_CHECK      => 30,
   SELECT_SCAN             => 31,
   SORT_MERGE_PASSES       => 32,
   SORT_RANGE              => 33,
   SORT_ROWS               => 34,
   SORT_SCAN               => 35,
   NO_INDEX_USED           => 36,
   NO_GOOD_INDEX_USED      => 37,
   NESTING_EVENT_ID        => 38,
   NESTING_EVENT_TYPE      => 39,
   NESTING_EVENT_LEVEL     => 40,
};

# Sub: new
#
# Parameters:
#   %args - Arguments
#
# Optional Arguments:
#   interval - Hi-res sleep time before polling processlist in <parse_event()>.
#
# Returns:
#   Processlist object
sub new {
   my ( $class, %args ) = @_;
   my $self = {
      %args,
      polls       => 0,
      last_poll   => 0,
      event_cache => [],
   };

   die "--performance-schema requires --interval of at least 1 second"
      unless $args{interval} >= 1_000_000;

   return bless $self, $class;
}

# Sub: parse_event
#   Parse rows from events_statements_history to make events when queries
#   finish.
#
# Parameters:
#   %args - Arguments
#
# Required Arguments:
#   code - Callback that returns an arrayref of rows from a PS query.
#          $dbh->{mysql_thread_id} should be removed from the return value.
#
# Returns:
#   Hashref of a completed event.
sub parse_event {
   my ( $self, %args ) = @_;
   my @required_args = qw(code);
   foreach my $arg ( @required_args ) {
     die "I need a $arg argument" unless $args{$arg};
   }
   my ($code) = @args{@required_args};

   # Our first priority is to return cached events.  The caller expects
   # one event per return so we have to cache our events.  And the caller
   # should accept events as fast as we can return them; i.e. the caller
   # should not sleep between polls--that's our job in here (below).
   # XXX: This should only cause a problem if the caller is really slow
   # between calls to us, in which case polling may be delayed by the
   # caller's slowness plus our interval sleep.
   if ( @{$self->{event_cache}} ) {
      PTDEBUG && _d("Returning cached event");
      return shift @{$self->{event_cache}};
   }

   # It's time to sleep if we want to sleep and this is not the first poll.
   # Again, this assumes that the caller is not sleeping before calling us
   # and is not really slow between calls.  By "really slow" I mean slower
   # than the interval time.
   if ( $self->{interval} && $self->{polls} ) {
      PTDEBUG && _d("Sleeping between polls");
      usleep($self->{interval});
   }

   # Poll the processlist and time how long this takes.  Also get
   # the current time and calculate the poll time (etime) unless
   # these values are given via %args (for testing).
   # $time is the time after the poll so that $time-TIME should equal
   # the query's real start time, but see $query_start below...
   PTDEBUG && _d("Polling Performance Schema");
   my ($time, $etime) = @args{qw(time etime)};
   my $start          = $etime ? 0 : time;  # don't need start if etime given
   my $rows           = $code->();
   if ( !$rows ) {
      warn "The Performance Schema query callback did not return an arrayref";
      return;
   }
   $time  = time           unless $time;
   $etime = $time - $start unless $etime;
   $self->{polls}++;
   PTDEBUG && _d('Rows:', ($rows ? scalar @$rows : 0), 'in', $etime, 'seconds');

   my $active_cxn = $self->{active_cxn};
   my $curr_cxn   = {};
   my @new_cxn    = ();

   foreach my $row ( @$rows ) {

       push @{$self->{event_cache}},
          $self->make_event($row, $time);
   }

   $self->{last_poll} = $time;

   # Return the first event in our cache, if any.  It may be an event
   # we just made, or an event from a previous call.
   my $event = shift @{$self->{event_cache}};
   PTDEBUG && _d(scalar @{$self->{event_cache}}, "events in cache");
   return $event;
}

# The exec time of the query is the max of the time from the processlist, or the
# time during which we've actually observed the query running.  In case two
# back-to-back queries executed as the same one and we weren't able to tell them
# apart, their time will add up, which is kind of what we want.
sub make_event {
   my ( $self, $row, $time ) = @_;

   my $Query_time = $row->[TIMER_WAIT] / 1_000_000_000_000;
   my $Lock_time = $row->[LOCK_TIME] / 1_000_000_000_000;

   $Query_time = sprintf('%.17f', $Query_time) if $Query_time =~ /e/;
   $Query_time =~ s/\.(\d{1,6})\d*/\.$1/;

   $Lock_time = sprintf('%.17f', $Lock_time) if $Lock_time =~ /e/;
   $Lock_time =~ s/\.(\d{1,6})\d*/\.$1/;

   my $event = {
      Thread_id         => $row->[THREAD_ID],
      db                => $row->[CURRENT_SCHEMA],
      user              => "unknown",
      host              => "unknown",
      arg               => $row->[SQL_TEXT],
      bytes             => length($row->[SQL_TEXT]),
      ts                => Transformers::ts($time),
      Query_time        => $Query_time,
      Lock_time         => $Lock_time,
      Rows_examined     => $row->[ROWS_EXAMINED],
      Rows_sent         => $row->[ROWS_SENT],
      Rows_affected     => $row->[ROWS_AFFECTED],
      Tmp_tables        => $row->[CREATED_TMP_TABLES],
      Tmp_disk_tables   => $row->[CREATED_TMP_DISK_TABLES],
      Full_scan         => $row->[SELECT_SCAN],
      Full_join         => $row->[SELECT_FULL_JOIN] + $row->[SELECT_FULL_RANGE_JOIN],
      Tmp_table         => $row->[CREATED_TMP_TABLES] > 0 ? "Yes" : "No",
      Tmp_table_on_disk => $row->[CREATED_TMP_DISK_TABLES] > 0 ? "Yes" : "No",
      Filesort          => $row->[SORT_SCAN] > 0 || $row->[SORT_MERGE_PASSES] > 0 ? "Yes" : "No",
      Merge_passes      => $row->[SORT_MERGE_PASSES],
   };
   PTDEBUG && _d('Properties of event:', Dumper($event));
   return $event;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End PerformanceSchema package
# ###########################################################################