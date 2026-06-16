-- Additional database integrity safeguards for the ML matching remediation.
--
-- Run the diagnostic SELECT statements first. If they return rows, resolve the
-- existing data issue before applying the constraint/index section.

-- Diagnostic: Matches rows that would violate ride/flight foreign keys.
select 'missing ride for match' as issue, m.*
from public."Matches" m
left join public."Rides" r on r.ride_id = m.ride_id
where m.ride_id is not null
  and r.ride_id is null;

select 'missing flight for match' as issue, m.*
from public."Matches" m
left join public."Flights" f on f.flight_id = m.flight_id
where m.flight_id is not null
  and f.flight_id is null;

-- Diagnostic: duplicate active match rows for the same flight.
select flight_id, count(*) as match_count, array_agg(ride_id order by ride_id) as ride_ids
from public."Matches"
where flight_id is not null
group by flight_id
having count(*) > 1;

-- Diagnostic: voucher rows whose used state is internally inconsistent.
select *
from public."Vouchers"
where (used = false and (used_at is not null or used_by_run_id is not null))
   or (used = true and (used_at is null or used_by_run_id is null));

-- Constraint/index section.

do $$
begin
  if not exists (
    select 1 from pg_constraint where conname = 'matches_ride_id_fk'
  ) then
    alter table public."Matches"
      add constraint matches_ride_id_fk
      foreign key (ride_id)
      references public."Rides"(ride_id)
      not valid;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'matches_flight_id_fk'
  ) then
    alter table public."Matches"
      add constraint matches_flight_id_fk
      foreign key (flight_id)
      references public."Flights"(flight_id)
      not valid;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'vouchers_used_by_run_fk'
  ) then
    alter table public."Vouchers"
      add constraint vouchers_used_by_run_fk
      foreign key (used_by_run_id)
      references public."MatchingRuns"(run_id)
      not valid;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'vouchers_used_run_consistency_chk'
  ) then
    alter table public."Vouchers"
      add constraint vouchers_used_run_consistency_chk
      check (
        (used = false and used_by_run_id is null)
        or
        (used = true and used_by_run_id is not null)
      )
      not valid;
  end if;

  if not exists (
    select 1 from pg_constraint where conname = 'matches_required_fields_chk'
  ) then
    alter table public."Matches"
      add constraint matches_required_fields_chk
      check (
        ride_id is not null
        and user_id is not null
        and flight_id is not null
        and date is not null
        and time is not null
        and source is not null
      )
      not valid;
  end if;
end;
$$;

-- This unique index enforces one active Matches row per flight. It will fail if
-- existing duplicate match rows are present; resolve duplicates reported by the
-- diagnostic query before applying.
create unique index if not exists matches_one_row_per_flight_idx
  on public."Matches" (flight_id)
  where flight_id is not null;

create index if not exists matches_ride_id_idx
  on public."Matches" (ride_id)
  where ride_id is not null;

create index if not exists matches_flight_id_idx
  on public."Matches" (flight_id)
  where flight_id is not null;
