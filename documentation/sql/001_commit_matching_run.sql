-- Atomic production commit for the ML matching pipeline.
--
-- Supabase RPC calls run this Postgres function in a single transaction. If
-- any statement inside the function fails, Postgres rolls back all ride,
-- match, flight, voucher, and Connect cleanup changes made by the function call.

create table if not exists public."MatchingRuns" (
  run_id uuid primary key,
  status text not null default 'committing',
  payload_hash text not null,
  commit_result jsonb,
  started_at timestamptz not null default now(),
  committed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  constraint matching_runs_status_chk
    check (status in ('committing', 'committed')),

  constraint matching_runs_committed_state_chk
    check (
      (status = 'committing' and committed_at is null and commit_result is null)
      or
      (status = 'committed' and committed_at is not null and commit_result is not null)
    )
);

alter table public."MatchingRuns" enable row level security;

revoke all on public."MatchingRuns" from anon;
revoke all on public."MatchingRuns" from authenticated;

create index if not exists matching_runs_status_idx
  on public."MatchingRuns" (status);

create index if not exists matching_runs_payload_hash_idx
  on public."MatchingRuns" (payload_hash);

create or replace function public.commit_matching_run(
  p_run_id uuid,
  p_payload jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  group_item jsonb;
  member_item jsonb;
  v_ride_id int8;
  v_group_count integer := 0;
  v_match_count integer := 0;
  v_cleanup_flight_ids int8[] := array[]::int8[];
  v_cleanup_ride_ids int8[] := array[]::int8[];
  v_matched_flight_ids int8[] := array[]::int8[];
  v_unmatched_flight_ids int8[] := array[]::int8[];
  v_group_voucher_id uuid;
  v_group_voucher_link text;
  v_contingency_voucher_id uuid;
  v_contingency_voucher_link text;
  v_group_is_subsidized boolean;
  v_group_is_connect boolean;
  v_group_airport text;
  v_group_to_airport boolean;
  v_ride_date date;
  v_group_vouchers_used integer := 0;
  v_contingency_vouchers_used integer := 0;
  v_payload_hash text;
  v_existing_run public."MatchingRuns"%rowtype;
  v_commit_result jsonb;
begin
  if p_run_id is null then
    raise exception 'commit_matching_run requires p_run_id';
  end if;

  if p_payload is null or jsonb_typeof(p_payload) <> 'object' then
    raise exception 'commit_matching_run requires an object payload';
  end if;

  if coalesce(p_payload->>'run_id', '') <> p_run_id::text then
    raise exception 'payload run_id does not match p_run_id';
  end if;

  v_payload_hash := md5(p_payload::text);

  insert into public."MatchingRuns" (
    run_id,
    status,
    payload_hash
  )
  values (
    p_run_id,
    'committing',
    v_payload_hash
  )
  on conflict (run_id) do nothing;

  select *
  into v_existing_run
  from public."MatchingRuns"
  where run_id = p_run_id
  for update;

  if not found then
    raise exception 'could not create or lock MatchingRuns row for run_id %', p_run_id;
  end if;

  if v_existing_run.payload_hash <> v_payload_hash then
    raise exception 'run_id % was already used with a different payload', p_run_id;
  end if;

  if v_existing_run.status = 'committed' then
    return v_existing_run.commit_result || jsonb_build_object('idempotent_replay', true);
  end if;

  select coalesce(array_agg(value::text::int8), array[]::int8[])
  into v_cleanup_flight_ids
  from jsonb_array_elements(coalesce(p_payload->'connect_cleanup_flight_ids', '[]'::jsonb));

  select coalesce(array_agg(value::text::int8), array[]::int8[])
  into v_unmatched_flight_ids
  from jsonb_array_elements(coalesce(p_payload->'unmatched_flight_ids', '[]'::jsonb));

  if cardinality(v_cleanup_flight_ids) > 0 then
    select coalesce(array_agg(distinct ride_id), array[]::int8[])
    into v_cleanup_ride_ids
    from public."Matches"
    where flight_id = any(v_cleanup_flight_ids)
      and ride_id is not null;

    delete from public."Matches"
    where flight_id = any(v_cleanup_flight_ids);

    if cardinality(v_cleanup_ride_ids) > 0 then
      delete from public."Rides" r
      where r.ride_id = any(v_cleanup_ride_ids)
        and not exists (
          select 1
          from public."Matches" m
          where m.ride_id = r.ride_id
        );
    end if;
  end if;

  for group_item in
    select value
    from jsonb_array_elements(coalesce(p_payload->'groups', '[]'::jsonb))
  loop
    if jsonb_array_length(coalesce(group_item->'members', '[]'::jsonb)) = 0 then
      raise exception 'commit payload contains an empty group';
    end if;

    v_ride_date := (group_item->>'ride_date')::date;
    v_group_airport := upper(group_item->>'airport');
    v_group_to_airport := (group_item->>'to_airport')::boolean;
    v_group_is_subsidized := coalesce((group_item->>'is_subsidized')::boolean, false);
    v_group_is_connect := coalesce(group_item->>'ride_type', '') = 'Connect';
    v_group_voucher_id := null;
    v_group_voucher_link := null;

    insert into public."Rides" (ride_date, ride_type)
    values (
      v_ride_date,
      nullif(group_item->>'ride_type', '')
    )
    returning ride_id into v_ride_id;

    v_group_count := v_group_count + 1;

    if v_group_is_subsidized and not v_group_is_connect then
      select voucher_id, voucher_link
      into v_group_voucher_id, v_group_voucher_link
      from public."Vouchers"
      where used = false
        and contingency = false
        and airport = v_group_airport
        and to_airport = v_group_to_airport
        and start_date <= v_ride_date
        and end_date >= v_ride_date
      order by start_date, end_date, created_at, voucher_id
      limit 1
      for update skip locked;

      if v_group_voucher_id is null then
        raise exception
          'No available group voucher for airport %, direction %, ride date %',
          v_group_airport,
          case when v_group_to_airport then 'to_airport' else 'from_airport' end,
          v_ride_date;
      end if;

      update public."Vouchers"
      set used = true,
          used_at = now(),
          used_by_run_id = p_run_id,
          assigned_ride_id = v_ride_id,
          assigned_flight_id = null,
          updated_at = now()
      where voucher_id = v_group_voucher_id;

      v_group_vouchers_used := v_group_vouchers_used + 1;
    end if;

    for member_item in
      select value
      from jsonb_array_elements(group_item->'members')
    loop
      v_contingency_voucher_id := null;
      v_contingency_voucher_link := null;

      if v_group_is_subsidized and not v_group_is_connect and not v_group_to_airport then
        select voucher_id, voucher_link
        into v_contingency_voucher_id, v_contingency_voucher_link
        from public."Vouchers"
        where used = false
          and contingency = true
          and airport = v_group_airport
          and to_airport = v_group_to_airport
          and start_date <= v_ride_date
          and end_date >= v_ride_date
        order by start_date, end_date, created_at, voucher_id
        limit 1
        for update skip locked;

        if v_contingency_voucher_id is null then
          raise exception
            'No available contingency voucher for airport %, direction from_airport, ride date %, flight %',
            v_group_airport,
            v_ride_date,
            member_item->>'flight_id';
        end if;

        update public."Vouchers"
        set used = true,
            used_at = now(),
            used_by_run_id = p_run_id,
            assigned_ride_id = v_ride_id,
            assigned_flight_id = (member_item->>'flight_id')::int8,
            updated_at = now()
        where voucher_id = v_contingency_voucher_id;

        v_contingency_vouchers_used := v_contingency_vouchers_used + 1;
      end if;

      insert into public."Matches" (
        ride_id,
        user_id,
        flight_id,
        date,
        time,
        earliest_time,
        latest_time,
        source,
        voucher,
        contingency_voucher,
        is_verified,
        is_subsidized,
        uber_type
      )
      values (
        v_ride_id,
        (member_item->>'user_id')::uuid,
        (member_item->>'flight_id')::int8,
        (member_item->>'date')::date,
        (member_item->>'time')::time,
        (member_item->>'earliest_time')::time,
        (member_item->>'latest_time')::time,
        member_item->>'source',
        v_group_voucher_link,
        v_contingency_voucher_link,
        coalesce((member_item->>'is_verified')::boolean, false),
        v_group_is_subsidized,
        nullif(member_item->>'uber_type', '')
      );

      v_match_count := v_match_count + 1;
      v_matched_flight_ids := array_append(v_matched_flight_ids, (member_item->>'flight_id')::int8);
    end loop;
  end loop;

  if cardinality(v_matched_flight_ids) > 0 then
    update public."Flights"
    set matching_status = 'matched',
        original_unmatched = false
    where flight_id = any(v_matched_flight_ids);
  end if;

  if cardinality(v_unmatched_flight_ids) > 0 then
    update public."Flights"
    set matching_status = 'unmatched',
        original_unmatched = true
    where flight_id = any(v_unmatched_flight_ids);
  end if;

  v_commit_result := jsonb_build_object(
    'run_id', p_run_id,
    'groups_inserted', v_group_count,
    'matches_inserted', v_match_count,
    'matched_flights_updated', cardinality(v_matched_flight_ids),
    'unmatched_flights_updated', cardinality(v_unmatched_flight_ids),
    'group_vouchers_used', v_group_vouchers_used,
    'contingency_vouchers_used', v_contingency_vouchers_used,
    'connect_cleanup_flights', cardinality(v_cleanup_flight_ids),
    'connect_cleanup_rides_touched', cardinality(v_cleanup_ride_ids),
    'idempotent_replay', false
  );

  update public."MatchingRuns"
  set status = 'committed',
      commit_result = v_commit_result,
      committed_at = now(),
      updated_at = now()
  where run_id = p_run_id;

  return v_commit_result;
end;
$$;

revoke all on function public.commit_matching_run(uuid, jsonb) from public;
revoke all on function public.commit_matching_run(uuid, jsonb) from anon;
revoke all on function public.commit_matching_run(uuid, jsonb) from authenticated;
grant execute on function public.commit_matching_run(uuid, jsonb) to service_role;
