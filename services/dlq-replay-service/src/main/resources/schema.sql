create table if not exists replay_job (
    id uuid primary key,
    region varchar(16) not null,
    partition_no integer not null,
    start_offset bigint not null,
    end_offset_exclusive bigint not null,
    next_offset bigint not null,
    max_records integer not null,
    records_per_second integer not null,
    reason varchar(1000) not null,
    incident_id varchar(255),
    requested_by varchar(255) not null,
    status varchar(32) not null,
    replayed_count integer not null default 0,
    error_message varchar(1000),
    created_at timestamptz not null,
    updated_at timestamptz not null
);

create index if not exists idx_replay_job_region_status on replay_job(region, status);
create index if not exists idx_replay_job_created_at on replay_job(created_at desc);
