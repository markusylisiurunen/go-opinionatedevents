create table :SCHEMA.events (
  id bigserial primary key,
  uuid text not null,
  published_at timestamptz not null,
  topic text not null,
  queue text not null,
  name text not null,
  status text not null,
  deliver_at timestamptz not null,
  delivery_attempts integer not null default 0,
  payload json not null,

  unique (queue, uuid),

  constraint events_status_check check (status in ('pending', 'processed', 'dropped'))
);

create table :SCHEMA.routing (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  last_declared_at timestamptz not null default now(),
  topic text not null,
  queue text not null,

  unique (topic, queue)
);

-- an index for making ordered queries for a certain set of queues with a `pending` status
create index events_status_queue_published_at_idx
on :SCHEMA.events (status, queue, published_at);

-- an index for making updates to a specific message
create index events_uuid_queue_idx
on :SCHEMA.events (uuid, queue);

create function :SCHEMA.notify_of_new_events() returns trigger as $$
declare
  notification json;
begin
  notification = json_build_object(
    'topic', new .topic,
    'queue', new .queue,
    'uuid',  new .uuid
  );

  perform pg_notify('__events', notification::text);

  return null;
end;
$$ language plpgsql;

create trigger notify_of_new_events_trigger after insert on :SCHEMA.events
for each row execute procedure :SCHEMA.notify_of_new_events();
