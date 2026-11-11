-- Payments accepted by the API. The idempotency key makes a retried request return the same payment.
create table payments (
    id              uuid primary key,
    idempotency_key varchar(100) not null unique,
    request_hash    char(64)     not null,
    from_account    varchar(64)  not null,
    to_account      varchar(64)  not null,
    amount_cents    bigint       not null check (amount_cents > 0),
    currency        char(3)      not null,
    created_at      timestamptz  not null default now()
);

-- Transactional outbox: written in the same transaction as the payment, published to Kafka later.
create table outbox (
    id           bigserial    primary key,
    event_id     uuid         not null unique,
    topic        varchar(100) not null,
    event_key    varchar(100) not null,
    payload      text         not null,
    created_at   timestamptz  not null default now(),
    published_at timestamptz
);
create index outbox_unpublished on outbox (id) where published_at is null;

-- Events a worker has already applied. A second delivery of the same event is skipped.
create table processed_events (
    event_id     uuid        primary key,
    processed_at timestamptz not null default now()
);

-- Every skipped second delivery, so the crash test can show duplicates really happened.
create table skipped_duplicates (
    id         bigserial   primary key,
    event_id   uuid        not null,
    skipped_at timestamptz not null default now()
);

-- Double entry: each payment is one debit and one credit that sum to zero.
create table ledger_entries (
    id           bigserial   primary key,
    payment_id   uuid        not null,
    account      varchar(64) not null,
    amount_cents bigint      not null,
    applied_at   timestamptz not null default now(),
    unique (payment_id, account)
);

create table account_balances (
    account       varchar(64) primary key,
    balance_cents bigint      not null default 0,
    updated_at    timestamptz not null default now()
);
