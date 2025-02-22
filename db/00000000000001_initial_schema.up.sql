-- Load pgcrypto for gen_random_bytes.
CREATE EXTENSION pgcrypto;
-- Load an extension to allow indexing over both BIGINT and TSRANGE in a multicolumn GiST index.
CREATE EXTENSION btree_gist;

-- Identifies which aggregator role is being played for this task.
CREATE TYPE AGGREGATOR_ROLE AS ENUM(
    'LEADER',
    'HELPER'
);

-- Identifies the different types of authentication tokens.
CREATE TYPE AUTH_TOKEN_TYPE AS ENUM(
    'DAP_AUTH', -- DAP-01 style DAP-Auth-Token header
    'BEARER'    -- RFC 6750 bearer token
);

CREATE TYPE HPKE_KEY_STATE AS ENUM(
    'ACTIVE',    -- the key should be advertised to DAP clients
    'PENDING',   -- the key should not be advertised to DAP clients, but could be used for
                 -- decrypting client reports depending on when aggregators pick up the state change
    'EXPIRED'    -- the key is pending deletion. it should not be advertised, but could be used
                 -- for decrypting client reports depending on the age of those reports
);

CREATE TABLE global_hpke_keys(
    -- These columns should be treated as immutable.
    config_id SMALLINT PRIMARY KEY,  -- HPKE config ID
    config BYTEA NOT NULL,           -- HPKE config, including public key (encoded HpkeConfig message)
    private_key BYTEA NOT NULL,      -- private key (encrypted)

    -- These columns are mutable.
    state HPKE_KEY_STATE NOT NULL DEFAULT 'PENDING',  -- state of the key
    updated_at TIMESTAMP NOT NULL,                    -- when the key state was last changed

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL        -- the name of the transaction that last updated the row
);

-- Another DAP aggregator who we've partnered with to use the taskprov extension.
CREATE TABLE taskprov_peer_aggregators(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal only.
    endpoint TEXT NOT NULL,         -- peer aggregator HTTPS endpoint
    role AGGREGATOR_ROLE NOT NULL,  -- the role of this aggregator relative to the peer
    verify_key_init BYTEA NOT NULL, -- the preshared key used for VDAF verify key derivation.

    -- Parameters applied to every task created with this peer aggregator.
    tolerable_clock_skew   BIGINT NOT NULL, -- the maximum acceptable clock skew to allow between client and aggregator, in seconds
    report_expiry_age      BIGINT,          -- the maximum age of a report before it is considered expired (and acceptable for garbage collection), in seconds. NULL means that GC is disabled.
    collector_hpke_config BYTEA NOT NULL,   -- the HPKE config of the collector (encoded HpkeConfig message)

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT taskprov_peer_aggregator_endpoint_and_role_unique UNIQUE(endpoint, role)
);

-- Task aggregator auth tokens that we've shared with the peer aggregator.
CREATE TABLE taskprov_aggregator_auth_tokens(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    peer_aggregator_id BIGINT NOT NULL,  -- task ID the token is associated with
    ord BIGINT NOT NULL,                 -- a value used to specify the ordering of the authentication tokens
    token BYTEA NOT NULL,                -- bearer token used to authenticate messages to/from the other aggregator (encrypted)
    type AUTH_TOKEN_TYPE NOT NULL DEFAULT 'BEARER',

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT task_aggregator_auth_tokens_unique_peer_aggregator_id_and_ord UNIQUE(peer_aggregator_id, ord),
    CONSTRAINT fk_peer_aggregator_id FOREIGN KEY(peer_aggregator_id) REFERENCES taskprov_peer_aggregators(id) ON DELETE CASCADE
);

-- Task collector auth tokens that we've shared with the peer aggregator.
CREATE TABLE taskprov_collector_auth_tokens(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    peer_aggregator_id BIGINT NOT NULL,  -- task ID the token is associated with
    ord BIGINT NOT NULL,                 -- a value used to specify the ordering of the authentication tokens
    token BYTEA NOT NULL,                -- bearer token used to authenticate messages to/from the other aggregator (encrypted)
    type AUTH_TOKEN_TYPE NOT NULL DEFAULT 'BEARER',

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT task_collector_auth_tokens_unique_peer_aggregator_id_and_ord UNIQUE(peer_aggregator_id, ord),
    CONSTRAINT fk_peer_aggregator_id FOREIGN KEY(peer_aggregator_id) REFERENCES taskprov_peer_aggregators(id) ON DELETE CASCADE
);

-- Corresponds to a DAP task, containing static data associated with the task.
CREATE TABLE tasks(
    id                          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    task_id                     BYTEA UNIQUE NOT NULL,     -- 32-byte TaskID as defined by the DAP specification
    aggregator_role             AGGREGATOR_ROLE NOT NULL,  -- the role of this aggregator for this task
    peer_aggregator_endpoint    TEXT NOT NULL,             -- peer aggregator's API endpoint
    query_type                  JSONB NOT NULL,            -- the query type in use for this task, along with its parameters
    vdaf                        JSON NOT NULL,             -- the VDAF instance in use for this task, along with its parameters
    max_batch_query_count       BIGINT NOT NULL,           -- the maximum number of times a given batch may be collected
    task_expiration             TIMESTAMP,                 -- the time after which client reports are no longer accepted
    report_expiry_age           BIGINT,                    -- the maximum age of a report before it is considered expired (and acceptable for garbage collection), in seconds. NULL means that GC is disabled.
    min_batch_size              BIGINT NOT NULL,           -- the minimum number of reports in a batch to allow it to be collected
    time_precision              BIGINT NOT NULL,           -- the duration to which clients are expected to round their report timestamps, in seconds
    tolerable_clock_skew        BIGINT NOT NULL,           -- the maximum acceptable clock skew to allow between client and aggregator, in seconds
    collector_hpke_config       BYTEA,                     -- the HPKE config of the collector (encoded HpkeConfig message)
    vdaf_verify_key             BYTEA NOT NULL,            -- the VDAF verification key (encrypted)

    -- Authentication token used to authenticate messages to/from the other aggregator.
    -- These columns are NULL if the task was provisioned by taskprov.
    aggregator_auth_token_type  AUTH_TOKEN_TYPE,    -- the type of the authentication token
    aggregator_auth_token       BYTEA,              -- encrypted bearer token (only set for leader)
    aggregator_auth_token_hash  BYTEA,              -- hash of the token (only set for helper)
    CONSTRAINT aggregator_auth_token_null CHECK (
        -- If aggregator_auth_token_type is not NULL, then exactly one of aggregator_auth_token or
        -- aggregator_auth_token_hash must be not NULL.
        ((aggregator_auth_token_type IS NOT NULL) AND (aggregator_auth_token IS NULL) != (aggregator_auth_token_hash IS NULL))
        -- If aggregator_auth_token_type is NULL, then both aggregator_auth_token and
        -- aggregator_auth_token_hash must be NULL
        OR ((aggregator_auth_token_type IS NULL) AND (aggregator_auth_token IS NULL) AND (aggregator_auth_token_hash IS NULL))
    ),

    -- Authentication token used to authenticate messages to the leader from the collector. These
    -- columns are NULL if the task was provisioned by taskprov or if the task's role is helper.
    collector_auth_token_type   AUTH_TOKEN_TYPE,    -- the type of the authentication token
    collector_auth_token_hash        BYTEA,         -- hash of the token
    -- The collector_auth_token columns must either both be NULL or both be non-NULL
    CONSTRAINT collector_auth_token_null CHECK ((collector_auth_token_type IS NULL) = (collector_auth_token_hash IS NULL)),

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL        -- the name of the transaction that last updated the row
);
CREATE INDEX task_id_index ON tasks(task_id);

-- The HPKE public keys (aka configs) and private keys used by a given task.
CREATE TABLE task_hpke_keys(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    task_id BIGINT NOT NULL,      -- task ID the HPKE key is associated with
    config_id SMALLINT NOT NULL,  -- HPKE config ID
    config BYTEA NOT NULL,        -- HPKE config, including public key (encoded HpkeConfig message)
    private_key BYTEA NOT NULL,   -- private key (encrypted)

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT task_hpke_keys_unique_task_id_and_config_id UNIQUE(task_id, config_id),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

-- Individual reports received from clients.
CREATE TABLE client_reports(
    id                              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal-only
    task_id                         BIGINT NOT NULL,                 -- task ID the report is associated with
    report_id                       BYTEA NOT NULL,                  -- 16-byte ReportID as defined by the DAP specification
    client_timestamp                TIMESTAMP NOT NULL,              -- report timestamp, from client
    extensions                      BYTEA,                           -- encoded sequence of Extension messages (populated for leader only)
    public_share                    BYTEA,                           -- encoded public share (opaque VDAF message, populated for leader only)
    leader_input_share              BYTEA,                           -- encoded, decrypted leader input share (populated for leader only)
    helper_encrypted_input_share    BYTEA,                           -- encoded HpkeCiphertext message containing the helper's input share (populated for leader only)
    aggregation_started             BOOLEAN NOT NULL DEFAULT FALSE,  -- has this client report been associated with an aggregation job?

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT client_reports_unique_task_id_and_report_id UNIQUE(task_id, report_id),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX client_reports_task_and_timestamp_unaggregated_index ON client_reports (task_id, client_timestamp) WHERE aggregation_started = FALSE;
CREATE INDEX client_reports_task_and_timestamp_index ON client_reports(task_id, client_timestamp);

-- Specifies the possible state of an aggregation job.
CREATE TYPE AGGREGATION_JOB_STATE AS ENUM(
    'IN_PROGRESS', -- at least one included report is in a non-terminal (START, WAITING) state, processing can continue
    'FINISHED',    -- all reports have reached a terminal state (FINISHED, FAILED, INVALID)
    'ABANDONED'    -- we have given up on the aggregation job entirely
);

-- An aggregation job, representing the aggregation of a number of client reports.
CREATE TABLE aggregation_jobs(
    id                         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal-only
    task_id                    BIGINT NOT NULL,                 -- ID of related task
    aggregation_job_id         BYTEA NOT NULL,                  -- 16-byte AggregationJobID as defined by the DAP specification
    aggregation_param          BYTEA NOT NULL,                  -- encoded aggregation parameter (opaque VDAF message)
    batch_id                   BYTEA NOT NULL,                  -- batch ID (fixed-size only; corresponds to identifier in BatchSelector)
    client_timestamp_interval  TSRANGE NOT NULL,                -- the minimal interval containing all of client timestamps included in this aggregation job
    state                      AGGREGATION_JOB_STATE NOT NULL,  -- current state of the aggregation job
    step                       INTEGER NOT NULL,                -- current step of the aggregation job
    last_request_hash          BYTEA,                           -- SHA-256 hash of the most recently received AggregationJobInitReq or AggregationJobContinueReq (helper only)
    trace_context              JSONB,                           -- distributed tracing metadata

    lease_expiry             TIMESTAMP NOT NULL DEFAULT TIMESTAMP '-infinity',  -- when lease on this aggregation job expires; -infinity implies no current lease
    lease_token              BYTEA,                                             -- a value identifying the current leaseholder; NULL implies no current lease
    lease_attempts           BIGINT NOT NULL DEFAULT 0,                         -- the number of lease acquiries since the last successful lease release

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT aggregation_jobs_unique_id UNIQUE(task_id, aggregation_job_id),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX aggregation_jobs_state_and_lease_expiry ON aggregation_jobs(state, lease_expiry) WHERE state = 'IN_PROGRESS';
CREATE INDEX aggregation_jobs_task_and_batch_id ON aggregation_jobs(task_id, batch_id);
CREATE INDEX aggregation_jobs_task_and_client_timestamp_interval ON aggregation_jobs USING gist (task_id, client_timestamp_interval);

-- Specifies the possible state of aggregating a single report.
CREATE TYPE REPORT_AGGREGATION_STATE AS ENUM(
    'START',     -- the aggregator is waiting to decrypt its input share & compute initial preparation state
    'WAITING',   -- the aggregator is waiting for a message from its peer before proceeding
    'FINISHED',  -- the aggregator has completed the preparation process and recovered an output share
    'FAILED'     -- an error has occurred and an output share cannot be recovered
);

-- An aggregation attempt for a single client report. An aggregation job logically contains a number
-- of report aggregations. A single client report might be aggregated in multiple aggregation jobs &
-- therefore have multiple associated report aggregations.
CREATE TABLE report_aggregations(
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal-only
    task_id             BIGINT NOT NULL,                    -- ID of related task
    aggregation_job_id  BIGINT NOT NULL,                    -- the aggregation job ID this report aggregation is associated with
    client_report_id    BYTEA NOT NULL,                     -- the client report ID this report aggregation is associated with
    client_timestamp    TIMESTAMP NOT NULL,                 -- the client timestamp this report aggregation is associated with
    ord                 BIGINT NOT NULL,                    -- a value used to specify the ordering of client reports in the aggregation job
    state               REPORT_AGGREGATION_STATE NOT NULL,  -- the current state of this report aggregation
    helper_prep_state   BYTEA,                              -- the current VDAF prepare state (opaque VDAF message, only if in state WAITING, only populated for helper)
    leader_prep_transition BYTEA,                           -- the current VDAF prepare transition (opaque VDAF message, only if in state WAITING, only populated for leader)
    error_code          SMALLINT,                           -- error code corresponding to a DAP ReportShareError value; null if in a state other than FAILED
    last_prep_resp      BYTEA,                              -- the last PrepareResp message sent to the Leader, to assist in replay (opaque DAP message, populated for Helper only)

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT report_aggregations_unique_ord UNIQUE(task_id, aggregation_job_id, ord),
    CONSTRAINT fk_task_id FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE CASCADE,
    CONSTRAINT fk_aggregation_job_id FOREIGN KEY(aggregation_job_id) REFERENCES aggregation_jobs(id) ON DELETE CASCADE
);
CREATE INDEX report_aggregations_aggregation_job_id_index ON report_aggregations(aggregation_job_id);
CREATE INDEX report_aggregations_client_report_id_index ON report_aggregations(client_report_id);

-- Specifies the possible state of aggregation for a given batch.
CREATE TYPE BATCH_STATE AS ENUM(
    'OPEN',     -- this batch can accept additional aggregation jobs.
    'CLOSING',  -- this batch can accept additional aggregation jobs, but will transition to CLOSED when there are no outstanding aggregation jobs.
    'CLOSED'    -- this batch can no longer accept additional aggregation jobs.
);

-- Tracks the state of a given batch, by aggregation parameter. Populated for the Leader only.
CREATE TABLE batches(
    id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    task_id                       BIGINT NOT NULL,       -- the task ID
    batch_identifier              BYTEA NOT NULL,        -- encoded query-type-specific batch identifier (corresponds to identifier in BatchSelector)
    batch_interval                TSRANGE,
    aggregation_param             BYTEA NOT NULL,        -- the aggregation parameter (opaque VDAF message)
    state                         BATCH_STATE NOT NULL,  -- the state of aggregations for this batch
    outstanding_aggregation_jobs  BIGINT NOT NULL,       -- the number of outstanding aggregation jobs
    client_timestamp_interval     TSRANGE NOT NULL,

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT batches_unique_id UNIQUE(task_id, batch_identifier, aggregation_param),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

-- Specifies the possible states of a batch aggregation.
CREATE TYPE BATCH_AGGREGATION_STATE AS ENUM(
    'AGGREGATING',  -- this batch aggregation has not been collected & permits further aggregation
    'COLLECTED'     -- this batch aggregation has been collected & no longer permits aggregation
);

-- Information on aggregation for a single batch. This information may be incremental if the VDAF
-- supports incremental aggregation. Each batch's aggregation is sharded via the `ord` column.
CREATE TABLE batch_aggregations(
    id                         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    task_id                    BIGINT NOT NULL,                   -- the task ID
    batch_identifier           BYTEA NOT NULL,                    -- encoded query-type-specific batch identifier (corresponds to identifier in BatchSelector)
    batch_interval             TSRANGE,                           -- batch interval, as a TSRANGE, populated only for time-interval tasks. (will always match batch_identifier)
    aggregation_param          BYTEA NOT NULL,                    -- the aggregation parameter (opaque VDAF message)
    ord                        BIGINT NOT NULL,                   -- the index of this batch aggregation shard, over (task ID, batch_identifier, aggregation_param).
    state                      BATCH_AGGREGATION_STATE NOT NULL,  -- the current state of this batch aggregation
    aggregate_share            BYTEA,                             -- the (possibly-incremental) aggregate share; NULL only if report_count is 0.
    report_count               BIGINT NOT NULL,                   -- the (possibly-incremental) client report count
    client_timestamp_interval  TSRANGE NOT NULL,                  -- the minimal interval containing all of client timestamps included in this batch aggregation
    checksum                   BYTEA NOT NULL,                    -- the (possibly-incremental) checksum

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT batch_aggregations_unique_task_id_batch_id_aggregation_param UNIQUE(task_id, batch_identifier, aggregation_param, ord),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);

-- Specifies the possible state of a collection job.
CREATE TYPE COLLECTION_JOB_STATE AS ENUM(
    'START',        -- this collection job is waiting for reports to be aggregated
    'COLLECTABLE',  -- this collection job is ready to be collected
    'FINISHED',     -- this collection job has run successfully and is ready for collection
    'ABANDONED',    -- this collection job has been abandoned & will never be run again
    'DELETED'       -- this collection job has been deleted
);

-- The leader's view of collect requests from the Collector.
CREATE TABLE collection_jobs(
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- artificial ID, internal-only
    collection_job_id       BYTEA NOT NULL,              -- 16 byte identifier used by collector to refer to this job
    task_id                 BIGINT NOT NULL,             -- the task ID being collected
    query                   BYTEA NOT NULL,              -- encoded query-type-specific query (corresponds to Query)
    aggregation_param       BYTEA NOT NULL,              -- the aggregation parameter (opaque VDAF message)
    batch_identifier        BYTEA NOT NULL,              -- encoded query-type-specific batch identifier (corresponds to identifier in BatchSelector)
    batch_interval          TSRANGE,                     -- batch interval, as a TSRANGE, populated only for time-interval tasks. (will always match batch_identifier)
    state                   COLLECTION_JOB_STATE NOT NULL,  -- the current state of this collection job
    report_count            BIGINT,                      -- the number of reports included in this collection job (only if in state FINISHED)
    helper_aggregate_share  BYTEA,                       -- the helper's encrypted aggregate share (HpkeCiphertext, only if in state FINISHED)
    leader_aggregate_share  BYTEA,                       -- the leader's unencrypted aggregate share (opaque VDAF message, only if in state FINISHED)
    trace_context           JSONB,                       -- distributed tracing metadata

    lease_expiry            TIMESTAMP NOT NULL DEFAULT TIMESTAMP '-infinity',  -- when lease on this collection job expires; -infinity implies no current lease
    lease_token             BYTEA,                                             -- a value identifying the current leaseholder; NULL implies no current lease
    lease_attempts          BIGINT NOT NULL DEFAULT 0,                         -- the number of lease acquiries since the last successful lease release

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_at TIMESTAMP NOT NULL,  -- when the row was last changed
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT collection_jobs_unique_id UNIQUE(task_id, collection_job_id),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX collection_jobs_task_id_batch_id ON collection_jobs(task_id, batch_identifier);
-- TODO(#224): verify that this index is optimal for purposes of acquiring collection jobs.
CREATE INDEX collection_jobs_state_and_lease_expiry ON collection_jobs(state, lease_expiry) WHERE state = 'COLLECTABLE';
CREATE INDEX collection_jobs_interval_containment_index ON collection_jobs USING gist (task_id, batch_interval);

-- The helper's view of aggregate share jobs.
CREATE TABLE aggregate_share_jobs(
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal-only
    task_id                 BIGINT NOT NULL,    -- the task ID being collected
    batch_identifier        BYTEA NOT NULL,     -- encoded query-type-specific batch identifier (corresponds to identifier in BatchSelector)
    batch_interval          TSRANGE,            -- batch interval, as a TSRANGE, populated only for time-interval tasks. (will always match batch_identifier)
    aggregation_param       BYTEA NOT NULL,     -- the aggregation parameter (opaque VDAF message)
    helper_aggregate_share  BYTEA NOT NULL,     -- the helper's unencrypted aggregate share
    report_count            BIGINT NOT NULL,    -- the count of reports included helper_aggregate_share
    checksum                BYTEA NOT NULL,     -- the checksum over the reports included in helper_aggregate_share

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT aggregate_share_jobs_unique_task_id_batch_id_aggregation_param UNIQUE(task_id, batch_identifier, aggregation_param),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX aggregate_share_jobs_interval_containment_index ON aggregate_share_jobs USING gist (task_id, batch_interval);

-- The leader's view of outstanding batches, which are batches which have not yet started
-- collection. Used for fixed-size tasks only.
CREATE TABLE outstanding_batches(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- artificial ID, internal-only
    task_id BIGINT NOT NULL, -- the task ID containing the batch
    batch_id BYTEA NOT NULL, -- 32-byte BatchID as defined by the DAP specification.
    time_bucket_start TIMESTAMP,

    -- creation/update records
    created_at TIMESTAMP NOT NULL,  -- when the row was created
    updated_by TEXT NOT NULL,       -- the name of the transaction that last updated the row

    CONSTRAINT outstanding_batches_unique_task_id_batch_id UNIQUE(task_id, batch_id),
    CONSTRAINT fk_task_id FOREIGN KEY(task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX outstanding_batches_task_and_time_bucket_index ON outstanding_batches (task_id, time_bucket_start);
