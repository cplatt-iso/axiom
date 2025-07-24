--
-- PostgreSQL database dump
--

-- Dumped from database version 15.13
-- Dumped by pg_dump version 15.13

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: crosswalk_db_type_enum; Type: TYPE; Schema: public; Owner: dicom_processor_user
--

CREATE TYPE public.crosswalk_db_type_enum AS ENUM (
    'POSTGRES',
    'MYSQL',
    'MSSQL'
);


ALTER TYPE public.crosswalk_db_type_enum OWNER TO dicom_processor_user;

--
-- Name: crosswalk_sync_status_enum; Type: TYPE; Schema: public; Owner: dicom_processor_user
--

CREATE TYPE public.crosswalk_sync_status_enum AS ENUM (
    'PENDING',
    'SYNCING',
    'SUCCESS',
    'FAILED'
);


ALTER TYPE public.crosswalk_sync_status_enum OWNER TO dicom_processor_user;

--
-- Name: ruleset_execution_mode_enum; Type: TYPE; Schema: public; Owner: dicom_processor_user
--

CREATE TYPE public.ruleset_execution_mode_enum AS ENUM (
    'FIRST_MATCH',
    'ALL_MATCHES'
);


ALTER TYPE public.ruleset_execution_mode_enum OWNER TO dicom_processor_user;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: a_i_prompt_configs; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.a_i_prompt_configs (
    name character varying(255) NOT NULL,
    description text,
    dicom_tag_keyword character varying(100) NOT NULL,
    is_enabled boolean NOT NULL,
    prompt_template text NOT NULL,
    model_identifier character varying(100) NOT NULL,
    model_parameters jsonb,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.a_i_prompt_configs OWNER TO dicom_processor_user;

--
-- Name: COLUMN a_i_prompt_configs.name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.name IS 'Unique, human-readable name for this AI prompt configuration.';


--
-- Name: COLUMN a_i_prompt_configs.description; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.description IS 'Optional detailed description of what this prompt config does.';


--
-- Name: COLUMN a_i_prompt_configs.dicom_tag_keyword; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.dicom_tag_keyword IS 'DICOM keyword of the tag this configuration targets (e.g., BodyPartExamined, PatientSex).';


--
-- Name: COLUMN a_i_prompt_configs.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.is_enabled IS 'Whether this AI prompt configuraiton is active and can be used.';


--
-- Name: COLUMN a_i_prompt_configs.prompt_template; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.prompt_template IS 'The prompt template. Should include ''{value}'' placeholder. Can also use ''{dicom_tag_keyword}''.';


--
-- Name: COLUMN a_i_prompt_configs.model_identifier; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.model_identifier IS 'Identifier for the AI model to be used (e.g., ''gemini-1.5-flash-001'').';


--
-- Name: COLUMN a_i_prompt_configs.model_parameters; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.a_i_prompt_configs.model_parameters IS 'JSON object for model-specific parameters like temperature, max_output_tokens, top_p, etc.';


--
-- Name: a_i_prompt_configs_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.a_i_prompt_configs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.a_i_prompt_configs_id_seq OWNER TO dicom_processor_user;

--
-- Name: a_i_prompt_configs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.a_i_prompt_configs_id_seq OWNED BY public.a_i_prompt_configs.id;


--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.api_keys (
    hashed_key character varying(255) NOT NULL,
    prefix character varying(8) NOT NULL,
    name character varying(100) NOT NULL,
    user_id integer NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    is_active boolean NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.api_keys OWNER TO dicom_processor_user;

--
-- Name: api_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.api_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.api_keys_id_seq OWNER TO dicom_processor_user;

--
-- Name: api_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.api_keys_id_seq OWNED BY public.api_keys.id;


--
-- Name: crosswalk_data_sources; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.crosswalk_data_sources (
    name character varying(100) NOT NULL,
    description text,
    db_type public.crosswalk_db_type_enum NOT NULL,
    connection_details json NOT NULL,
    target_table character varying(255) NOT NULL,
    sync_interval_seconds integer NOT NULL,
    is_enabled boolean NOT NULL,
    last_sync_status public.crosswalk_sync_status_enum NOT NULL,
    last_sync_time timestamp with time zone,
    last_sync_error text,
    last_sync_row_count integer,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.crosswalk_data_sources OWNER TO dicom_processor_user;

--
-- Name: COLUMN crosswalk_data_sources.connection_details; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_data_sources.connection_details IS 'DB connection parameters (host, port, user, password_secret, dbname)';


--
-- Name: COLUMN crosswalk_data_sources.target_table; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_data_sources.target_table IS 'Name of the table or view containing the crosswalk data.';


--
-- Name: COLUMN crosswalk_data_sources.sync_interval_seconds; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_data_sources.sync_interval_seconds IS 'How often to refresh the cache (in seconds).';


--
-- Name: crosswalk_data_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.crosswalk_data_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.crosswalk_data_sources_id_seq OWNER TO dicom_processor_user;

--
-- Name: crosswalk_data_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.crosswalk_data_sources_id_seq OWNED BY public.crosswalk_data_sources.id;


--
-- Name: crosswalk_maps; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.crosswalk_maps (
    name character varying(100) NOT NULL,
    description text,
    is_enabled boolean NOT NULL,
    data_source_id integer NOT NULL,
    match_columns json NOT NULL,
    cache_key_columns json NOT NULL,
    replacement_mapping json NOT NULL,
    cache_ttl_seconds integer,
    on_cache_miss character varying(50) NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.crosswalk_maps OWNER TO dicom_processor_user;

--
-- Name: COLUMN crosswalk_maps.match_columns; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_maps.match_columns IS 'How to match DICOM tags to source table columns. [{"column_name": "mrn", "dicom_tag": "0010,0020"}]';


--
-- Name: COLUMN crosswalk_maps.cache_key_columns; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_maps.cache_key_columns IS 'List of column names from the source table used to build the cache key.';


--
-- Name: COLUMN crosswalk_maps.replacement_mapping; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_maps.replacement_mapping IS 'How to map source table columns to target DICOM tags. [{"source_column": "new_mrn", "dicom_tag": "0010,0020", "dicom_vr": "LO"}]';


--
-- Name: COLUMN crosswalk_maps.cache_ttl_seconds; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_maps.cache_ttl_seconds IS 'Optional TTL for cache entries (overrides source sync interval for lookup).';


--
-- Name: COLUMN crosswalk_maps.on_cache_miss; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.crosswalk_maps.on_cache_miss IS 'Behavior on cache miss (''fail'', ''query_db'', ''log_only'').';


--
-- Name: crosswalk_maps_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.crosswalk_maps_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.crosswalk_maps_id_seq OWNER TO dicom_processor_user;

--
-- Name: crosswalk_maps_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.crosswalk_maps_id_seq OWNED BY public.crosswalk_maps.id;


--
-- Name: dicom_exception_log; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.dicom_exception_log (
    exception_uuid uuid DEFAULT gen_random_uuid() NOT NULL,
    study_instance_uid character varying(128),
    series_instance_uid character varying(128),
    sop_instance_uid character varying(128),
    patient_name character varying(255),
    patient_id character varying(128),
    accession_number character varying(64),
    modality character varying(16),
    failure_timestamp timestamp with time zone DEFAULT now() NOT NULL,
    processing_stage character varying(20) NOT NULL,
    error_message text NOT NULL,
    error_details text,
    failed_filepath character varying(1024),
    original_source_type character varying(17),
    original_source_identifier character varying(255),
    calling_ae_title character varying(16),
    target_destination_id integer,
    target_destination_name character varying(100),
    status character varying(22) DEFAULT 'NEW'::character varying NOT NULL,
    retry_count integer DEFAULT 0 NOT NULL,
    next_retry_attempt_at timestamp with time zone,
    last_retry_attempt_at timestamp with time zone,
    resolved_at timestamp with time zone,
    resolved_by_user_id integer,
    resolution_notes text,
    celery_task_id character varying(255),
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.dicom_exception_log OWNER TO dicom_processor_user;

--
-- Name: COLUMN dicom_exception_log.exception_uuid; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.exception_uuid IS 'Stable, unique identifier for this exception record.';


--
-- Name: COLUMN dicom_exception_log.patient_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.patient_name IS 'Patient''s Name, if available from DICOM header.';


--
-- Name: COLUMN dicom_exception_log.patient_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.patient_id IS 'Patient''s ID, if available from DICOM header.';


--
-- Name: COLUMN dicom_exception_log.accession_number; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.accession_number IS 'Accession Number, if available from DICOM header.';


--
-- Name: COLUMN dicom_exception_log.modality; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.modality IS 'DICOM Modality, if available from DICOM header.';


--
-- Name: COLUMN dicom_exception_log.failure_timestamp; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.failure_timestamp IS 'Timestamp when the failure was logged.';


--
-- Name: COLUMN dicom_exception_log.processing_stage; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.processing_stage IS 'Specific stage in the pipeline where failure occurred.';


--
-- Name: COLUMN dicom_exception_log.error_message; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.error_message IS 'Detailed error message or exception string.';


--
-- Name: COLUMN dicom_exception_log.error_details; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.error_details IS 'Additional details, like traceback or context-specific info.';


--
-- Name: COLUMN dicom_exception_log.failed_filepath; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.failed_filepath IS 'Path to the errored DICOM file, if applicable (e.g., from listener or file upload).';


--
-- Name: COLUMN dicom_exception_log.original_source_type; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.original_source_type IS 'Original source system type that provided the DICOM data.';


--
-- Name: COLUMN dicom_exception_log.original_source_identifier; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.original_source_identifier IS 'Identifier of the original source (e.g., Listener AE Title, DICOMweb source name, QR source ID).';


--
-- Name: COLUMN dicom_exception_log.target_destination_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.target_destination_id IS 'ID of the StorageBackendConfig if failure was during send to a specific destination.';


--
-- Name: COLUMN dicom_exception_log.target_destination_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.target_destination_name IS 'Name of the target destination at the time of failure.';


--
-- Name: COLUMN dicom_exception_log.status; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.status IS 'Current status of this exception record.';


--
-- Name: COLUMN dicom_exception_log.retry_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.retry_count IS 'Number of automatic retry attempts made.';


--
-- Name: COLUMN dicom_exception_log.next_retry_attempt_at; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.next_retry_attempt_at IS 'Timestamp for the next scheduled retry attempt.';


--
-- Name: COLUMN dicom_exception_log.last_retry_attempt_at; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicom_exception_log.last_retry_attempt_at IS 'Timestamp of the last retry attempt.';


--
-- Name: dicom_exception_log_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.dicom_exception_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dicom_exception_log_id_seq OWNER TO dicom_processor_user;

--
-- Name: dicom_exception_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.dicom_exception_log_id_seq OWNED BY public.dicom_exception_log.id;


--
-- Name: dicomweb_source_state; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.dicomweb_source_state (
    source_name character varying NOT NULL,
    description text,
    base_url character varying NOT NULL,
    qido_prefix character varying NOT NULL,
    wado_prefix character varying NOT NULL,
    polling_interval_seconds integer NOT NULL,
    is_enabled boolean NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    auth_type character varying(50) NOT NULL,
    auth_config jsonb,
    search_filters jsonb,
    last_processed_timestamp timestamp with time zone,
    last_successful_run timestamp with time zone,
    last_error_run timestamp with time zone,
    last_error_message text,
    found_instance_count integer DEFAULT 0 NOT NULL,
    queued_instance_count integer DEFAULT 0 NOT NULL,
    processed_instance_count integer DEFAULT 0 NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.dicomweb_source_state OWNER TO dicom_processor_user;

--
-- Name: COLUMN dicomweb_source_state.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicomweb_source_state.is_enabled IS 'Whether this source configuration is generally enabled and available (e.g., for data browser).';


--
-- Name: COLUMN dicomweb_source_state.is_active; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicomweb_source_state.is_active IS 'Whether AUTOMATIC polling for this source is active based on its schedule.';


--
-- Name: COLUMN dicomweb_source_state.found_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicomweb_source_state.found_instance_count IS 'Total count of unique instances found by QIDO across all polls.';


--
-- Name: COLUMN dicomweb_source_state.queued_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicomweb_source_state.queued_instance_count IS 'Total count of instances actually queued for metadata processing.';


--
-- Name: COLUMN dicomweb_source_state.processed_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dicomweb_source_state.processed_instance_count IS 'Total count of instances successfully processed after being queued.';


--
-- Name: dicomweb_source_state_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.dicomweb_source_state_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dicomweb_source_state_id_seq OWNER TO dicom_processor_user;

--
-- Name: dicomweb_source_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.dicomweb_source_state_id_seq OWNED BY public.dicomweb_source_state.id;


--
-- Name: dimse_listener_configs; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.dimse_listener_configs (
    name character varying(100) NOT NULL,
    description text,
    ae_title character varying(16) NOT NULL,
    port integer NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    instance_id character varying(255),
    tls_enabled boolean DEFAULT false NOT NULL,
    tls_cert_secret_name character varying(512),
    tls_key_secret_name character varying(512),
    tls_ca_cert_secret_name character varying(512),
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.dimse_listener_configs OWNER TO dicom_processor_user;

--
-- Name: COLUMN dimse_listener_configs.name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.name IS 'Unique, user-friendly name for this listener configuration.';


--
-- Name: COLUMN dimse_listener_configs.description; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.description IS 'Optional description of the listener''s purpose.';


--
-- Name: COLUMN dimse_listener_configs.ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.ae_title IS 'The Application Entity Title the listener will use.';


--
-- Name: COLUMN dimse_listener_configs.port; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.port IS 'The network port the listener will bind to.';


--
-- Name: COLUMN dimse_listener_configs.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.is_enabled IS 'Whether this listener configuration is active and should be started.';


--
-- Name: COLUMN dimse_listener_configs.instance_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.instance_id IS 'Unique ID matching AXIOM_INSTANCE_ID env var of the listener process using this config.';


--
-- Name: COLUMN dimse_listener_configs.tls_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.tls_enabled IS 'Enable TLS for incoming connections to this listener.';


--
-- Name: COLUMN dimse_listener_configs.tls_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.tls_cert_secret_name IS 'REQUIRED if TLS enabled: Secret Manager resource name for the server''s TLS certificate (PEM).';


--
-- Name: COLUMN dimse_listener_configs.tls_key_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.tls_key_secret_name IS 'REQUIRED if TLS enabled: Secret Manager resource name for the server''s TLS private key (PEM).';


--
-- Name: COLUMN dimse_listener_configs.tls_ca_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_configs.tls_ca_cert_secret_name IS 'Optional (for mTLS): Secret Manager resource name for the CA certificate (PEM) used to verify client certificates.';


--
-- Name: dimse_listener_configs_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.dimse_listener_configs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dimse_listener_configs_id_seq OWNER TO dicom_processor_user;

--
-- Name: dimse_listener_configs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.dimse_listener_configs_id_seq OWNED BY public.dimse_listener_configs.id;


--
-- Name: dimse_listener_state; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.dimse_listener_state (
    id integer NOT NULL,
    listener_id character varying NOT NULL,
    status character varying NOT NULL,
    status_message text,
    host character varying,
    port integer,
    ae_title character varying,
    last_heartbeat timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    received_instance_count integer DEFAULT 0 NOT NULL,
    processed_instance_count integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.dimse_listener_state OWNER TO dicom_processor_user;

--
-- Name: COLUMN dimse_listener_state.received_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_state.received_instance_count IS 'Total count of instances received by this listener.';


--
-- Name: COLUMN dimse_listener_state.processed_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_listener_state.processed_instance_count IS 'Total count of instances successfully processed after reception.';


--
-- Name: dimse_listener_state_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.dimse_listener_state_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dimse_listener_state_id_seq OWNER TO dicom_processor_user;

--
-- Name: dimse_listener_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.dimse_listener_state_id_seq OWNED BY public.dimse_listener_state.id;


--
-- Name: dimse_qr_sources; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.dimse_qr_sources (
    name character varying(100) NOT NULL,
    description text,
    remote_ae_title character varying(16) NOT NULL,
    remote_host character varying(255) NOT NULL,
    remote_port integer NOT NULL,
    local_ae_title character varying(16) NOT NULL,
    tls_enabled boolean DEFAULT false NOT NULL,
    tls_ca_cert_secret_name character varying(512),
    tls_client_cert_secret_name character varying(512),
    tls_client_key_secret_name character varying(512),
    polling_interval_seconds integer NOT NULL,
    is_enabled boolean NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    query_level character varying(10) NOT NULL,
    query_filters json,
    move_destination_ae_title character varying(16),
    last_successful_query timestamp with time zone,
    last_successful_move timestamp with time zone,
    last_error_time timestamp with time zone,
    last_error_message text,
    found_study_count integer DEFAULT 0 NOT NULL,
    move_queued_study_count integer DEFAULT 0 NOT NULL,
    processed_instance_count integer DEFAULT 0 NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.dimse_qr_sources OWNER TO dicom_processor_user;

--
-- Name: COLUMN dimse_qr_sources.name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.name IS 'Unique, user-friendly name for this remote DIMSE peer configuration.';


--
-- Name: COLUMN dimse_qr_sources.description; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.description IS 'Optional description of the remote peer or its purpose.';


--
-- Name: COLUMN dimse_qr_sources.remote_ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.remote_ae_title IS 'AE Title of the remote peer to query/retrieve from.';


--
-- Name: COLUMN dimse_qr_sources.remote_host; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.remote_host IS 'Hostname or IP address of the remote peer.';


--
-- Name: COLUMN dimse_qr_sources.remote_port; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.remote_port IS 'Network port of the remote peer''s DIMSE service.';


--
-- Name: COLUMN dimse_qr_sources.local_ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.local_ae_title IS 'AE Title our SCU will use when associating with the remote peer.';


--
-- Name: COLUMN dimse_qr_sources.tls_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.tls_enabled IS 'Enable TLS for outgoing connections to the remote peer.';


--
-- Name: COLUMN dimse_qr_sources.tls_ca_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.tls_ca_cert_secret_name IS 'REQUIRED for TLS: Secret Manager resource name for the CA certificate (PEM) used to verify the remote peer''s server certificate.';


--
-- Name: COLUMN dimse_qr_sources.tls_client_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.tls_client_cert_secret_name IS 'Optional (for mTLS): Secret Manager resource name for OUR client certificate (PEM).';


--
-- Name: COLUMN dimse_qr_sources.tls_client_key_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.tls_client_key_secret_name IS 'Optional (for mTLS): Secret Manager resource name for OUR client private key (PEM).';


--
-- Name: COLUMN dimse_qr_sources.polling_interval_seconds; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.polling_interval_seconds IS 'Frequency in seconds at which to poll the source using C-FIND.';


--
-- Name: COLUMN dimse_qr_sources.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.is_enabled IS 'Whether this source configuration is generally enabled and available (e.g., for data browser).';


--
-- Name: COLUMN dimse_qr_sources.is_active; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.is_active IS 'Whether AUTOMATIC polling for this source is active based on its schedule.';


--
-- Name: COLUMN dimse_qr_sources.query_level; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.query_level IS 'Query Retrieve Level for C-FIND (e.g., STUDY).';


--
-- Name: COLUMN dimse_qr_sources.query_filters; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.query_filters IS 'JSON object containing key-value pairs for C-FIND query identifiers.';


--
-- Name: COLUMN dimse_qr_sources.move_destination_ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.move_destination_ae_title IS 'AE Title of OUR listener where retrieved instances should be sent via C-MOVE.';


--
-- Name: COLUMN dimse_qr_sources.last_successful_query; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.last_successful_query IS 'Timestamp of the last successful C-FIND query.';


--
-- Name: COLUMN dimse_qr_sources.last_successful_move; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.last_successful_move IS 'Timestamp of the last successful C-MOVE request completion (if applicable).';


--
-- Name: COLUMN dimse_qr_sources.last_error_time; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.last_error_time IS 'Timestamp of the last error encountered during polling or retrieval.';


--
-- Name: COLUMN dimse_qr_sources.last_error_message; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.last_error_message IS 'Details of the last error encountered.';


--
-- Name: COLUMN dimse_qr_sources.found_study_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.found_study_count IS 'Total count of unique studies found by C-FIND across all polls.';


--
-- Name: COLUMN dimse_qr_sources.move_queued_study_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.move_queued_study_count IS 'Total count of studies actually queued for C-MOVE retrieval.';


--
-- Name: COLUMN dimse_qr_sources.processed_instance_count; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.dimse_qr_sources.processed_instance_count IS 'Total count of instances successfully processed after C-MOVE.';


--
-- Name: dimse_qr_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.dimse_qr_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.dimse_qr_sources_id_seq OWNER TO dicom_processor_user;

--
-- Name: dimse_qr_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.dimse_qr_sources_id_seq OWNED BY public.dimse_qr_sources.id;


--
-- Name: google_healthcare_sources; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.google_healthcare_sources (
    name character varying NOT NULL,
    description character varying,
    gcp_project_id character varying NOT NULL,
    gcp_location character varying NOT NULL,
    gcp_dataset_id character varying NOT NULL,
    gcp_dicom_store_id character varying NOT NULL,
    polling_interval_seconds integer NOT NULL,
    query_filters json,
    is_enabled boolean NOT NULL,
    is_active boolean NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.google_healthcare_sources OWNER TO dicom_processor_user;

--
-- Name: google_healthcare_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.google_healthcare_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.google_healthcare_sources_id_seq OWNER TO dicom_processor_user;

--
-- Name: google_healthcare_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.google_healthcare_sources_id_seq OWNED BY public.google_healthcare_sources.id;


--
-- Name: imaging_orders; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.imaging_orders (
    patient_name character varying(255),
    patient_id character varying(128) NOT NULL,
    patient_dob date,
    patient_sex character varying(16),
    accession_number character varying(64) NOT NULL,
    placer_order_number character varying(128),
    filler_order_number character varying(128),
    requested_procedure_description character varying(255),
    requested_procedure_code character varying(64),
    modality character varying(16) NOT NULL,
    scheduled_station_ae_title character varying(16),
    scheduled_station_name character varying(128),
    scheduled_procedure_step_start_datetime timestamp with time zone,
    requesting_physician character varying(255),
    referring_physician character varying(255),
    order_status character varying(12) DEFAULT 'SCHEDULED'::character varying NOT NULL,
    study_instance_uid character varying(128),
    source character varying(255) NOT NULL,
    order_received_at timestamp with time zone DEFAULT now() NOT NULL,
    raw_hl7_message text,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.imaging_orders OWNER TO dicom_processor_user;

--
-- Name: COLUMN imaging_orders.patient_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.imaging_orders.patient_id IS 'MRN';


--
-- Name: COLUMN imaging_orders.accession_number; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.imaging_orders.accession_number IS 'The critical, unique identifier for the study.';


--
-- Name: COLUMN imaging_orders.study_instance_uid; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.imaging_orders.study_instance_uid IS 'Will be generated on first C-FIND, unless provided.';


--
-- Name: COLUMN imaging_orders.source; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.imaging_orders.source IS 'Identifier for the source system, e.g., ''HL7v2_MLLP_LISTENER_MAIN''';


--
-- Name: COLUMN imaging_orders.raw_hl7_message; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.imaging_orders.raw_hl7_message IS 'For debugging the fuck-ups.';


--
-- Name: imaging_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.imaging_orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.imaging_orders_id_seq OWNER TO dicom_processor_user;

--
-- Name: imaging_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.imaging_orders_id_seq OWNED BY public.imaging_orders.id;


--
-- Name: processed_study_log; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.processed_study_log (
    source_type character varying(17) NOT NULL,
    source_id character varying(255) NOT NULL,
    study_instance_uid character varying(128) NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    retrieval_queued_at timestamp with time zone DEFAULT now() NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.processed_study_log OWNER TO dicom_processor_user;

--
-- Name: COLUMN processed_study_log.source_type; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.processed_study_log.source_type IS 'Type of the source that initiated processing (DICOMWEB, DIMSE_QR, DIMSE_LISTENER).';


--
-- Name: COLUMN processed_study_log.source_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.processed_study_log.source_id IS 'Identifier of the source config (int ID for DICOMweb/QR, string instance_id for Listener).';


--
-- Name: COLUMN processed_study_log.study_instance_uid; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.processed_study_log.study_instance_uid IS 'The Study Instance UID that was processed/queued.';


--
-- Name: COLUMN processed_study_log.first_seen_at; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.processed_study_log.first_seen_at IS 'Timestamp when the study was first encountered by this source.';


--
-- Name: COLUMN processed_study_log.retrieval_queued_at; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.processed_study_log.retrieval_queued_at IS 'Timestamp when the retrieval/processing task was queued.';


--
-- Name: processed_study_log_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.processed_study_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.processed_study_log_id_seq OWNER TO dicom_processor_user;

--
-- Name: processed_study_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.processed_study_log_id_seq OWNED BY public.processed_study_log.id;


--
-- Name: roles; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.roles (
    name character varying(50) NOT NULL,
    description character varying(255),
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.roles OWNER TO dicom_processor_user;

--
-- Name: roles_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.roles_id_seq OWNER TO dicom_processor_user;

--
-- Name: roles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.roles_id_seq OWNED BY public.roles.id;


--
-- Name: rule_destination_association; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.rule_destination_association (
    rule_id integer NOT NULL,
    storage_backend_config_id integer NOT NULL
);


ALTER TABLE public.rule_destination_association OWNER TO dicom_processor_user;

--
-- Name: rule_sets; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.rule_sets (
    name character varying(100) NOT NULL,
    description text,
    is_active boolean NOT NULL,
    priority integer NOT NULL,
    execution_mode public.ruleset_execution_mode_enum NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.rule_sets OWNER TO dicom_processor_user;

--
-- Name: COLUMN rule_sets.priority; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rule_sets.priority IS 'Lower numbers execute first';


--
-- Name: rule_sets_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.rule_sets_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.rule_sets_id_seq OWNER TO dicom_processor_user;

--
-- Name: rule_sets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.rule_sets_id_seq OWNED BY public.rule_sets.id;


--
-- Name: rules; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.rules (
    name character varying(100) NOT NULL,
    description text,
    is_active boolean NOT NULL,
    priority integer NOT NULL,
    ruleset_id integer NOT NULL,
    match_criteria jsonb NOT NULL,
    association_criteria jsonb,
    tag_modifications jsonb NOT NULL,
    applicable_sources jsonb,
    ai_prompt_config_ids jsonb,
    schedule_id integer,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.rules OWNER TO dicom_processor_user;

--
-- Name: COLUMN rules.priority; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.priority IS 'Lower numbers execute first within a ruleset';


--
-- Name: COLUMN rules.match_criteria; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.match_criteria IS 'Criteria list (structure defined/validated by Pydantic schema)';


--
-- Name: COLUMN rules.association_criteria; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.association_criteria IS 'Optional list of criteria based on association details (Calling AE, IP).';


--
-- Name: COLUMN rules.tag_modifications; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.tag_modifications IS 'List of modification action objects (validated by Pydantic)';


--
-- Name: COLUMN rules.applicable_sources; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.applicable_sources IS 'List of source identifiers. Applies to all if null/empty.';


--
-- Name: COLUMN rules.ai_prompt_config_ids; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.ai_prompt_config_ids IS 'List of AIPromptConfig IDs to be used for AI vocabulary standardization for this rule.';


--
-- Name: COLUMN rules.schedule_id; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.rules.schedule_id IS 'Optional ID of the Schedule controlling when this rule is active.';


--
-- Name: rules_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.rules_id_seq OWNER TO dicom_processor_user;

--
-- Name: rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.rules_id_seq OWNED BY public.rules.id;


--
-- Name: schedules; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.schedules (
    name character varying(100) NOT NULL,
    description text,
    is_enabled boolean NOT NULL,
    time_ranges json NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.schedules OWNER TO dicom_processor_user;

--
-- Name: COLUMN schedules.name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.schedules.name IS 'Unique, user-friendly name for this schedule (e.g., ''Overnight Telerad Hours'').';


--
-- Name: COLUMN schedules.description; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.schedules.description IS 'Optional description of when this schedule is active.';


--
-- Name: COLUMN schedules.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.schedules.is_enabled IS 'Whether this schedule definition is active and can be assigned to rules.';


--
-- Name: COLUMN schedules.time_ranges; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.schedules.time_ranges IS 'List of time range definitions (days, start_time HH:MM, end_time HH:MM).';


--
-- Name: schedules_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.schedules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.schedules_id_seq OWNER TO dicom_processor_user;

--
-- Name: schedules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.schedules_id_seq OWNED BY public.schedules.id;


--
-- Name: storage_backend_configs; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.storage_backend_configs (
    name character varying(100) NOT NULL,
    description text,
    backend_type character varying(50) NOT NULL,
    is_enabled boolean NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    path character varying(512),
    bucket character varying(255),
    prefix character varying(512),
    remote_ae_title character varying(16),
    remote_host character varying(255),
    remote_port integer,
    local_ae_title character varying(16),
    tls_enabled boolean DEFAULT false,
    tls_ca_cert_secret_name character varying(512),
    tls_client_cert_secret_name character varying(512),
    tls_client_key_secret_name character varying(512),
    gcp_project_id character varying(255),
    gcp_location character varying(100),
    gcp_dataset_id character varying(100),
    gcp_dicom_store_id character varying(100),
    base_url character varying(512),
    auth_type character varying(50),
    basic_auth_username_secret_name character varying(512),
    basic_auth_password_secret_name character varying(512),
    bearer_token_secret_name character varying(512),
    api_key_secret_name character varying(512),
    api_key_header_name_override character varying(100)
);


ALTER TABLE public.storage_backend_configs OWNER TO dicom_processor_user;

--
-- Name: COLUMN storage_backend_configs.name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.name IS 'Unique, user-friendly name for this storage backend configuration.';


--
-- Name: COLUMN storage_backend_configs.description; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.description IS 'Optional description of the backend''s purpose or location.';


--
-- Name: COLUMN storage_backend_configs.backend_type; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.backend_type IS 'Discriminator: Identifier for the type of storage backend.';


--
-- Name: COLUMN storage_backend_configs.is_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.is_enabled IS 'Whether this storage backend configuration is active and usable in rules.';


--
-- Name: COLUMN storage_backend_configs.path; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.path IS 'Path to the directory for storing files.';


--
-- Name: COLUMN storage_backend_configs.bucket; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.bucket IS 'Name of the GCS bucket.';


--
-- Name: COLUMN storage_backend_configs.prefix; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.prefix IS 'Optional prefix (folder path) within the bucket.';


--
-- Name: COLUMN storage_backend_configs.remote_ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.remote_ae_title IS 'AE Title of the remote C-STORE SCP.';


--
-- Name: COLUMN storage_backend_configs.remote_host; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.remote_host IS 'Hostname or IP address of the remote SCP.';


--
-- Name: COLUMN storage_backend_configs.remote_port; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.remote_port IS 'Network port of the remote SCP.';


--
-- Name: COLUMN storage_backend_configs.local_ae_title; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.local_ae_title IS 'AE Title OUR SCU will use when associating.';


--
-- Name: COLUMN storage_backend_configs.tls_enabled; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.tls_enabled IS 'Enable TLS for outgoing connections to the remote peer.';


--
-- Name: COLUMN storage_backend_configs.tls_ca_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.tls_ca_cert_secret_name IS 'Optional: Secret Manager resource name for a custom CA certificate (PEM) to verify the STOW-RS server.';


--
-- Name: COLUMN storage_backend_configs.tls_client_cert_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.tls_client_cert_secret_name IS 'Optional (for mTLS): Secret Manager resource name for OUR client certificate (PEM).';


--
-- Name: COLUMN storage_backend_configs.tls_client_key_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.tls_client_key_secret_name IS 'Optional (for mTLS): Secret Manager resource name for OUR client private key (PEM).';


--
-- Name: COLUMN storage_backend_configs.base_url; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.base_url IS 'Base URL of the STOW-RS service (e.g., https://dicom.server.com/dicomweb).';


--
-- Name: COLUMN storage_backend_configs.auth_type; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.auth_type IS 'Authentication type for the STOW-RS endpoint.';


--
-- Name: COLUMN storage_backend_configs.basic_auth_username_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.basic_auth_username_secret_name IS 'GCP Secret Manager name for Basic Auth username. Required if auth_type is ''basic''.';


--
-- Name: COLUMN storage_backend_configs.basic_auth_password_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.basic_auth_password_secret_name IS 'GCP Secret Manager name for Basic Auth password. Required if auth_type is ''basic''.';


--
-- Name: COLUMN storage_backend_configs.bearer_token_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.bearer_token_secret_name IS 'GCP Secret Manager name for Bearer token. Required if auth_type is ''bearer''.';


--
-- Name: COLUMN storage_backend_configs.api_key_secret_name; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.api_key_secret_name IS 'GCP Secret Manager name for the API key. Required if auth_type is ''apikey''.';


--
-- Name: COLUMN storage_backend_configs.api_key_header_name_override; Type: COMMENT; Schema: public; Owner: dicom_processor_user
--

COMMENT ON COLUMN public.storage_backend_configs.api_key_header_name_override IS 'Header name for the API key (e.g., ''X-API-Key''). Required if auth_type is ''apikey''.';


--
-- Name: storage_backend_configs_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.storage_backend_configs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.storage_backend_configs_id_seq OWNER TO dicom_processor_user;

--
-- Name: storage_backend_configs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.storage_backend_configs_id_seq OWNED BY public.storage_backend_configs.id;


--
-- Name: user_role_association; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.user_role_association (
    user_id integer NOT NULL,
    role_id integer NOT NULL
);


ALTER TABLE public.user_role_association OWNER TO dicom_processor_user;

--
-- Name: users; Type: TABLE; Schema: public; Owner: dicom_processor_user
--

CREATE TABLE public.users (
    email character varying(255) NOT NULL,
    google_id character varying(255),
    full_name character varying(200),
    picture character varying(512),
    hashed_password character varying(255) NOT NULL,
    is_active boolean NOT NULL,
    is_superuser boolean NOT NULL,
    id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.users OWNER TO dicom_processor_user;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: dicom_processor_user
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.users_id_seq OWNER TO dicom_processor_user;

--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: dicom_processor_user
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: a_i_prompt_configs id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.a_i_prompt_configs ALTER COLUMN id SET DEFAULT nextval('public.a_i_prompt_configs_id_seq'::regclass);


--
-- Name: api_keys id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.api_keys ALTER COLUMN id SET DEFAULT nextval('public.api_keys_id_seq'::regclass);


--
-- Name: crosswalk_data_sources id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.crosswalk_data_sources ALTER COLUMN id SET DEFAULT nextval('public.crosswalk_data_sources_id_seq'::regclass);


--
-- Name: crosswalk_maps id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.crosswalk_maps ALTER COLUMN id SET DEFAULT nextval('public.crosswalk_maps_id_seq'::regclass);


--
-- Name: dicom_exception_log id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dicom_exception_log ALTER COLUMN id SET DEFAULT nextval('public.dicom_exception_log_id_seq'::regclass);


--
-- Name: dicomweb_source_state id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dicomweb_source_state ALTER COLUMN id SET DEFAULT nextval('public.dicomweb_source_state_id_seq'::regclass);


--
-- Name: dimse_listener_configs id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_listener_configs ALTER COLUMN id SET DEFAULT nextval('public.dimse_listener_configs_id_seq'::regclass);


--
-- Name: dimse_listener_state id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_listener_state ALTER COLUMN id SET DEFAULT nextval('public.dimse_listener_state_id_seq'::regclass);


--
-- Name: dimse_qr_sources id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_qr_sources ALTER COLUMN id SET DEFAULT nextval('public.dimse_qr_sources_id_seq'::regclass);


--
-- Name: google_healthcare_sources id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.google_healthcare_sources ALTER COLUMN id SET DEFAULT nextval('public.google_healthcare_sources_id_seq'::regclass);


--
-- Name: imaging_orders id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.imaging_orders ALTER COLUMN id SET DEFAULT nextval('public.imaging_orders_id_seq'::regclass);


--
-- Name: processed_study_log id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.processed_study_log ALTER COLUMN id SET DEFAULT nextval('public.processed_study_log_id_seq'::regclass);


--
-- Name: roles id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.roles ALTER COLUMN id SET DEFAULT nextval('public.roles_id_seq'::regclass);


--
-- Name: rule_sets id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rule_sets ALTER COLUMN id SET DEFAULT nextval('public.rule_sets_id_seq'::regclass);


--
-- Name: rules id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rules ALTER COLUMN id SET DEFAULT nextval('public.rules_id_seq'::regclass);


--
-- Name: schedules id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.schedules ALTER COLUMN id SET DEFAULT nextval('public.schedules_id_seq'::regclass);


--
-- Name: storage_backend_configs id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.storage_backend_configs ALTER COLUMN id SET DEFAULT nextval('public.storage_backend_configs_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: a_i_prompt_configs pk_a_i_prompt_configs; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.a_i_prompt_configs
    ADD CONSTRAINT pk_a_i_prompt_configs PRIMARY KEY (id);


--
-- Name: api_keys pk_api_keys; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT pk_api_keys PRIMARY KEY (id);


--
-- Name: crosswalk_data_sources pk_crosswalk_data_sources; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.crosswalk_data_sources
    ADD CONSTRAINT pk_crosswalk_data_sources PRIMARY KEY (id);


--
-- Name: crosswalk_maps pk_crosswalk_maps; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.crosswalk_maps
    ADD CONSTRAINT pk_crosswalk_maps PRIMARY KEY (id);


--
-- Name: dicom_exception_log pk_dicom_exception_log; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dicom_exception_log
    ADD CONSTRAINT pk_dicom_exception_log PRIMARY KEY (id);


--
-- Name: dicomweb_source_state pk_dicomweb_source_state; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dicomweb_source_state
    ADD CONSTRAINT pk_dicomweb_source_state PRIMARY KEY (id);


--
-- Name: dimse_listener_configs pk_dimse_listener_configs; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_listener_configs
    ADD CONSTRAINT pk_dimse_listener_configs PRIMARY KEY (id);


--
-- Name: dimse_listener_state pk_dimse_listener_state; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_listener_state
    ADD CONSTRAINT pk_dimse_listener_state PRIMARY KEY (id);


--
-- Name: dimse_qr_sources pk_dimse_qr_sources; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.dimse_qr_sources
    ADD CONSTRAINT pk_dimse_qr_sources PRIMARY KEY (id);


--
-- Name: google_healthcare_sources pk_google_healthcare_sources; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.google_healthcare_sources
    ADD CONSTRAINT pk_google_healthcare_sources PRIMARY KEY (id);


--
-- Name: imaging_orders pk_imaging_orders; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.imaging_orders
    ADD CONSTRAINT pk_imaging_orders PRIMARY KEY (id);


--
-- Name: processed_study_log pk_processed_study_log; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.processed_study_log
    ADD CONSTRAINT pk_processed_study_log PRIMARY KEY (id);


--
-- Name: roles pk_roles; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.roles
    ADD CONSTRAINT pk_roles PRIMARY KEY (id);


--
-- Name: rule_destination_association pk_rule_destination_association; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rule_destination_association
    ADD CONSTRAINT pk_rule_destination_association PRIMARY KEY (rule_id, storage_backend_config_id);


--
-- Name: rule_sets pk_rule_sets; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rule_sets
    ADD CONSTRAINT pk_rule_sets PRIMARY KEY (id);


--
-- Name: rules pk_rules; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rules
    ADD CONSTRAINT pk_rules PRIMARY KEY (id);


--
-- Name: schedules pk_schedules; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.schedules
    ADD CONSTRAINT pk_schedules PRIMARY KEY (id);


--
-- Name: storage_backend_configs pk_storage_backend_configs; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.storage_backend_configs
    ADD CONSTRAINT pk_storage_backend_configs PRIMARY KEY (id);


--
-- Name: user_role_association pk_user_role_association; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.user_role_association
    ADD CONSTRAINT pk_user_role_association PRIMARY KEY (user_id, role_id);


--
-- Name: users pk_users; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT pk_users PRIMARY KEY (id);


--
-- Name: processed_study_log uq_processed_study_source_uid; Type: CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.processed_study_log
    ADD CONSTRAINT uq_processed_study_source_uid UNIQUE (source_type, source_id, study_instance_uid);


--
-- Name: ix_a_i_prompt_configs_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_a_i_prompt_configs_created_at ON public.a_i_prompt_configs USING btree (created_at);


--
-- Name: ix_a_i_prompt_configs_dicom_tag_keyword; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_a_i_prompt_configs_dicom_tag_keyword ON public.a_i_prompt_configs USING btree (dicom_tag_keyword);


--
-- Name: ix_a_i_prompt_configs_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_a_i_prompt_configs_id ON public.a_i_prompt_configs USING btree (id);


--
-- Name: ix_a_i_prompt_configs_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_a_i_prompt_configs_is_enabled ON public.a_i_prompt_configs USING btree (is_enabled);


--
-- Name: ix_a_i_prompt_configs_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_a_i_prompt_configs_name ON public.a_i_prompt_configs USING btree (name);


--
-- Name: ix_a_i_prompt_configs_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_a_i_prompt_configs_updated_at ON public.a_i_prompt_configs USING btree (updated_at);


--
-- Name: ix_api_keys_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_api_keys_created_at ON public.api_keys USING btree (created_at);


--
-- Name: ix_api_keys_hashed_key; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_api_keys_hashed_key ON public.api_keys USING btree (hashed_key);


--
-- Name: ix_api_keys_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_api_keys_id ON public.api_keys USING btree (id);


--
-- Name: ix_api_keys_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_api_keys_name ON public.api_keys USING btree (name);


--
-- Name: ix_api_keys_prefix; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_api_keys_prefix ON public.api_keys USING btree (prefix);


--
-- Name: ix_api_keys_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_api_keys_updated_at ON public.api_keys USING btree (updated_at);


--
-- Name: ix_crosswalk_data_sources_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_data_sources_created_at ON public.crosswalk_data_sources USING btree (created_at);


--
-- Name: ix_crosswalk_data_sources_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_data_sources_id ON public.crosswalk_data_sources USING btree (id);


--
-- Name: ix_crosswalk_data_sources_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_data_sources_is_enabled ON public.crosswalk_data_sources USING btree (is_enabled);


--
-- Name: ix_crosswalk_data_sources_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_crosswalk_data_sources_name ON public.crosswalk_data_sources USING btree (name);


--
-- Name: ix_crosswalk_data_sources_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_data_sources_updated_at ON public.crosswalk_data_sources USING btree (updated_at);


--
-- Name: ix_crosswalk_maps_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_maps_created_at ON public.crosswalk_maps USING btree (created_at);


--
-- Name: ix_crosswalk_maps_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_maps_id ON public.crosswalk_maps USING btree (id);


--
-- Name: ix_crosswalk_maps_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_maps_is_enabled ON public.crosswalk_maps USING btree (is_enabled);


--
-- Name: ix_crosswalk_maps_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_crosswalk_maps_name ON public.crosswalk_maps USING btree (name);


--
-- Name: ix_crosswalk_maps_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_crosswalk_maps_updated_at ON public.crosswalk_maps USING btree (updated_at);


--
-- Name: ix_dicom_exception_log_accession_number; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_accession_number ON public.dicom_exception_log USING btree (accession_number);


--
-- Name: ix_dicom_exception_log_celery_task_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_celery_task_id ON public.dicom_exception_log USING btree (celery_task_id);


--
-- Name: ix_dicom_exception_log_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_created_at ON public.dicom_exception_log USING btree (created_at);


--
-- Name: ix_dicom_exception_log_exception_uuid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dicom_exception_log_exception_uuid ON public.dicom_exception_log USING btree (exception_uuid);


--
-- Name: ix_dicom_exception_log_failure_timestamp; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_failure_timestamp ON public.dicom_exception_log USING btree (failure_timestamp);


--
-- Name: ix_dicom_exception_log_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_id ON public.dicom_exception_log USING btree (id);


--
-- Name: ix_dicom_exception_log_modality; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_modality ON public.dicom_exception_log USING btree (modality);


--
-- Name: ix_dicom_exception_log_next_retry_attempt_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_next_retry_attempt_at ON public.dicom_exception_log USING btree (next_retry_attempt_at);


--
-- Name: ix_dicom_exception_log_original_source_identifier; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_original_source_identifier ON public.dicom_exception_log USING btree (original_source_identifier);


--
-- Name: ix_dicom_exception_log_original_source_type; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_original_source_type ON public.dicom_exception_log USING btree (original_source_type);


--
-- Name: ix_dicom_exception_log_patient_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_patient_id ON public.dicom_exception_log USING btree (patient_id);


--
-- Name: ix_dicom_exception_log_patient_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_patient_name ON public.dicom_exception_log USING btree (patient_name);


--
-- Name: ix_dicom_exception_log_processing_stage; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_processing_stage ON public.dicom_exception_log USING btree (processing_stage);


--
-- Name: ix_dicom_exception_log_resolved_by_user_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_resolved_by_user_id ON public.dicom_exception_log USING btree (resolved_by_user_id);


--
-- Name: ix_dicom_exception_log_series_instance_uid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_series_instance_uid ON public.dicom_exception_log USING btree (series_instance_uid);


--
-- Name: ix_dicom_exception_log_sop_instance_uid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_sop_instance_uid ON public.dicom_exception_log USING btree (sop_instance_uid);


--
-- Name: ix_dicom_exception_log_status; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_status ON public.dicom_exception_log USING btree (status);


--
-- Name: ix_dicom_exception_log_status_next_retry; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_status_next_retry ON public.dicom_exception_log USING btree (status, next_retry_attempt_at);


--
-- Name: ix_dicom_exception_log_study_instance_uid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_study_instance_uid ON public.dicom_exception_log USING btree (study_instance_uid);


--
-- Name: ix_dicom_exception_log_target_destination_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_target_destination_id ON public.dicom_exception_log USING btree (target_destination_id);


--
-- Name: ix_dicom_exception_log_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicom_exception_log_updated_at ON public.dicom_exception_log USING btree (updated_at);


--
-- Name: ix_dicomweb_source_state_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicomweb_source_state_created_at ON public.dicomweb_source_state USING btree (created_at);


--
-- Name: ix_dicomweb_source_state_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicomweb_source_state_id ON public.dicomweb_source_state USING btree (id);


--
-- Name: ix_dicomweb_source_state_is_active; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicomweb_source_state_is_active ON public.dicomweb_source_state USING btree (is_active);


--
-- Name: ix_dicomweb_source_state_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicomweb_source_state_is_enabled ON public.dicomweb_source_state USING btree (is_enabled);


--
-- Name: ix_dicomweb_source_state_source_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dicomweb_source_state_source_name ON public.dicomweb_source_state USING btree (source_name);


--
-- Name: ix_dicomweb_source_state_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dicomweb_source_state_updated_at ON public.dicomweb_source_state USING btree (updated_at);


--
-- Name: ix_dimse_listener_configs_ae_title; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_ae_title ON public.dimse_listener_configs USING btree (ae_title);


--
-- Name: ix_dimse_listener_configs_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_created_at ON public.dimse_listener_configs USING btree (created_at);


--
-- Name: ix_dimse_listener_configs_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_id ON public.dimse_listener_configs USING btree (id);


--
-- Name: ix_dimse_listener_configs_instance_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dimse_listener_configs_instance_id ON public.dimse_listener_configs USING btree (instance_id);


--
-- Name: ix_dimse_listener_configs_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_is_enabled ON public.dimse_listener_configs USING btree (is_enabled);


--
-- Name: ix_dimse_listener_configs_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dimse_listener_configs_name ON public.dimse_listener_configs USING btree (name);


--
-- Name: ix_dimse_listener_configs_port; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_port ON public.dimse_listener_configs USING btree (port);


--
-- Name: ix_dimse_listener_configs_tls_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_tls_enabled ON public.dimse_listener_configs USING btree (tls_enabled);


--
-- Name: ix_dimse_listener_configs_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_configs_updated_at ON public.dimse_listener_configs USING btree (updated_at);


--
-- Name: ix_dimse_listener_state_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_state_id ON public.dimse_listener_state USING btree (id);


--
-- Name: ix_dimse_listener_state_listener_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dimse_listener_state_listener_id ON public.dimse_listener_state USING btree (listener_id);


--
-- Name: ix_dimse_listener_state_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_listener_state_updated_at ON public.dimse_listener_state USING btree (updated_at);


--
-- Name: ix_dimse_qr_sources_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_created_at ON public.dimse_qr_sources USING btree (created_at);


--
-- Name: ix_dimse_qr_sources_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_id ON public.dimse_qr_sources USING btree (id);


--
-- Name: ix_dimse_qr_sources_is_active; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_is_active ON public.dimse_qr_sources USING btree (is_active);


--
-- Name: ix_dimse_qr_sources_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_is_enabled ON public.dimse_qr_sources USING btree (is_enabled);


--
-- Name: ix_dimse_qr_sources_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_dimse_qr_sources_name ON public.dimse_qr_sources USING btree (name);


--
-- Name: ix_dimse_qr_sources_remote_ae_title; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_remote_ae_title ON public.dimse_qr_sources USING btree (remote_ae_title);


--
-- Name: ix_dimse_qr_sources_tls_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_tls_enabled ON public.dimse_qr_sources USING btree (tls_enabled);


--
-- Name: ix_dimse_qr_sources_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_dimse_qr_sources_updated_at ON public.dimse_qr_sources USING btree (updated_at);


--
-- Name: ix_google_healthcare_sources_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_google_healthcare_sources_created_at ON public.google_healthcare_sources USING btree (created_at);


--
-- Name: ix_google_healthcare_sources_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_google_healthcare_sources_id ON public.google_healthcare_sources USING btree (id);


--
-- Name: ix_google_healthcare_sources_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_google_healthcare_sources_name ON public.google_healthcare_sources USING btree (name);


--
-- Name: ix_google_healthcare_sources_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_google_healthcare_sources_updated_at ON public.google_healthcare_sources USING btree (updated_at);


--
-- Name: ix_imaging_orders_accession_number; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_imaging_orders_accession_number ON public.imaging_orders USING btree (accession_number);


--
-- Name: ix_imaging_orders_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_created_at ON public.imaging_orders USING btree (created_at);


--
-- Name: ix_imaging_orders_filler_order_number; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_filler_order_number ON public.imaging_orders USING btree (filler_order_number);


--
-- Name: ix_imaging_orders_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_id ON public.imaging_orders USING btree (id);


--
-- Name: ix_imaging_orders_modality; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_modality ON public.imaging_orders USING btree (modality);


--
-- Name: ix_imaging_orders_order_status; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_order_status ON public.imaging_orders USING btree (order_status);


--
-- Name: ix_imaging_orders_patient_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_patient_id ON public.imaging_orders USING btree (patient_id);


--
-- Name: ix_imaging_orders_patient_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_patient_name ON public.imaging_orders USING btree (patient_name);


--
-- Name: ix_imaging_orders_placer_order_number; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_placer_order_number ON public.imaging_orders USING btree (placer_order_number);


--
-- Name: ix_imaging_orders_scheduled_procedure_step_start_datetime; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_scheduled_procedure_step_start_datetime ON public.imaging_orders USING btree (scheduled_procedure_step_start_datetime);


--
-- Name: ix_imaging_orders_study_instance_uid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_imaging_orders_study_instance_uid ON public.imaging_orders USING btree (study_instance_uid);


--
-- Name: ix_imaging_orders_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_imaging_orders_updated_at ON public.imaging_orders USING btree (updated_at);


--
-- Name: ix_processed_study_log_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_created_at ON public.processed_study_log USING btree (created_at);


--
-- Name: ix_processed_study_log_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_id ON public.processed_study_log USING btree (id);


--
-- Name: ix_processed_study_log_queued_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_queued_at ON public.processed_study_log USING btree (retrieval_queued_at);


--
-- Name: ix_processed_study_log_source_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_source_id ON public.processed_study_log USING btree (source_id);


--
-- Name: ix_processed_study_log_source_id_str; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_source_id_str ON public.processed_study_log USING btree (source_id);


--
-- Name: ix_processed_study_log_source_type; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_source_type ON public.processed_study_log USING btree (source_type);


--
-- Name: ix_processed_study_log_study_instance_uid; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_study_instance_uid ON public.processed_study_log USING btree (study_instance_uid);


--
-- Name: ix_processed_study_log_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_processed_study_log_updated_at ON public.processed_study_log USING btree (updated_at);


--
-- Name: ix_roles_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_roles_created_at ON public.roles USING btree (created_at);


--
-- Name: ix_roles_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_roles_id ON public.roles USING btree (id);


--
-- Name: ix_roles_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_roles_name ON public.roles USING btree (name);


--
-- Name: ix_roles_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_roles_updated_at ON public.roles USING btree (updated_at);


--
-- Name: ix_rule_sets_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rule_sets_created_at ON public.rule_sets USING btree (created_at);


--
-- Name: ix_rule_sets_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rule_sets_id ON public.rule_sets USING btree (id);


--
-- Name: ix_rule_sets_is_active; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rule_sets_is_active ON public.rule_sets USING btree (is_active);


--
-- Name: ix_rule_sets_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_rule_sets_name ON public.rule_sets USING btree (name);


--
-- Name: ix_rule_sets_priority; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rule_sets_priority ON public.rule_sets USING btree (priority);


--
-- Name: ix_rule_sets_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rule_sets_updated_at ON public.rule_sets USING btree (updated_at);


--
-- Name: ix_rules_applicable_sources; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_applicable_sources ON public.rules USING btree (applicable_sources);


--
-- Name: ix_rules_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_created_at ON public.rules USING btree (created_at);


--
-- Name: ix_rules_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_id ON public.rules USING btree (id);


--
-- Name: ix_rules_is_active; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_is_active ON public.rules USING btree (is_active);


--
-- Name: ix_rules_priority; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_priority ON public.rules USING btree (priority);


--
-- Name: ix_rules_ruleset_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_ruleset_id ON public.rules USING btree (ruleset_id);


--
-- Name: ix_rules_schedule_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_schedule_id ON public.rules USING btree (schedule_id);


--
-- Name: ix_rules_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_rules_updated_at ON public.rules USING btree (updated_at);


--
-- Name: ix_schedules_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_schedules_created_at ON public.schedules USING btree (created_at);


--
-- Name: ix_schedules_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_schedules_id ON public.schedules USING btree (id);


--
-- Name: ix_schedules_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_schedules_is_enabled ON public.schedules USING btree (is_enabled);


--
-- Name: ix_schedules_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_schedules_name ON public.schedules USING btree (name);


--
-- Name: ix_schedules_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_schedules_updated_at ON public.schedules USING btree (updated_at);


--
-- Name: ix_storage_backend_configs_backend_type; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_backend_type ON public.storage_backend_configs USING btree (backend_type);


--
-- Name: ix_storage_backend_configs_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_created_at ON public.storage_backend_configs USING btree (created_at);


--
-- Name: ix_storage_backend_configs_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_id ON public.storage_backend_configs USING btree (id);


--
-- Name: ix_storage_backend_configs_is_enabled; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_is_enabled ON public.storage_backend_configs USING btree (is_enabled);


--
-- Name: ix_storage_backend_configs_name; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_storage_backend_configs_name ON public.storage_backend_configs USING btree (name);


--
-- Name: ix_storage_backend_configs_remote_ae_title; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_remote_ae_title ON public.storage_backend_configs USING btree (remote_ae_title);


--
-- Name: ix_storage_backend_configs_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_storage_backend_configs_updated_at ON public.storage_backend_configs USING btree (updated_at);


--
-- Name: ix_users_created_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_users_created_at ON public.users USING btree (created_at);


--
-- Name: ix_users_email; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_users_email ON public.users USING btree (email);


--
-- Name: ix_users_google_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE UNIQUE INDEX ix_users_google_id ON public.users USING btree (google_id);


--
-- Name: ix_users_id; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_users_id ON public.users USING btree (id);


--
-- Name: ix_users_updated_at; Type: INDEX; Schema: public; Owner: dicom_processor_user
--

CREATE INDEX ix_users_updated_at ON public.users USING btree (updated_at);


--
-- Name: api_keys fk_api_keys_user_id_users; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT fk_api_keys_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: rule_destination_association fk_rule_destination_association_rule_id_rules; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rule_destination_association
    ADD CONSTRAINT fk_rule_destination_association_rule_id_rules FOREIGN KEY (rule_id) REFERENCES public.rules(id) ON DELETE CASCADE;


--
-- Name: rule_destination_association fk_rule_destination_association_storage_backend_config__1e50; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rule_destination_association
    ADD CONSTRAINT fk_rule_destination_association_storage_backend_config__1e50 FOREIGN KEY (storage_backend_config_id) REFERENCES public.storage_backend_configs(id) ON DELETE CASCADE;


--
-- Name: rules fk_rules_ruleset_id_rule_sets; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rules
    ADD CONSTRAINT fk_rules_ruleset_id_rule_sets FOREIGN KEY (ruleset_id) REFERENCES public.rule_sets(id) ON DELETE CASCADE;


--
-- Name: rules fk_rules_schedule_id_schedules; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.rules
    ADD CONSTRAINT fk_rules_schedule_id_schedules FOREIGN KEY (schedule_id) REFERENCES public.schedules(id) ON DELETE SET NULL;


--
-- Name: user_role_association fk_user_role_association_role_id_roles; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.user_role_association
    ADD CONSTRAINT fk_user_role_association_role_id_roles FOREIGN KEY (role_id) REFERENCES public.roles(id) ON DELETE CASCADE;


--
-- Name: user_role_association fk_user_role_association_user_id_users; Type: FK CONSTRAINT; Schema: public; Owner: dicom_processor_user
--

ALTER TABLE ONLY public.user_role_association
    ADD CONSTRAINT fk_user_role_association_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

