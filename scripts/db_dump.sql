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
-- Data for Name: a_i_prompt_configs; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.a_i_prompt_configs (name, description, dicom_tag_keyword, is_enabled, prompt_template, model_identifier, model_parameters, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.users (email, google_id, full_name, picture, hashed_password, is_active, is_superuser, id, created_at, updated_at) FROM stdin;
chris.platt@gmail.com	107319237646324040348	Chris Platt	https://lh3.googleusercontent.com/a/ACg8ocL9XEeD7OiJlpuDA0u4KeqwjjX9J2BDeaOrDpYy7Ft_DuYzNVuACg=s96-c	$2b$12$hQG9Wxmn0XayygBbyUVoYOexhqoib57ofpBNkbh11o64fKIZ30Hui	t	t	1	2025-06-19 04:10:06.881646+00	2025-06-19 04:10:06.881646+00
\.


--
-- Data for Name: api_keys; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.api_keys (hashed_key, prefix, name, user_id, expires_at, last_used_at, is_active, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: crosswalk_data_sources; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.crosswalk_data_sources (name, description, db_type, connection_details, target_table, sync_interval_seconds, is_enabled, last_sync_status, last_sync_time, last_sync_error, last_sync_row_count, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: crosswalk_maps; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.crosswalk_maps (name, description, is_enabled, data_source_id, match_columns, cache_key_columns, replacement_mapping, cache_ttl_seconds, on_cache_miss, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: dicom_exception_log; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.dicom_exception_log (exception_uuid, study_instance_uid, series_instance_uid, sop_instance_uid, patient_name, patient_id, accession_number, modality, failure_timestamp, processing_stage, error_message, error_details, failed_filepath, original_source_type, original_source_identifier, calling_ae_title, target_destination_id, target_destination_name, status, retry_count, next_retry_attempt_at, last_retry_attempt_at, resolved_at, resolved_by_user_id, resolution_notes, celery_task_id, id, created_at, updated_at) FROM stdin;
a6560ffb-36df-47d7-a929-3202ac355978	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.82205152065420330072497738886161270095	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:21.972675+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.134614+00	2025-07-23 13:07:26.253347+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	d5bb72ca-9792-4807-9f6d-fef457acf9c6	3	2025-07-23 13:06:21.972675+00	2025-07-23 13:13:01.849074+00
694dc15b-7b30-460c-8d0c-39bd91ce42b3	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.26142509367458244700840633780206625328	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.031716+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.180853+00	2025-07-23 13:07:26.37776+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	fc3a9088-ce4c-4c68-8ef8-53df271871e4	4	2025-07-23 13:06:22.031716+00	2025-07-23 13:13:01.849074+00
05d7ddc5-bc1b-4098-940b-315d96fc212b	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.7768483101813706899008197934757050590	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.186035+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.341671+00	2025-07-23 13:07:26.765957+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	fc84303a-8ed0-4fea-a01b-79cd1ba04563	7	2025-07-23 13:06:22.186035+00	2025-07-23 13:13:01.849074+00
cb9ee230-d230-4a92-9b5e-df61eb440ca9	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.51263981760407352302418776730308687437	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.367426+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.523191+00	2025-07-23 13:12:25.979005+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	d4e95a93-d66a-4204-925e-9966ede224c9	11	2025-07-23 13:06:22.367426+00	2025-07-23 13:13:01.849074+00
4963b199-cd40-498e-9418-a8d99846a99b	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.14278134618993358338002161128023998213	1.2.826.0.1.3680043.8.498.65554951795110008353775861968502725742	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	SR	2025-07-23 13:06:22.225001+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.372997+00	2025-07-23 13:07:26.890132+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	a1d03e8e-7144-42ce-9892-3433d98cc227	8	2025-07-23 13:06:22.225001+00	2025-07-23 13:13:01.849074+00
65edc304-f7e8-4de5-bd73-889db95c6be7	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.4036152389591902938942397923519585877	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.403042+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.55546+00	2025-07-23 13:12:26.250435+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	56461855-2f12-443d-9a5a-0988e21547d8	13	2025-07-23 13:06:22.403042+00	2025-07-23 13:13:01.849074+00
c912cbf0-ab2a-4ac8-87c3-29613fe0a012	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.23818252980457150080638939925014542597	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.664463+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.813051+00	2025-07-23 13:12:26.857927+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	ace2ee4e-062b-4941-a388-9bae33b27f09	18	2025-07-23 13:06:22.664463+00	2025-07-23 13:13:01.849074+00
66877f6c-77c3-4ffc-8398-f4f86025ed08	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.16272153524409836406136128366430242679	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.248375+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.402831+00	2025-07-23 13:07:26.995302+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	c8bc8ec2-dbe6-46bd-8433-fd6aef349aa3	9	2025-07-23 13:06:22.248375+00	2025-07-23 13:13:01.849074+00
c133dbfc-301e-4513-9e7b-3d1532b66711	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.70633024803596742382822664737307048777	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.415698+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.548637+00	2025-07-23 13:12:26.110814+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	1b9ea3bd-934a-4af0-8da5-1c471a0c67ad	12	2025-07-23 13:06:22.415698+00	2025-07-23 13:13:01.849074+00
e12841bf-7159-476b-b4b4-0c6ff60db369	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.22766119264445814475739341218354636789	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.055518+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.208179+00	2025-07-23 13:07:26.516594+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	8e40c825-988e-462d-ae9b-cf7829fdcf17	5	2025-07-23 13:06:22.055518+00	2025-07-23 13:13:01.849074+00
cc36b6ed-3d91-46ae-af7c-87c6baf61b7c	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.75963641313441167447807841285195332013	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.0983+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.251273+00	2025-07-23 13:07:26.64165+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	1b3776f1-3bfb-4a86-9351-4f7a637b6c24	6	2025-07-23 13:06:22.0983+00	2025-07-23 13:13:01.849074+00
de7dc296-126a-4f21-b78b-cf3f3473e463	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.26853124666322315083183966786311345722	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.545748+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.698546+00	2025-07-23 13:12:26.48965+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	2d6375d9-03d3-413d-b4bd-a218a7903700	15	2025-07-23 13:06:22.545748+00	2025-07-23 13:13:01.849074+00
2ed6457c-8b44-4f99-84e1-9cec5ceb13ae	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.85414236105587761765874313087746837146	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.719778+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.867589+00	2025-07-23 13:12:26.980274+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	59547f6c-219f-4699-bca2-f77442ef0784	19	2025-07-23 13:06:22.719778+00	2025-07-23 13:13:01.849074+00
35329de3-70b5-437e-8f6c-f881adc06ec9	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.78462141863047220357570378607732300664	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.585086+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.740545+00	2025-07-23 13:12:26.614086+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	352610ed-a564-4db0-995e-dfdbdd8a203a	16	2025-07-23 13:06:22.585086+00	2025-07-23 13:13:01.849074+00
53f59893-60b0-4373-8636-7052827c3713	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.12048286240133955937715799759348824930	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.761771+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.912267+00	2025-07-23 13:12:27.095281+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	4bb114f5-0d4d-4fa0-9b3c-a5936f5e9e08	20	2025-07-23 13:06:22.761771+00	2025-07-23 13:13:01.849074+00
d0baa91b-6618-452d-880b-766c593b1b78	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.88866488217717600316541777928664698126	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:21.854+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.021556+00	2025-07-23 13:07:25.978598+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	9a82f3ee-fa31-4e84-a26a-9e0a14ba0faf	1	2025-07-23 13:06:21.854+00	2025-07-23 13:13:01.849074+00
8a6e8463-8e4d-4d6e-8c86-4a543ff14c98	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.26916863669043127200743507203511821333	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:21.913363+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.063762+00	2025-07-23 13:07:26.125242+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	bfd57d7f-ba3e-4e08-b0a2-d180fbc371f4	2	2025-07-23 13:06:21.913363+00	2025-07-23 13:13:01.849074+00
96388d70-6815-49bb-8c61-6218178dc500	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.12343760425535036285243376717800643510	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.277242+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.432247+00	2025-07-23 13:07:27.117134+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	c275e03f-9b09-4772-a0aa-502e96af8484	10	2025-07-23 13:06:22.277242+00	2025-07-23 13:13:01.849074+00
e6c81871-788b-4671-838d-65f64a3462af	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.39018624379848472636312202074130255314	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.468353+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.600037+00	2025-07-23 13:12:26.377354+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	80c1d6b9-808d-4863-b7a4-23febad82a8d	14	2025-07-23 13:06:22.468353+00	2025-07-23 13:13:01.849074+00
b3b016bf-01ce-49cc-8515-a2f3ea54b33e	1.2.826.0.1.3680043.8.498.35453480522071137792723473648108437218	1.2.826.0.1.3680043.8.498.6968625727709785056479287346234008088	1.2.826.0.1.3680043.8.498.30888099517099670196602071301497782980	Michael^Johnston	3428202956	TEST_2VGV3AEN4M50	MR	2025-07-23 13:06:22.641746+00	DESTINATION_SEND	[RESOLVED VIA RETRY] Failed to establish association with ORTHANC_TEST	Traceback (most recent call last):\n  File "/app/app/worker/executors/file_based_executor.py", line 175, in execute_file_based_task\n    store_result = storage_backend.store(\n                   ^^^^^^^^^^^^^^^^^^^^^^\n  File "/app/app/services/storage_backends/dicom_cstore.py", line 278, in store\n    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")\napp.services.storage_backends.base_backend.StorageBackendError: Failed to establish association with ORTHANC_TEST\n	\N	DIMSE_LISTENER	DIMSE C-STORE SCP TLS	POPULATE	2	Orthanc DIMSE TLS	ARCHIVED	1	2025-07-23 13:07:22.794584+00	2025-07-23 13:12:26.743286+00	2025-07-23 13:13:01.86833+00	\N	Bulk archived (study level)	e165fcef-5e4e-4da7-bd96-112894252048	17	2025-07-23 13:06:22.641746+00	2025-07-23 13:13:01.849074+00
\.


--
-- Data for Name: dicomweb_source_state; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.dicomweb_source_state (source_name, description, base_url, qido_prefix, wado_prefix, polling_interval_seconds, is_enabled, is_active, auth_type, auth_config, search_filters, last_processed_timestamp, last_successful_run, last_error_run, last_error_message, found_instance_count, queued_instance_count, processed_instance_count, id, created_at, updated_at) FROM stdin;
Orthanc	Orthanc	http://orthanc:8042/	dicom-web	dicom-web	300	t	f	basic	{"password": "orthancpassword", "username": "orthancuser"}	{"StudyDate": "TODAY"}	\N	\N	2025-06-19 04:22:33.646499+00	QIDO request failed: 404 Client Error: Not Found for url: http://orthanc:8042/dicom-web/qido-rs/studies?StudyDate=20250619&limit=5000. Response: {\n\t"HttpError" : "Not Found",\n\t"HttpStatus" : 404,\n\t"Message" : "Unknown resource",\n\t"Method" : "GET",\n\t"OrthancError" : "Unknown resource",\n\t"OrthancStatus" : 17,\n\t"Uri" : "/dicom-web/qido-rs/studies"\n}\n (URL: http://orthanc:8042/dicom-web/qido-rs/studies?StudyDate=20250619&limit=5000, Status: 404)	0	0	0	1	2025-06-19 04:21:49.274716+00	2025-06-19 04:26:06.058737+00
\.


--
-- Data for Name: dimse_listener_configs; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.dimse_listener_configs (name, description, ae_title, port, is_enabled, instance_id, tls_enabled, tls_cert_secret_name, tls_key_secret_name, tls_ca_cert_secret_name, id, created_at, updated_at) FROM stdin;
DIMSE C-STORE SCP	DIMSE C-STORE SCP, non TLS.	AXIOM_SCP	11112	t	storescp_1	f	\N	\N	\N	1	2025-06-19 04:13:03.744896+00	2025-06-19 04:13:03.744896+00
DIMSE C-STORE SCP TLS	TLS C-STORE SCP	AXIOM_SCP_TLS	11113	t	storescp_2	t	axiom-sscp-crt	axiom-sscp-key	\N	2	2025-06-19 04:17:14.425582+00	2025-06-19 04:19:23.467228+00
\.


--
-- Data for Name: dimse_listener_state; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.dimse_listener_state (id, listener_id, status, status_message, host, port, ae_title, last_heartbeat, created_at, received_instance_count, processed_instance_count, updated_at) FROM stdin;
1	storescp_1	running	Listener active.	0.0.0.0	11112	AXIOM_SCP	2025-07-23 17:16:58.868182+00	2025-06-19 04:13:12.364482+00	0	0	2025-07-23 17:16:58.868182+00
2	storescp_2	running	Listener active. (TLS enabled)	0.0.0.0	11113	AXIOM_SCP_TLS	2025-07-23 17:16:59.02763+00	2025-06-19 04:19:42.025617+00	60	40	2025-07-23 17:16:59.02763+00
\.


--
-- Data for Name: dimse_qr_sources; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.dimse_qr_sources (name, description, remote_ae_title, remote_host, remote_port, local_ae_title, tls_enabled, tls_ca_cert_secret_name, tls_client_cert_secret_name, tls_client_key_secret_name, polling_interval_seconds, is_enabled, is_active, query_level, query_filters, move_destination_ae_title, last_successful_query, last_successful_move, last_error_time, last_error_message, found_study_count, move_queued_study_count, processed_instance_count, id, created_at, updated_at) FROM stdin;
Orthanc TLS Test Source	\N	ORTHANC_TEST	orthanc	4242	AXIOM_QR_SCU	t	axiom-test-ca-cert	\N	\N	300	t	f	STUDY	{"StudyDate": "TODAY"}	AXIOM_SSCP_TLS	\N	\N	\N	\N	0	0	0	1	2025-06-22 19:13:00.736379+00	2025-06-22 19:14:29.182659+00
\.


--
-- Data for Name: google_healthcare_sources; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.google_healthcare_sources (name, description, gcp_project_id, gcp_location, gcp_dataset_id, gcp_dicom_store_id, polling_interval_seconds, query_filters, is_enabled, is_active, id, created_at, updated_at) FROM stdin;
Healthcare API DICOM Store	Healthcare API [axiom-dicom-store-0001]	axiom-flow	us-central1	axiom-dataset-test-0001	axiom-dicom-store-0001	300	{"StudyDate": "TODAY"}	t	f	1	2025-06-22 19:16:40.230208+00	2025-06-22 19:16:40.230208+00
\.


--
-- Data for Name: imaging_orders; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.imaging_orders (patient_name, patient_id, patient_dob, patient_sex, accession_number, placer_order_number, filler_order_number, requested_procedure_description, requested_procedure_code, modality, scheduled_station_ae_title, scheduled_station_name, scheduled_procedure_step_start_datetime, requesting_physician, referring_physician, order_status, study_instance_uid, source, order_received_at, raw_hl7_message, creator_id, id, created_at, updated_at) FROM stdin;
DOE, JANE	PATID12345	1980-01-01	F	CT_ACCESSION_001	PLACER123	CT_ACCESSION_001	CT CHEST W CONTRAST	\N	CT	\N	\N	2025-06-20 14:00:00+00			SCHEDULED	\N	hl7_mllp:SENDING_FACILITY	2025-06-23 19:17:36.920148+00	MSH|^~\\&|SENDING_APP|SENDING_FACILITY|AXIOM_DMWL|AXIOM_FACILITY|20250620120000||ORM^O01|MSG_CONTROL_12345|P|2.5.1\rPID|||PATID12345^^^MRN||DOE^JANE||19800101|F\rORC|NW|PLACER123|CT_ACCESSION_001\rOBR|||CT_ACCESSION_001|PROC001^CT CHEST W CONTRAST^CUSTOM|||20250620140000|||||||||||||||||CT\r	\N	9	2025-06-23 19:17:36.920148+00	2025-06-24 18:59:39.889756+00
NEWMAN, JERRY	MRN_FRESH_BLOOD	1954-04-29	M	ACCN_FRESH_001	PLACER_FRESH_BLOOD_001	ACCN_FRESH_001	MRI BRAIN W/O CONTRAST	\N		\N	\N	\N			CANCELED	\N	hl7_mllp:HOSPITAL	2025-06-24 19:03:24.133549+00	MSH|^~\\&|RIS|HOSPITAL|AXIOM_DMWL|AXIOM_FACILITY|20250624193400||ORM^O01|MSG_FRESH_3|P|2.5.1\rPID|1||MRN_FRESH_BLOOD|ALT_ID_123|NEWMAN^JERRY|SEINFELD|19540429|M\rORC|CA|PLACER_FRESH_BLOOD_001|ACCN_FRESH_001\rOBR|1||ACCN_FRESH_001|MRBRAIN^MRI BRAIN W/O CONTRAST||||||||||DR. VAN NOSTRAND||||||||MR|||||CANCELED\r	\N	10	2025-06-24 19:03:24.133549+00	2025-06-24 19:06:18.106157+00
WANKER, JOHN	MRN99999	2000-01-01	M	ACCN_67890	PLACER_12345	ACCN_67890	CT CHEST W/O CONTRAST	\N		\N	\N	2025-06-26 10:30:00+00			SCHEDULED	\N	hl7_mllp:HOSPITAL	2025-06-24 19:16:06.209951+00	MSH|^~\\&|RIS|HOSPITAL|AXIOM_DMWL|AXIOM_FACILITY|20250624170000||ORM^O01|MSG00001|P|2.5.1\rPID|1||MRN98765^^^MRN|ALT_ID_123|WANKER^JOHN^A|DOE|20000101|M\rORC|NW|PLACER_12345|ACCN_67890|||||||20250624170000\rOBR|1||ACCN_67890|CTCHEST^CT CHEST W/O CONTRAST|||20250626103000|||||||DR. STRANGELOVE||||||||CT|||||SCHEDULED\r	\N	11	2025-06-24 19:16:06.209951+00	2025-06-24 19:58:23.433144+00
CRONIN, CANDACE	10666160	1975-06-04	M	ACC12345_9ER	EMR-ORDER-oSmca	ACC12345_9ER	CT Chest W/O Contrast	\N	US	\N	\N	2025-06-30 22:56:19+00		698963, KIEHN	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-01 03:05:06.834566+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250630225619||ORM^O01|MSGID44140|P|2.5.1|||||||||||\rPID|1||10666160^^^MRN^MR||CRONIN^CANDACE^M||19750604|M|||61798 Johnston Hills^^Ulicesburgh^VT^62811-9935||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||698963^KIEHN^MAC||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-oSmca|||||||20250630225619|||698963^KIEHN^MAC|||||||||||||||||||\rOBR|1|EMR-ORDER-vuc3k|ACC12345_9ER|CTCHEST^CT Chest W/O Contrast^C4|||20250630225619|||||||||||||||||US||||||||||||||||||||||||||\r	\N	12	2025-07-01 03:05:06.834566+00	2025-07-01 03:05:06.834566+00
LAKIN, ALTHEA	76199518	1971-02-19	M	RIS-ACC-68509039	EMR-ORDER-RlEBJ	RIS-ACC-68509039	MRI Brain W/O Contrast	\N	NM	\N	\N	2025-07-01 00:35:48+00	295757, EFFERTZ	295757, EFFERTZ	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-01 04:36:10.66878+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250701003548||ORM^O01|MSGID22671|P|2.5.1||||||||||\rPID|1||76199518^^^MRN^MR||LAKIN^ALTHEA^B||19710219|M|||711 Bo Ferry^^Sporerview^AK^71525-0764||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||295757^EFFERTZ^JAYDA||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-RlEBJ|RIS-ACC-68509039|O|||||20250701003548|||295757^EFFERTZ^JAYDA|||||||||||||||||||\rOBR|1|EMR-ORDER-RlEBJ|RIS-ACC-68509039|MRBRAIN^MRI Brain W/O Contrast^C4|||20250701003548|||||||||295757^EFFERTZ^JAYDA||||||||NM||||||||||||||||||||||||||\r	\N	13	2025-07-01 04:36:10.66878+00	2025-07-01 04:36:10.66878+00
GOLDNER, AURELIA	16830932	1970-05-05	F	RIS-ACC-61300322	EMR-ORDER-5MM7E	RIS-ACC-61300322	Ultrasound Abdomen Complete	\N	XA	\N	\N	2025-07-01 11:17:26+00	742788, OKUNEVA	742788, OKUNEVA	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-01 15:17:58.145406+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250701111726||ORM^O01|MSGID35487|P|2.5.1||||||||||\rPID|1||16830932^^^MRN^MR||GOLDNER^AURELIA^J||19700505|F|||10647 N East Street^^Barrettville^NM^50766||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||742788^OKUNEVA^JEROD||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-5MM7E|RIS-ACC-61300322|O|||||20250701111726|||742788^OKUNEVA^JEROD|||||||||||||||||||\rOBR|1|EMR-ORDER-5MM7E|RIS-ACC-61300322|USABD^Ultrasound Abdomen Complete^C4|||20250701111726|||||||||742788^OKUNEVA^JEROD||||||||XA||||||||||||||||||||||||||\r	\N	14	2025-07-01 15:17:58.145406+00	2025-07-01 15:17:58.145406+00
HAAG, WILTON	64250161	1945-07-07	F	RIS-ACC-50423204	EMR-ORDER-4cbak	RIS-ACC-50423204		\N	CR	\N	\N	2025-07-07 11:02:31+00	785090, KUPHAL	785090, KUPHAL	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:06:22.330038+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707110231||ORM^O01|12345|P|2.5.1||||||||||\rPID|1||64250161^^^MRN^MR||HAAG^WILTON^N||19450707|F|||90201 Willow Grove^^Murrayberg^MN^74968||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||785090^KUPHAL^MARION||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-4cbak|RIS-ACC-50423204|O|||||20250707110231|||785090^KUPHAL^MARION|||||||||||||||||||\rOBR|1|EMR-ORDER-4cbak|RIS-ACC-50423204|1234567890^^^C4|||20250707110231|||||||||785090^KUPHAL^MARION||||||||CR||||||||||||||||||||||||||\r	\N	15	2025-07-07 15:06:22.330038+00	2025-07-07 15:07:47.376575+00
ANKUNDING, SANDRA	53290151	1948-04-07	F	RIS-ACC-81736740	EMR-ORDER-DSSCa	RIS-ACC-81736740	MRI Brain W/O Contrast	\N	XA	\N	\N	2025-07-07 11:08:11+00	148081, EBERT	148081, EBERT	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:08:17.278473+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707110811||ORM^O01|MSGID14674|P|2.5.1||||||||||\rPID|1||53290151^^^MRN^MR||ANKUNDING^SANDRA^R||19480407|F|||97629 Mozell Meadow^^Sarasota^LA^59079-4416||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||148081^EBERT^HILLARY||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-DSSCa|RIS-ACC-81736740|O|||||20250707110811|||148081^EBERT^HILLARY|||||||||||||||||||\rOBR|1|EMR-ORDER-DSSCa|RIS-ACC-81736740|MRBRAIN^MRI Brain W/O Contrast^C4|||20250707110811|||||||||148081^EBERT^HILLARY||||||||XA||||||||||||||||||||||||||\r	\N	16	2025-07-07 15:08:17.278473+00	2025-07-07 15:08:17.278473+00
RODRIGUEZ, HELENE	53017907	1970-03-31	M	RIS-ACC-86107214	EMR-ORDER-pzvk6	RIS-ACC-86107214	X-Ray Elbow Left	\N	MRI	\N	\N	2025-07-07 11:14:25+00	809186, NIKOLAUS	809186, NIKOLAUS	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:14:49.273865+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707111425||ORM^O01|MSGID11663|P|2.5.1||||||||||\rPID|1||53017907^^^MRN^MR||RODRIGUEZ^HELENE^C||19700331|M|||681 Wilkinson Pass^^Providence^MO^90227||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||809186^NIKOLAUS^MARISOL||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-pzvk6|RIS-ACC-86107214|O|||||20250707111425|||809186^NIKOLAUS^MARISOL|||||||||||||||||||\rOBR|1|EMR-ORDER-pzvk6|RIS-ACC-86107214|XRELBOW^X-Ray Elbow Left^C4|||20250707111425|||||||||809186^NIKOLAUS^MARISOL||||||||MRI||||||||||||||||||||||||||\r	\N	17	2025-07-07 15:14:49.273865+00	2025-07-07 15:14:49.273865+00
BASHIRIAN, LOIS	26032391	1967-08-31	M	RIS-ACC-47303285	EMR-ORDER-NA542	RIS-ACC-47303285	X-Ray Elbow Left	\N	MRI	\N	\N	2025-07-07 11:15:51+00	098282, OLSON	098282, OLSON	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:16:09.284271+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707111551||ORM^O01|MSGID28253|P|2.5.1||||||||||\rPID|1||26032391^^^MRN^MR||BASHIRIAN^LOIS^J||19670831|M|||419 Cow Lane^^Colorado Springs^MA^15383||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||098282^OLSON^THEO||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-NA542|RIS-ACC-47303285|O|||||20250707111551|||098282^OLSON^THEO|||||||||||||||||||\rOBR|1|EMR-ORDER-NA542|RIS-ACC-47303285|XRELBOW^X-Ray Elbow Left^C4|||20250707111551|||||||||098282^OLSON^THEO||||||||MRI||||||||||||||||||||||||||\r	\N	18	2025-07-07 15:16:09.284271+00	2025-07-07 15:16:09.284271+00
SMITH, AMERICO	10980634	1965-04-22	F	RIS-ACC-30554456	EMR-ORDER-172wf	RIS-ACC-30554456	CT Chest W/O Contrast	\N	CR	\N	\N	2025-07-07 11:24:25+00	622055, CRIST	622055, CRIST	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:24:49.084818+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707112425||ORM^O01|MSGID74856|P|2.5.1||||||||||\rPID|1||10980634^^^MRN^MR||SMITH^AMERICO^H||19650422|F|||40359 Airport Road^^Troy^WA^62222||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||622055^CRIST^MELYSSA||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-172wf|RIS-ACC-30554456|O|||||20250707112425|||622055^CRIST^MELYSSA|||||||||||||||||||\rOBR|1|EMR-ORDER-172wf|RIS-ACC-30554456|CTCHEST^CT Chest W/O Contrast^C4|||20250707112425|||||||||622055^CRIST^MELYSSA||||||||CR||||||||||||||||||||||||||\r	\N	19	2025-07-07 15:24:49.084818+00	2025-07-07 15:24:49.084818+00
HAGENES, EDMOND	23531387	1967-12-12	F	RIS-ACC-55588052	EMR-ORDER-kHEkF	RIS-ACC-55588052	X-Ray Elbow Left	\N	MRI	\N	\N	2025-07-07 11:25:14+00	360986, SMITHAM	360986, SMITHAM	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:25:37.801586+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707112514||ORM^O01|MSGID51945|P|2.5.1||||||||||\rPID|1||23531387^^^MRN^MR||HAGENES^EDMOND^J||19671212|F|||93769 Marks Alley^^Fort Barney^OH^51312||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||360986^SMITHAM^BRANDI||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-kHEkF|RIS-ACC-55588052|O|||||20250707112514|||360986^SMITHAM^BRANDI|||||||||||||||||||\rOBR|1|EMR-ORDER-kHEkF|RIS-ACC-55588052|XRELBOW^X-Ray Elbow Left^C4|||20250707112514|||||||||360986^SMITHAM^BRANDI||||||||MRI||||||||||||||||||||||||||\r	\N	20	2025-07-07 15:25:37.801586+00	2025-07-07 15:25:37.801586+00
JACOBI, RAE	48667650	1962-06-01	F	RIS-ACC-05370121	EMR-ORDER-bOChQ	RIS-ACC-05370121	X-Ray Elbow Left	\N	US	\N	\N	2025-07-07 11:30:44+00	420271, MUELLER	420271, MUELLER	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:31:03.562074+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707113044||ORM^O01|MSGID94755|P|2.5.1||||||||||\rPID|1||48667650^^^MRN^MR||JACOBI^RAE^D||19620601|F|||929 Jenkins Ports^^Las Cruces^NE^42645||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||420271^MUELLER^COLTON||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-bOChQ|RIS-ACC-05370121|O|||||20250707113044|||420271^MUELLER^COLTON|||||||||||||||||||\rOBR|1|EMR-ORDER-bOChQ|RIS-ACC-05370121|XRELBOW^X-Ray Elbow Left^C4|||20250707113044|||||||||420271^MUELLER^COLTON||||||||US||||||||||||||||||||||||||\r	\N	21	2025-07-07 15:31:03.562074+00	2025-07-07 15:31:03.562074+00
REILLY, ROSEMARY	14202716	2000-03-16	M	RIS-ACC-53869703	EMR-ORDER-VhL09	RIS-ACC-53869703	CT Chest W/O Contrast	\N	MRI	\N	\N	2025-07-07 11:31:45+00	217461, GERLACH	217461, GERLACH	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:31:59.205731+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707113145||ORM^O01|MSGID84316|P|2.5.1||||||||||\rPID|1||14202716^^^MRN^MR||REILLY^ROSEMARY^R||20000316|M|||5315 Laurianne Harbor^^South Heber^ME^68219-6766||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||217461^GERLACH^CICERO||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-VhL09|RIS-ACC-53869703|O|||||20250707113145|||217461^GERLACH^CICERO|||||||||||||||||||\rOBR|1|EMR-ORDER-VhL09|RIS-ACC-53869703|CTCHEST^CT Chest W/O Contrast^C4|||20250707113145|||||||||217461^GERLACH^CICERO||||||||MRI||||||||||||||||||||||||||\r	\N	22	2025-07-07 15:31:59.205731+00	2025-07-07 15:31:59.205731+00
HUEL, FURMAN	09158545	1980-11-11	F	RIS-ACC-25802637	EMR-ORDER-euuNU	RIS-ACC-25802637	Ultrasound Abdomen Complete	\N	CT	\N	\N	2025-07-07 11:34:26+00	150561, BERGSTROM	150561, BERGSTROM	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 15:34:38.892222+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707113426||ORM^O01|MSGID28106|P|2.5.1||||||||||\rPID|1||09158545^^^MRN^MR||HUEL^FURMAN^B||19801111|F|||5760 Allan Trail^^Marietta^IL^95684-3537||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||150561^BERGSTROM^TRUDIE||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-euuNU|RIS-ACC-25802637|O|||||20250707113426|||150561^BERGSTROM^TRUDIE|||||||||||||||||||\rOBR|1|EMR-ORDER-euuNU|RIS-ACC-25802637|USABD^Ultrasound Abdomen Complete^C4|||20250707113426|||||||||150561^BERGSTROM^TRUDIE||||||||CT||||||||||||||||||||||||||\r	\N	23	2025-07-07 15:34:38.892222+00	2025-07-07 15:35:06.32859+00
MILLER, LONNIE	75360957	1989-12-10	F	RIS-ACC-58358393	EMR-ORDER-3NgBx	RIS-ACC-58358393	MRI Brain W/O Contrast	\N	MRI	\N	\N	2025-07-07 12:33:41+00	270193, ROMAGUERA	270193, ROMAGUERA	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 16:34:16.474073+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707123341||ORM^O01|MSGID98062|P|2.5.1||||||||||\rPID|1||75360957^^^MRN^MR||MILLER^LONNIE^C||19891210|F|||96884 Lydia Motorway^^South Dayneberg^SD^52808||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||270193^ROMAGUERA^DEE||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-3NgBx|RIS-ACC-58358393|O|||||20250707123341|||270193^ROMAGUERA^DEE|||||||||||||||||||\rOBR|1|EMR-ORDER-3NgBx|RIS-ACC-58358393|MRBRAIN^MRI Brain W/O Contrast^C4|||20250707123341|||||||||270193^ROMAGUERA^DEE||||||||MRI||||||||||||||||||||||||||\r	\N	24	2025-07-07 16:34:16.474073+00	2025-07-07 16:34:16.474073+00
ANDERSON, ADA	00533180	1947-09-10	M	RIS-ACC-84327088	EMR-ORDER-4V15X	RIS-ACC-84327088	Ultrasound Abdomen Complete	\N	XA	\N	\N	2025-07-07 13:02:20+00	661712, GOLDNER	661712, GOLDNER	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 17:02:26.362688+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707130220||ORM^O01|MSGID47745|P|2.5.1||||||||||\rPID|1||00533180^^^MRN^MR||ANDERSON^ADA^J||19470910|M|||250 Ford Valleys^^South Williamcester^LA^35532-2254||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||661712^GOLDNER^JOANNIE||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-4V15X|RIS-ACC-84327088|O|||||20250707130220|||661712^GOLDNER^JOANNIE|||||||||||||||||||\rOBR|1|EMR-ORDER-4V15X|RIS-ACC-84327088|USABD^Ultrasound Abdomen Complete^C4|||20250707130220|||||||||661712^GOLDNER^JOANNIE||||||||XA||||||||||||||||||||||||||\r	\N	25	2025-07-07 17:02:26.362688+00	2025-07-07 17:02:26.362688+00
TOY, AYANA	83775197	1985-03-05	M	RIS-ACC-84133217	EMR-ORDER-JOsqH	RIS-ACC-84133217	Ultrasound Abdomen Complete	\N	MRI	\N	\N	2025-07-07 13:06:16+00	897453, MOORE	897453, MOORE	SCHEDULED	\N	hl7_mllp:MainClinic	2025-07-07 17:06:24.32474+00	MSH|^~\\&|ClinicEMR|MainClinic|MegaRIS|MainClinic|20250707130616||ORM^O01|MSGID91869|P|2.5.1||||||||||\rPID|1||83775197^^^MRN^MR||TOY^AYANA^E||19850305|M|||781 Margaretta Mission^^Darefield^ND^58530-6458||||||||||||||||||||||||||||\rPV1|1|O|OP^Outpatient^^|||||897453^MOORE^BOYD||||||||||||||||||||||||||||||||||||||||||||\rORC|NW|EMR-ORDER-JOsqH|RIS-ACC-84133217|O|||||20250707130616|||897453^MOORE^BOYD|||||||||||||||||||\rOBR|1|EMR-ORDER-JOsqH|RIS-ACC-84133217|USABD^Ultrasound Abdomen Complete^C4|||20250707130616|||||||||897453^MOORE^BOYD||||||||MRI||||||||||||||||||||||||||\r	\N	26	2025-07-07 17:06:24.32474+00	2025-07-07 17:06:24.32474+00
RUTHERFORD, ERNA	54708245	1955-01-20	F	71604357	76684826^ORD_APP	71604357	Chest X-Ray (2 views)	\N	CR	\N	\N	2025-07-23 14:00:00+00	67890, CASEY	67890, CASEY	SCHEDULED	\N	hl7_mllp:ORD_FAC	2025-07-23 13:41:25.879755+00	MSH|^~\\&|ORD_APP|ORD_FAC|RAD_SYS|RAD_FAC|20250723134100||ORM^O01^ORM_O01|MSG6369120|P|2.5.1|||AL|AL|USA|UNICODE UTF-8|||IHE_RAD_PDI^IHE_RAD^1.3.6.1.4.1.19376.1.5.3.1.1.1^ISO\rPID|1||54708245^^^ORD_FAC^MR||RUTHERFORD^ERNA^J|SMITH^JANE|19550120|F||2106-3^White^CDCREC|123 Main St^^Anytown^CA^90210^USA^H||(555)555-1212|(555)555-8888|en|M^Married|CATH^Roman Catholic|ACCT98765|999-00-1111|F12345678||2186-5^Not Hispanic or Latino^CDCREC|Anytown, CA|N||||N||N|N||||20250723133000|ORD_FAC|||\rPV1|1|O|RAD^R101^1^RADIOLOGY|R|||12345^WELBY^MARCUS^A^DR^MD|67890^CASEY^BEN^J^DR^MD||RAD|||R|1||A0|12345^WELBY^MARCUS^A^DR^MD||V9876543||||||||||||||||||01|||||20250723134000|||||AV12345|V||||\rORC|NW|76684826^ORD_APP|71604357^RAD_SYS|GRP123^ORD_APP|SC||1^^^^^S||20250723134100|U1234^USER^TEST||67890^CASEY^BEN^J^DR^MD|UROLOGY_CLINIC^C1|(555)555-9999|20250723140000||ORD_FAC|EMR_TERM_123|||||||MOD1^Example Modifier^HL70339||||||\rOBR|1|76684826^ORD_APP|71604357^RAD_SYS|71046^Chest X-Ray (2 views)^C4|S|20250723134000|20250723140000|||TECH123^JONES^ROBERT|O|INFECT^Infectious Material|Patient c/o shortness of breath and persistent cough for 3 days. R/O pneumonia.|||67890^CASEY^BEN^J^DR^MD|(555)555-9999|||Scheduled AE Title: CHEST_MOD|Protocol Code: P-CHEST-2V|||CR|S||1^^^^^S|9876^KILDARE^JAMES^A^DR^MD||WHEELCHAIR|233604007^Pneumonia^SCT|RADTECH^RAD^TECH||TECH123^JONES^ROBERT||20250723140000||||||||71046^Chest X-Ray (2 views)^C4|LT^Left Side||CR|||\rZDS|1|1.2.840.10008.5.1.4.1.1.2.1634887964.981.54708245.71604357|CHEST_MOD^1.2.3.4.5.6.7^AETITLE|WHEELCHAIR```\r	\N	27	2025-07-23 13:41:25.879755+00	2025-07-23 13:57:20.884418+00
\.


--
-- Data for Name: processed_study_log; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.processed_study_log (source_type, source_id, study_instance_uid, first_seen_at, retrieval_queued_at, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: roles; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.roles (name, description, id, created_at, updated_at) FROM stdin;
Admin	Full administrative privileges	1	2025-07-23 17:16:21.390359+00	2025-07-23 17:16:21.390359+00
User	Standard user privileges	2	2025-07-23 17:16:21.390359+00	2025-07-23 17:16:21.390359+00
\.


--
-- Data for Name: rule_sets; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.rule_sets (name, description, is_active, priority, execution_mode, id, created_at, updated_at) FROM stdin;
Test Set	Rule container	t	0	ALL_MATCHES	1	2025-06-22 20:48:12.058684+00	2025-06-22 20:48:12.058684+00
\.


--
-- Data for Name: schedules; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.schedules (name, description, is_enabled, time_ranges, id, created_at, updated_at) FROM stdin;
\.


--
-- Data for Name: rules; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.rules (name, description, is_active, priority, ruleset_id, match_criteria, association_criteria, tag_modifications, applicable_sources, ai_prompt_config_ids, schedule_id, id, created_at, updated_at) FROM stdin;
Basic morph	Adjust accesison number	t	100	1	[{"op": "exists", "tag": "0010,0020", "value": null}]	null	[{"tag": "0008,0050", "value": "TEST_", "action": "prepend"}]	["DIMSE C-STORE SCP TLS"]	null	\N	1	2025-06-22 20:49:24.398622+00	2025-06-22 20:49:24.398622+00
\.


--
-- Data for Name: storage_backend_configs; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.storage_backend_configs (name, description, backend_type, is_enabled, id, created_at, updated_at, path, bucket, prefix, remote_ae_title, remote_host, remote_port, local_ae_title, tls_enabled, tls_ca_cert_secret_name, tls_client_cert_secret_name, tls_client_key_secret_name, gcp_project_id, gcp_location, gcp_dataset_id, gcp_dicom_store_id, base_url, auth_type, basic_auth_username_secret_name, basic_auth_password_secret_name, bearer_token_secret_name, api_key_secret_name, api_key_header_name_override) FROM stdin;
Local Filesystem	\N	filesystem	t	1	2025-06-22 19:17:23.101229+00	2025-06-22 19:17:23.101229+00	/dicom_data/processed	\N	\N	\N	\N	\N	AXIOM_STORE_SCU	f	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
Orthanc DIMSE TLS	\N	cstore	t	2	2025-06-22 19:18:39.846561+00	2025-06-22 19:18:39.846561+00	\N	\N	\N	ORTHANC_TEST	orthanc	4242	AXIOM_QR_SCU	t	axiom-test-ca-cert	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
Google Cloud Storage [axiom-flow-test-0001]	\N	gcs	t	3	2025-06-22 19:19:33.975886+00	2025-06-22 19:19:33.975886+00	\N	axiom-flow-test-0001	test_iteration_02/	\N	\N	\N	AXIOM_STORE_SCU	f	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
GCP Healthcare API	Google Cloud Healthcare API DICOM Store	google_healthcare	t	4	2025-06-22 19:20:28.759337+00	2025-06-22 19:20:28.759337+00	\N	\N	\N	\N	\N	\N	AXIOM_STORE_SCU	f	\N	\N	\N	axiom-flow	us-central1	axiom-dataset-test-0001	axiom-dicom-store-0001	\N	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: rule_destination_association; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.rule_destination_association (rule_id, storage_backend_config_id) FROM stdin;
1	2
\.


--
-- Data for Name: user_role_association; Type: TABLE DATA; Schema: public; Owner: dicom_processor_user
--

COPY public.user_role_association (user_id, role_id) FROM stdin;
1	2
1	1
\.


--
-- Name: a_i_prompt_configs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.a_i_prompt_configs_id_seq', 1, false);


--
-- Name: api_keys_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.api_keys_id_seq', 1, false);


--
-- Name: crosswalk_data_sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.crosswalk_data_sources_id_seq', 1, false);


--
-- Name: crosswalk_maps_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.crosswalk_maps_id_seq', 1, false);


--
-- Name: dicom_exception_log_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.dicom_exception_log_id_seq', 20, true);


--
-- Name: dicomweb_source_state_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.dicomweb_source_state_id_seq', 1, true);


--
-- Name: dimse_listener_configs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.dimse_listener_configs_id_seq', 2, true);


--
-- Name: dimse_listener_state_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.dimse_listener_state_id_seq', 2, true);


--
-- Name: dimse_qr_sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.dimse_qr_sources_id_seq', 1, true);


--
-- Name: google_healthcare_sources_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.google_healthcare_sources_id_seq', 1, true);


--
-- Name: imaging_orders_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.imaging_orders_id_seq', 27, true);


--
-- Name: processed_study_log_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.processed_study_log_id_seq', 1, false);


--
-- Name: roles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.roles_id_seq', 2, true);


--
-- Name: rule_sets_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.rule_sets_id_seq', 1, true);


--
-- Name: rules_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.rules_id_seq', 1, true);


--
-- Name: schedules_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.schedules_id_seq', 1, false);


--
-- Name: storage_backend_configs_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.storage_backend_configs_id_seq', 4, true);


--
-- Name: users_id_seq; Type: SEQUENCE SET; Schema: public; Owner: dicom_processor_user
--

SELECT pg_catalog.setval('public.users_id_seq', 1, true);


--
-- PostgreSQL database dump complete
--

