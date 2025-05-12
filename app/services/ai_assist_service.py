# filename: backend/app/services/ai_assist_service.py
import asyncio
import concurrent.futures
import json
import os
from typing import Optional, Dict, Any, Tuple
import enum
from app.schemas.ai_prompt_config import AIPromptConfigRead

import structlog
logger = structlog.get_logger(__name__)

from app.core.config import settings # For all configurable parameters

# GCP Utils Import
try:
    from app.core import gcp_utils
    from app.core.gcp_utils import SecretManagerError, SecretNotFoundError, PermissionDeniedError
    GCP_UTILS_IMPORT_SUCCESS = True
except ImportError as gcp_import_err:
     logger.error("Failed to import app.core.gcp_utils", error_details=str(gcp_import_err), exc_info=True)
     GCP_UTILS_IMPORT_SUCCESS = False
     # Define dummy exceptions if import fails, so type hints and except blocks don't break
     class SecretManagerError(Exception): pass # type: ignore
     class SecretNotFoundError(SecretManagerError): pass # type: ignore
     class PermissionDeniedError(SecretManagerError): pass # type: ignore

# ThreadPoolExecutor for AI calls
# This executor is used by the synchronous wrappers to run async AI functions.
thread_pool_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=settings.AI_THREAD_POOL_WORKERS
)

# OpenAI Client
try:
    from openai import AsyncOpenAI, APIError, RateLimitError, APIConnectionError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    class AsyncOpenAI: pass # type: ignore
    class APIError(Exception): pass # type: ignore
    class RateLimitError(APIError): pass # type: ignore
    class APIConnectionError(APIError): pass # type: ignore
    class ChatCompletionMessage: pass # type: ignore
    class Choice: pass # type: ignore
    class CompletionUsage: pass # type: ignore

if not OPENAI_AVAILABLE:
    logger.warning("OpenAI library not found. OpenAI features unavailable.")

# Vertex AI Client
logger.info("Attempting Vertex AI library imports...")
try:
    import vertexai
    # Note: UsageMetadata is NOT imported here directly
    from vertexai.generative_models import GenerativeModel, Part, FinishReason
    import vertexai.preview.generative_models as generative_models_preview
    from google.oauth2 import service_account
    from google.auth import default as google_auth_default
    from google.auth.exceptions import DefaultCredentialsError
    from google.api_core import exceptions as google_api_exceptions
    VERTEX_AI_AVAILABLE = True
    logger.info("Vertex AI library imports successful.")
except ImportError as vertex_import_err:
    VERTEX_AI_AVAILABLE = False
    logger.warning("Failed to import Vertex AI libraries. Vertex AI features unavailable.", error_details=str(vertex_import_err))
    # Define dummy classes/enums if import fails
    class GenerativeModel: pass # type: ignore
    class Part: pass # type: ignore
    class FinishReason(enum.Enum): STOP=0; MAX_TOKENS=1; SAFETY=2; RECITATION=3; OTHER=4; UNSPECIFIED=5 # type: ignore
    # NO mock UsageMetadata class here
    class generative_models_preview: # type: ignore
         HarmCategory = enum.Enum('HarmCategory', ['HARM_CATEGORY_HATE_SPEECH', 'HARM_CATEGORY_DANGEROUS_CONTENT', 'HARM_CATEGORY_HARASSMENT', 'HARM_CATEGORY_SEXUALLY_EXPLICIT']) # type: ignore
         HarmBlockThreshold = enum.Enum('HarmBlockThreshold', ['BLOCK_MEDIUM_AND_ABOVE', 'BLOCK_LOW_AND_ABOVE', 'BLOCK_ONLY_HIGH', 'BLOCK_NONE']) # type: ignore
    class service_account: pass # type: ignore
    class google_auth_default: pass # type: ignore
    class DefaultCredentialsError(Exception): pass # type: ignore
    class google_api_exceptions: pass # type: ignore

# Synchronous Redis Client (for counter, if enabled)
try:
    import redis # Sync redis client
    REDIS_SYNC_CLIENT_AVAILABLE = True
except ImportError:
    REDIS_SYNC_CLIENT_AVAILABLE = False
    class redis: # type: ignore
        class Redis: # type: ignore
            @staticmethod
            def from_url(*args, **kwargs): return None
            def __getattr__(self, name): return lambda *args, **kwargs: None # type: ignore
if not REDIS_SYNC_CLIENT_AVAILABLE:
    logger.warning("Synchronous redis library not found. AI invocation counting will be unavailable.")

from app.schemas.ai_assist import RuleGenRequest, RuleGenResponse, RuleGenSuggestion
from app.schemas.rule import MatchOperation, ModifyAction

openai_client: Optional[AsyncOpenAI] = None
if OPENAI_AVAILABLE and settings.OPENAI_API_KEY:
    try:
        # Ensure OPENAI_API_KEY is accessed correctly (it's a Pydantic SecretStr)
        api_key_value = settings.OPENAI_API_KEY.get_secret_value() if settings.OPENAI_API_KEY else None
        if api_key_value:
            openai_client = AsyncOpenAI(api_key=api_key_value)
            logger.info("OpenAI client initialized.")
        else:
            logger.warning("OpenAI API Key is configured but empty. OpenAI features disabled.")
    except Exception as e:
        logger.error("Failed to initialize OpenAI client.", error_details=str(e), exc_info=True)
elif OPENAI_AVAILABLE:
     logger.warning("OpenAI API Key not configured. OpenAI features disabled.")


VERTEX_AI_INITIALIZED_SUCCESSFULLY: bool = False
if globals().get('VERTEX_AI_AVAILABLE', False) and settings.VERTEX_AI_PROJECT:
    logger.info("VERTEX_AI_INIT: Starting Initialization...")
    credentials = None
    project_id = settings.VERTEX_AI_PROJECT
    location = settings.VERTEX_AI_LOCATION

    try:
        # 1. Try Secret Manager for credentials JSON
        if settings.VERTEX_AI_CREDENTIALS_SECRET_ID and GCP_UTILS_IMPORT_SUCCESS and getattr(gcp_utils, 'GCP_SECRET_MANAGER_AVAILABLE', False):
            logger.info(f"VERTEX_AI_INIT: Attempting to load credentials from Secret Manager: {settings.VERTEX_AI_CREDENTIALS_SECRET_ID} in project {settings.VERTEX_AI_CONFIG_PROJECT_ID or 'default'}")
            try:
                secret_json_str = gcp_utils.access_secret_version(
                    project_id=settings.VERTEX_AI_CONFIG_PROJECT_ID or project_id, # Project where secret resides
                    secret_id=settings.VERTEX_AI_CREDENTIALS_SECRET_ID,
                    version_id="latest"
                )
                if secret_json_str:
                    secret_info = json.loads(secret_json_str)
                    credentials = service_account.Credentials.from_service_account_info(secret_info)
                    logger.info("VERTEX_AI_INIT: Successfully loaded credentials from Secret Manager.")
                else:
                    logger.warning("VERTEX_AI_INIT: Secret content from Secret Manager was empty.")
            except SecretNotFoundError:
                logger.warning("VERTEX_AI_INIT: Credentials secret not found in Secret Manager.")
            except PermissionDeniedError:
                logger.error("VERTEX_AI_INIT: Permission denied accessing credentials secret in Secret Manager.")
            except SecretManagerError as sm_err:
                logger.error("VERTEX_AI_INIT: Error loading credentials from Secret Manager.", error_details=str(sm_err))
            except json.JSONDecodeError:
                logger.error("VERTEX_AI_INIT: Failed to parse credentials JSON from Secret Manager.")

        # 2. Try File Path for credentials JSON (if not loaded from Secret Manager)
        if not credentials and settings.VERTEX_AI_CREDENTIALS_JSON_PATH:
            logger.info(f"VERTEX_AI_INIT: Attempting to load credentials from JSON file path: {settings.VERTEX_AI_CREDENTIALS_JSON_PATH}")
            try:
                if os.path.exists(settings.VERTEX_AI_CREDENTIALS_JSON_PATH):
                    credentials = service_account.Credentials.from_service_account_file(settings.VERTEX_AI_CREDENTIALS_JSON_PATH)
                    logger.info("VERTEX_AI_INIT: Successfully loaded credentials from JSON file.")
                else:
                    logger.warning(f"VERTEX_AI_INIT: Credentials JSON file not found at path: {settings.VERTEX_AI_CREDENTIALS_JSON_PATH}")
            except Exception as file_err:
                logger.error("VERTEX_AI_INIT: Error loading credentials from JSON file.", error_details=str(file_err))

        # 3. Try Application Default Credentials (ADC) (if not loaded from above methods)
        if not credentials:
            logger.info("VERTEX_AI_INIT: Attempting to use Application Default Credentials (ADC).")
            try:
                # For ADC, project is usually inferred or can be set via gcloud config
                # Scopes might be needed depending on the environment
                credentials, inferred_project_id = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
                logger.info(f"VERTEX_AI_INIT: Successfully obtained ADC. Inferred project: {inferred_project_id}")
                if not project_id and inferred_project_id: # If main project_id wasn't set, use inferred one
                    project_id = inferred_project_id
                    logger.info(f"VERTEX_AI_INIT: Using inferred project ID from ADC: {project_id}")
            except DefaultCredentialsError as adc_err:
                logger.error("VERTEX_AI_INIT: Failed to obtain Application Default Credentials.", error_details=str(adc_err))
            except Exception as e: # Catch any other exception during google_auth_default
                logger.error("VERTEX_AI_INIT: Unexpected error obtaining Application Default Credentials.", error_details=str(e), exc_info=True)

        if not project_id:
            logger.error("VERTEX_AI_INIT: Vertex AI Project ID is not set and could not be inferred. Initialization failed.")
            VERTEX_AI_INITIALIZED_SUCCESSFULLY = False
        elif credentials:
            vertexai.init(project=project_id, location=location, credentials=credentials)
            logger.info(f"VERTEX_AI_INIT: vertexai.init() successful for project '{project_id}' and location '{location}' with provided credentials.")
            VERTEX_AI_INITIALIZED_SUCCESSFULLY = True
        else: # ADC might work without explicit credentials object if env is set up (e.g. GCE metadata server)
            logger.info("VERTEX_AI_INIT: No explicit credentials provided or loaded, relying on environment for ADC for vertexai.init().")
            vertexai.init(project=project_id, location=location) # Let SDK try to find creds
            logger.info(f"VERTEX_AI_INIT: vertexai.init() successful for project '{project_id}' and location '{location}' (likely via environment ADC).")
            VERTEX_AI_INITIALIZED_SUCCESSFULLY = True

    except Exception as e_outer:
        logger.error("VERTEX_AI_INIT: Unhandled error during Vertex AI initialization sequence.", error_details=str(e_outer), exc_info=True)
        VERTEX_AI_INITIALIZED_SUCCESSFULLY = False
else:
    if not globals().get('VERTEX_AI_AVAILABLE', False):
        logger.info("Vertex AI libraries not available. Vertex AI features disabled.")
    if not settings.VERTEX_AI_PROJECT:
        logger.info("Vertex AI Project not configured (VERTEX_AI_PROJECT setting). Vertex AI features disabled.")


SYSTEM_PROMPT_RULE_GEN = f"""
You are an expert DICOM and clinical informatics assistant.
Your task is to generate a single JSON object representing a DICOM processing rule based on a natural language request.
The JSON object must conform to the following structure:
{{
  "name": "Descriptive Rule Name",
  "description": "Detailed explanation of what the rule does.",
  "match_criteria": {{
    "condition": "AND" / "OR", // How multiple tag/association criteria are combined
    "tag_criteria": [ // Optional: criteria based on DICOM tag values
      {{
        "tag_group": "0010", // DICOM Tag Group (hex)
        "tag_element": "0010", // DICOM Tag Element (hex)
        "value": "PATIENT_NAME", // Value to match. For numeric types, ensure it's a string.
        "op": "EQUALS" // MatchOperation, see below
      }}
    ],
    "association_criteria": [ // Optional: criteria based on source/destination of DICOM instance
        {{
            "association_type": "SOURCE_AE_TITLE", // Or DESTINATION_AE_TITLE, SOURCE_IP, DESTINATION_IP
            "value": "MY_PACS_AE",
            "op": "EQUALS"
        }}
    ]
  }},
  "modifications": [ // Optional: actions to modify DICOM tags
    {{
      "action": "SET", // ModifyAction, see below
      "tag_group": "0008",
      "tag_element": "0070",
      "value": "MegaCorp" // New value for the tag
    }},
    {{
      "action": "REMOVE",
      "tag_group": "0010",
      "tag_element": "0020" // Tag to remove (PatientID)
    }}
  ],
  "destinations": [ // Optional: names of pre-configured destination systems
    "ArchiveSystemX", "ResearchRepoY"
  ],
  "priority": 100, // Integer, lower numbers run first
  "is_enabled": true // boolean
}}

Available MatchOperation ("op") values for "tag_criteria" and "association_criteria":
{json.dumps([op.value for op in MatchOperation], indent=2)}

Available ModifyAction ("action") values for "modifications":
{json.dumps([action.value for action in ModifyAction], indent=2)}

Key Considerations:
1.  "name" and "description" should be human-readable and informative.
2.  "match_criteria" is mandatory. It must have "condition" and at least one of "tag_criteria" or "association_criteria".
3.  All DICOM tag groups and elements must be 4-character hexadecimal strings.
4.  "value" in "tag_criteria" should be a string, even for numeric DICOM tags.
5.  If "modifications" are present, each item must have "action", "tag_group", and "tag_element". "value" is required for actions like "SET", "ADD", "PREPEND", "APPEND".
6.  "destinations" is an array of strings. These strings must correspond to names of pre-configured destination systems.
7.  "priority" is an integer. Lower numbers indicate higher priority. A common default is 100.
8.  "is_enabled" is a boolean, typically true for new rules.
9.  If the request implies matching a specific DICOM tag (e.g., "Patient ID is '12345'"), use the correct hexadecimal tag group and element (e.g., PatientID is (0010,0020)).
10. If the request implies routing or sending to a specific system, list its name in the "destinations" array.
11. If the request implies changing a tag value, use "SET" action. If adding a new value to a multi-valued tag, use "ADD".
12. If the request is vague about a specific tag, try to infer the most common one (e.g., "institution" likely means InstitutionName (0008,0080)).
13. For matching non-existence of a tag, use "NOT_EXISTS". For existence, use "EXISTS".
14. For string comparisons, "CONTAINS", "STARTS_WITH", "ENDS_WITH" are useful. "REGEX" for complex patterns.
15. Respond ONLY with the JSON object. No explanations, greetings, or other text.
"""

async def generate_rule_suggestion(request: RuleGenRequest) -> RuleGenResponse:
    if not OPENAI_AVAILABLE or not openai_client:
        logger.warning("OpenAI client not available or not initialized for rule generation.")
        return RuleGenResponse(error="OpenAI features are not available or not configured.")

    log = logger.bind(request_text=request.request_text, model_name=settings.OPENAI_MODEL_NAME_RULE_GEN)
    log.info("Generating DICOM rule suggestion.")

    try:
        completion = await openai_client.chat.completions.create(
            model=settings.OPENAI_MODEL_NAME_RULE_GEN,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_RULE_GEN},
                {"role": "user", "content": request.request_text}
            ],
            temperature=settings.OPENAI_TEMPERATURE_RULE_GEN,
            max_tokens=settings.OPENAI_MAX_TOKENS_RULE_GEN,
            response_format={"type": "json_object"}
        )

        # Extract token usage for logging
        prompt_tokens, completion_tokens, total_tokens = "N/A", "N/A", "N/A"
        if completion and hasattr(completion, 'usage') and completion.usage:
            prompt_tokens = getattr(completion.usage, 'prompt_tokens', 'N/A')
            completion_tokens = getattr(completion.usage, 'completion_tokens', 'N/A')
            total_tokens = getattr(completion.usage, 'total_tokens', 'N/A')
        
        log.debug("OpenAI API call complete.",
                  openai_prompt_tokens=prompt_tokens,
                  openai_completion_tokens=completion_tokens,
                  openai_total_tokens=total_tokens)

        if not completion.choices or not completion.choices[0].message.content:
            log.warning("OpenAI response was empty or malformed.")
            return RuleGenResponse(error="Failed to generate rule: OpenAI response was empty.")

        generated_json_str = completion.choices[0].message.content
        log.debug("Raw JSON response from OpenAI.", raw_json=generated_json_str)

        try:
            generated_rule_dict = json.loads(generated_json_str)
            suggestion = RuleGenSuggestion(**generated_rule_dict)
            log.info("Successfully generated and parsed rule suggestion.",
                     openai_prompt_tokens=prompt_tokens,
                     openai_completion_tokens=completion_tokens,
                     openai_total_tokens=total_tokens)
            return RuleGenResponse(suggestion=suggestion)
        except json.JSONDecodeError as json_err:
            log.error("Failed to parse JSON from OpenAI response.", json_error=str(json_err), raw_response=generated_json_str)
            return RuleGenResponse(error=f"Failed to parse generated rule: {str(json_err)}. Raw: {generated_json_str[:200]}...")
        except Exception as pydantic_err: # Catch Pydantic validation errors
            log.error("Failed to validate generated rule against Pydantic schema.", pydantic_error=str(pydantic_err), raw_response=generated_json_str)
            return RuleGenResponse(error=f"Generated rule failed validation: {str(pydantic_err)}. Raw: {generated_json_str[:200]}...")

    except RateLimitError:
        log.error("OpenAI API rate limit exceeded.")
        return RuleGenResponse(error="OpenAI API rate limit exceeded. Please try again later.")
    except APIConnectionError:
        log.error("Failed to connect to OpenAI API.")
        return RuleGenResponse(error="Failed to connect to OpenAI API. Check network connectivity.")
    except APIError as api_err: # Catch other OpenAI API errors
        log.error("OpenAI API error.", api_error_details=str(api_err))
        return RuleGenResponse(error=f"OpenAI API error: {str(api_err)}")
    except Exception as e:
        log.error("An unexpected error occurred during rule generation.", error_details=str(e), exc_info=True)
        return RuleGenResponse(error=f"An unexpected error occurred: {str(e)}")


DEFAULT_SAFETY_SETTINGS = {}
if VERTEX_AI_AVAILABLE:
    DEFAULT_SAFETY_SETTINGS = {
        generative_models_preview.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models_preview.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        generative_models_preview.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models_preview.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        generative_models_preview.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models_preview.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        generative_models_preview.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models_preview.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    }

def _increment_invocation_count_sync(model_type: str, function_name: str):
    """Synchronously increments the AI invocation counter in Redis."""
    if not REDIS_SYNC_CLIENT_AVAILABLE:
        logger.debug("Sync Redis client library not available, skipping AI count increment.")
        return
    if not settings.AI_INVOCATION_COUNTER_ENABLED:
        logger.debug("AI invocation counter is disabled via settings, skipping increment.")
        return
    if not settings.REDIS_URL:
        logger.warning("Redis URL not configured, cannot increment AI count.")
        return

    # Instantiate Redis client per call for thread safety when called via asyncio.to_thread
    redis_client_instance = None
    log = logger.bind(counter_model_type=model_type, counter_function_name=function_name)
    try:
        redis_client_instance = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=2, socket_connect_timeout=2) # type: ignore
        counter_key = f"{settings.AI_INVOCATION_COUNTER_KEY_PREFIX}"
        field_key = f"{model_type}:{function_name}"
        redis_client_instance.hincrby(counter_key, field_key, 1)
        log.debug("Incremented AI invocation count (sync, per-call client).", counter_field=field_key)
    except redis.exceptions.TimeoutError as redis_timeout_err: # type: ignore
        log.warning("Redis timeout during AI count increment (sync, per-call client).", error_details=str(redis_timeout_err))
    except redis.exceptions.ConnectionError as redis_conn_err: # type: ignore
        log.warning("Redis connection error during AI count increment (sync, per-call client).", error_details=str(redis_conn_err))
    except Exception as redis_err:
        log.warning("Failed to increment AI count in Redis (sync, per-call client).", error_details=str(redis_err), exc_info=settings.LOG_LEVEL == "DEBUG")
    finally:
        if redis_client_instance:
            try:
                redis_client_instance.close()
            except Exception as close_err:
                log.warning("Error closing temporary Redis client for AI counter.", error_details=str(close_err))

async def _standardize_vocabulary_gemini_async(
    input_value: str,
    prompt_config: AIPromptConfigRead, # Caller passes the full Pydantic schema object
) -> Optional[str]:

    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY: # Global flag check
        logger.error("Vertex AI not initialized. Cannot standardize vocabulary using Gemini.")
        return None
        
    if not prompt_config: # Null check for the config object itself
        logger.error("Prompt configuration (AIPromptConfigRead object) not provided to async Gemini call.")
        return None

    log = logger.bind( # Bind common log attributes early
        ai_model_used=prompt_config.model_identifier,
        ai_prompt_config_id=prompt_config.id,
        ai_prompt_config_name=prompt_config.name,
        ai_dicom_tag_keyword=prompt_config.dicom_tag_keyword,
        ai_input_value_snippet=input_value[:80] + ("..." if len(input_value) > 80 else "")
    )

    if not input_value or not isinstance(input_value, str) or not input_value.strip():
        log.warning("Invalid or empty input for async standardization.",
                    received_input=input_value, # Full value might be sensitive, snippet is better
                    received_type=type(input_value).__name__)
        return None

    try:
        # Use model_identifier from the passed prompt_config
        local_gemini_model = GenerativeModel(prompt_config.model_identifier)
    except Exception as model_create_err:
        log.error("Failed to create GenerativeModel for async Gemini call.",
                  error_details=str(model_create_err), exc_info=True)
        return None

    # Use prompt_template from the passed prompt_config
    # The template should be designed to use {value} and optionally {dicom_tag_keyword}
    try:
        prompt = prompt_config.prompt_template.format(
            value=input_value,
            dicom_tag_keyword=prompt_config.dicom_tag_keyword # Make keyword available to prompt
        )
    except KeyError as fmt_err:
        log.error("Prompt template formatting error. Missing key.",
                  template_snippet=prompt_config.prompt_template[:100],
                  missing_key=str(fmt_err), exc_info=True)
        return None
    
    log.info("Attempting async vocabulary standardization with Gemini (using DB-driven config).")

    if settings.AI_INVOCATION_COUNTER_ENABLED:
        try:
            # Counter context key, e.g., "gemini:standardize_bodypartexamined" or "gemini:standardize_prompt_config_name_xyz"
            # Using dicom_tag_keyword for broad categorization seems reasonable.
            counter_function_name = f"standardize_{prompt_config.dicom_tag_keyword.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '')}"
            await asyncio.to_thread(_increment_invocation_count_sync, "gemini", counter_function_name)
        except Exception as e_redis_inc:
            log.warning("Submitting Redis sync increment call via to_thread failed.", error_details=str(e_redis_inc), exc_info=True)
            
    # --- GENERATION CONFIGURATION ---
    # Start with some sensible global defaults for generation if not in settings or overridden by prompt_config
    # These could come from settings.py for global Vertex AI defaults
    base_generation_config = {
        "max_output_tokens": settings.VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB,
        "temperature": settings.VERTEX_AI_TEMPERATURE_VOCAB,
        "top_p": settings.VERTEX_AI_TOP_P_VOCAB,
        # "top_k": settings.VERTEX_AI_TOP_K_VOCAB, # top_k often not used with top_p for Gemini
    }
    # Example of a settings-driven override for a specific known tag, if desired BEFORE DB config
    # This layer of override is optional. The DB config (model_parameters) is primary.
    # if prompt_config.dicom_tag_keyword == "BodyPartExamined":
    #     base_generation_config["temperature"] = getattr(settings, "VERTEX_AI_TEMPERATURE_BODYPART", 0.0)
    #     base_generation_config["max_output_tokens"] = getattr(
    #         settings, "VERTEX_AI_MAX_OUTPUT_TOKENS_BODYPART", base_generation_config["max_output_tokens"]
    #     )

    # Merge/override with parameters from the prompt_config.model_parameters
    # This ensures DB config takes precedence for these specific parameters.
    effective_model_params = prompt_config.model_parameters or {}
    current_gen_config_dict = {**base_generation_config, **effective_model_params}
    
    # Convert to Vertex AI's GenerationConfig object
    try:
        current_gen_config_obj = generative_models_preview.GenerationConfig(**current_gen_config_dict)
    except TypeError as te:
        log.error("Invalid type or unexpected keyword argument in generation_config from model_parameters.",
                  config_dict_used=current_gen_config_dict, error_details=str(te), exc_info=True)
        return None
    # --- END GENERATION CONFIGURATION ---

    log.debug("Using generation config for Gemini call.", gen_config_for_call=current_gen_config_dict)
    
    # Safety settings: For now, still using global DEFAULT_SAFETY_SETTINGS.
    # If these need to be configurable per prompt, they'd need to be in prompt_config.model_parameters too,
    # potentially under a specific key like "safety_settings" and then parsed.
    current_safety_settings = DEFAULT_SAFETY_SETTINGS if VERTEX_AI_AVAILABLE else {}

    gemini_prompt_tokens, gemini_candidates_tokens, gemini_total_tokens = "N/A", "N/A", "N/A"

    try:
        response = await local_gemini_model.generate_content_async(
            [prompt], # The formatted prompt string
            generation_config=current_gen_config_obj, # Pass the GenerationConfig object
            safety_settings=current_safety_settings,
            stream=False, # Non-streaming for this use case
        )

        # Extract token usage if available
        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            gemini_prompt_tokens = getattr(response.usage_metadata, 'prompt_token_count', 'N/A')
            gemini_candidates_tokens = getattr(response.usage_metadata, 'candidates_token_count', 'N/A')
            gemini_total_tokens = getattr(response.usage_metadata, 'total_token_count', 'N/A')
        
        raw_response_text_content = "" # Initialize
        if response.candidates and hasattr(response.candidates[0], 'content') and response.candidates[0].content and \
           hasattr(response.candidates[0].content, 'parts') and response.candidates[0].content.parts:
            # Collect text from all parts in the first candidate
            raw_response_text_content = "".join([getattr(part, 'text', '') for part in response.candidates[0].content.parts if hasattr(part, 'text')])

        finish_reason_val = getattr(getattr(response.candidates[0], 'finish_reason', None), 'name', 'N/A') if response.candidates else 'N/A'
        
        log.debug("Raw Gemini response received.",
                  finish_reason_candidate_0=finish_reason_val,
                  raw_text_candidate_0_snippet=raw_response_text_content[:100], # Log snippet
                  gemini_prompt_tokens=gemini_prompt_tokens,
                  gemini_candidates_tokens=gemini_candidates_tokens,
                  gemini_total_tokens=gemini_total_tokens)

        if not response.candidates:
            log.warning("Gemini returned no candidates.")
            return None
        
        candidate = response.candidates[0] # Assuming we only care about the first candidate
        finish_reason_enum_val = getattr(candidate, 'finish_reason', None)

        if finish_reason_enum_val == FinishReason.MAX_TOKENS:
            log.warning("Gemini hit MAX_TOKENS limit.",
                        max_tokens_set=current_gen_config_dict.get("max_output_tokens"),
                        received_partial_text=raw_response_text_content) # Log what was received
            return None # Or return partial if acceptable, but usually not for standardization
        
        if finish_reason_enum_val != FinishReason.STOP:
             log.warning("Gemini generation stopped for a non-STOP reason.",
                         reason=getattr(finish_reason_enum_val, 'name', 'UNKNOWN'))
        
        if hasattr(candidate, 'safety_ratings'):
             for rating in candidate.safety_ratings:
                  if hasattr(rating, 'blocked') and rating.blocked:
                       log.warning("Gemini response blocked by safety filter.",
                                   category=getattr(getattr(rating, 'category', None), 'name', 'UNKNOWN_CATEGORY'),
                                   probability=getattr(getattr(rating, 'probability', None), 'name', 'UNKNOWN_PROBABILITY'))
                       return None # Content was blocked
        
        standardized_text = raw_response_text_content.strip()
        if not standardized_text:
            log.warning("Gemini returned an empty string after stripping.",
                        original_raw_response_snippet=raw_response_text_content[:100],
                        finish_reason=finish_reason_val)
            return None 
            
        log.info("Successfully standardized term with Gemini (DB-driven config).",
                 standardized_term=standardized_text) # Full standardized term
        return standardized_text
        
    except google_api_exceptions.ResourceExhausted as e_resource: 
        log.error("Gemini API call failed due to resource exhaustion (e.g., quota).", error_details=str(e_resource), exc_info=True)
        return None
    except google_api_exceptions.InvalidArgument as e_invalid_arg: 
        log.error("Gemini API call failed due to invalid argument (check prompt, config).", error_details=str(e_invalid_arg), exc_info=True)
        return None
    except google_api_exceptions.GoogleAPICallError as e_gcall: 
        log.error("A Google API call error occurred during async Gemini call.", error_details=str(e_gcall), exc_info=True)
        return None
    except RuntimeError as e_runtime: # E.g. if asyncio loop is not running, though called from asyncio.run
        log.error("RuntimeError during async Gemini call.", error_details=str(e_runtime), exc_info=True)
        return None
    except Exception as e:
        log.error("Unexpected error during async Gemini call.", error_details=str(e), exc_info=True)
        return None

def standardize_vocabulary_gemini_sync(
    input_value: str,
    prompt_config: AIPromptConfigRead, # Caller passes the full Pydantic schema object
) -> Optional[str]:
    """
    Synchronous wrapper to call the async Gemini standardization function using a DB-driven prompt config.
    This is intended to be called from synchronous Celery tasks.
    Returns the standardized string, or None if standardization failed or was not possible.
    """

    # Early exit if Vertex AI is not even up
    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY: # Global flag check
        # Avoid binding log if prompt_config might be None here
        logger.error("Vertex AI not initialized. Sync wrapper for Gemini cannot proceed.",
                     ai_input_value_snippet=input_value[:80] if input_value else "N/A")
        return None
        
    # Check for prompt_config after VERTEX_AI_INITIALIZED_SUCCESSFULLY check
    if not prompt_config:
        logger.error("Prompt configuration (AIPromptConfigRead object) not provided to sync Gemini wrapper.",
                     ai_input_value_snippet=input_value[:80] if input_value else "N/A")
        return None

    log = logger.bind( # Bind common log attributes
        sync_wrapper_call_gemini=True,
        ai_prompt_config_id=prompt_config.id,
        ai_prompt_config_name=prompt_config.name,
        ai_dicom_tag_keyword=prompt_config.dicom_tag_keyword,
        ai_input_value_snippet=input_value[:80] + ("..." if len(input_value) > 80 else "")
    )
    
    if not input_value or not isinstance(input_value, str) or not input_value.strip():
        log.debug("Empty or invalid value passed to AI standardization sync wrapper, skipping.",
                  received_value_type=type(input_value).__name__) # Don't log potentially sensitive full value
        return None

    log.info("Executing sync wrapper for Gemini standardization (via thread pool, using DB-driven config).")

    def run_async_in_thread_capture_loop():
        # This function runs in a separate thread via ThreadPoolExecutor.
        # It creates its own event loop using asyncio.run().
        try:
            return asyncio.run(
                _standardize_vocabulary_gemini_async(
                    input_value,
                    prompt_config # Pass the full config object
                )
            )
        except RuntimeError as e:
            # Log context is bound in the outer scope (log variable)
            log.error("RuntimeError in thread's asyncio.run for Gemini. Likely event loop issue.",
                      error_details=str(e), exc_info=True)
            return None
        except Exception as e:
            log.error("Unexpected exception in thread's async Gemini call execution.",
                      error_details=str(e), exc_info=True)
            return None

    try:
        future = thread_pool_executor.submit(run_async_in_thread_capture_loop)
        # Timeout from settings, specific to this sync wrapper
        timeout_seconds = settings.AI_SYNC_WRAPPER_TIMEOUT
        
        # This blocks until the future completes or times out
        result = future.result(timeout=timeout_seconds)
        
        log.info("Sync wrapper for Gemini (thread pool, DB-driven config) completed.",
                 result_is_none=(result is None), result_snippet=result[:50] if result else "None",
                 timeout_used_seconds=timeout_seconds)
        return result
    except concurrent.futures.TimeoutError:
        log.error("Sync wrapper for Gemini timed out.", timeout_value_seconds=timeout_seconds)
        # It's good practice to try and cancel the future if it's still running,
        # though the work inside might continue until its next yield point or completion.
        if future.running():
            future.cancel()
            log.info("Attempted to cancel timed-out future.")
        return None
    except Exception as e:
        # This could be an error from submit() itself or other unexpected issues.
        log.error("Error submitting/getting result from thread pool for Gemini.",
                  error_details=str(e), exc_info=True)
        return None