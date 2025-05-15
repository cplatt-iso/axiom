# filename: backend/app/services/ai_assist_service.py
import asyncio
import concurrent.futures
import json
import os
from typing import Optional, Dict, Any, Tuple
import enum
import hashlib # For creating more robust cache keys if needed

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
     class SecretManagerError(Exception): pass # type: ignore
     class SecretNotFoundError(SecretManagerError): pass # type: ignore
     class PermissionDeniedError(SecretManagerError): pass # type: ignore

# ThreadPoolExecutor for AI calls
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
    class GenerativeModel: pass # type: ignore
    class Part: pass # type: ignore
    class FinishReason(enum.Enum): STOP=0; MAX_TOKENS=1; SAFETY=2; RECITATION=3; OTHER=4; UNSPECIFIED=5 # type: ignore
    class generative_models_preview: # type: ignore
         HarmCategory = enum.Enum('HarmCategory', ['HARM_CATEGORY_HATE_SPEECH', 'HARM_CATEGORY_DANGEROUS_CONTENT', 'HARM_CATEGORY_HARASSMENT', 'HARM_CATEGORY_SEXUALLY_EXPLICIT']) # type: ignore
         HarmBlockThreshold = enum.Enum('HarmBlockThreshold', ['BLOCK_MEDIUM_AND_ABOVE', 'BLOCK_LOW_AND_ABOVE', 'BLOCK_ONLY_HIGH', 'BLOCK_NONE']) # type: ignore
    class service_account: pass # type: ignore
    class google_auth_default: pass # type: ignore
    class DefaultCredentialsError(Exception): pass # type: ignore
    class google_api_exceptions: pass # type: ignore

# Synchronous Redis Client
redis_client_vocab_cache: Optional[Any] = None # For vocab caching
REDIS_CLIENT_AVAILABLE_FOR_CACHE = False

if settings.AI_VOCAB_CACHE_ENABLED and settings.REDIS_URL:
    try:
        import redis # Sync redis client
        redis_client_vocab_cache = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        # Test connection
        redis_client_vocab_cache.ping()
        REDIS_CLIENT_AVAILABLE_FOR_CACHE = True
        logger.info("Successfully initialized and connected to Redis for AI Vocab Cache.")
    except ImportError:
        logger.warning("Synchronous redis library not found. AI Vocab Cache will be unavailable.")
    except redis.exceptions.ConnectionError as e: # type: ignore
        logger.error(f"Failed to connect to Redis for AI Vocab Cache at {settings.REDIS_URL}. Caching disabled.", error_details=str(e))
        redis_client_vocab_cache = None # Ensure it's None if connection failed
    except Exception as e_redis_init:
        logger.error(f"An unexpected error occurred initializing Redis for AI Vocab Cache.", error_details=str(e_redis_init))
        redis_client_vocab_cache = None
else:
    if not settings.AI_VOCAB_CACHE_ENABLED:
        logger.info("AI Vocab Cache is disabled via settings (AI_VOCAB_CACHE_ENABLED=False).")
    if not settings.REDIS_URL:
        logger.warning("Redis URL not configured. AI Vocab Cache disabled.")


# Separate Redis client logic for counter to keep it independent
REDIS_SYNC_CLIENT_AVAILABLE_FOR_COUNTER = False # Renamed for clarity
if settings.AI_INVOCATION_COUNTER_ENABLED: # Counter check is separate from cache check
    try:
        import redis # Sync redis client (might be already imported)
        REDIS_SYNC_CLIENT_AVAILABLE_FOR_COUNTER = True
    except ImportError:
        REDIS_SYNC_CLIENT_AVAILABLE_FOR_COUNTER = False
        # logger already warned about redis lib for counter in its original spot below

if not REDIS_SYNC_CLIENT_AVAILABLE_FOR_COUNTER and settings.AI_INVOCATION_COUNTER_ENABLED: # Check if specifically counter is affected
    logger.warning("Synchronous redis library not found. AI invocation counting will be unavailable.")


from app.schemas.ai_assist import RuleGenRequest, RuleGenResponse, RuleGenSuggestion
from app.schemas.rule import MatchOperation, ModifyAction

openai_client: Optional[AsyncOpenAI] = None
if OPENAI_AVAILABLE and settings.OPENAI_API_KEY:
    try:
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
# ... (Vertex AI Initialization code remains IDENTICAL - no changes there) ...
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
                secret_json_str = gcp_utils.get_secret( # USE THE CACHED GET_SECRET
                    secret_id=settings.VERTEX_AI_CREDENTIALS_SECRET_ID,
                    project_id=settings.VERTEX_AI_CONFIG_PROJECT_ID or project_id, # Project where secret resides
                    version="latest"
                )
                if secret_json_str:
                    secret_info = json.loads(secret_json_str)
                    credentials = service_account.Credentials.from_service_account_info(secret_info)
                    logger.info("VERTEX_AI_INIT: Successfully loaded credentials from Secret Manager (via gcp_utils.get_secret).")
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
                credentials, inferred_project_id = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
                logger.info(f"VERTEX_AI_INIT: Successfully obtained ADC. Inferred project: {inferred_project_id}")
                if not project_id and inferred_project_id: 
                    project_id = inferred_project_id
                    logger.info(f"VERTEX_AI_INIT: Using inferred project ID from ADC: {project_id}")
            except DefaultCredentialsError as adc_err:
                logger.error("VERTEX_AI_INIT: Failed to obtain Application Default Credentials.", error_details=str(adc_err))
            except Exception as e: 
                logger.error("VERTEX_AI_INIT: Unexpected error obtaining Application Default Credentials.", error_details=str(e), exc_info=True)

        if not project_id:
            logger.error("VERTEX_AI_INIT: Vertex AI Project ID is not set and could not be inferred. Initialization failed.")
            VERTEX_AI_INITIALIZED_SUCCESSFULLY = False
        elif credentials:
            vertexai.init(project=project_id, location=location, credentials=credentials)
            logger.info(f"VERTEX_AI_INIT: vertexai.init() successful for project '{project_id}' and location '{location}' with provided credentials.")
            VERTEX_AI_INITIALIZED_SUCCESSFULLY = True
        else: 
            logger.info("VERTEX_AI_INIT: No explicit credentials provided or loaded, relying on environment for ADC for vertexai.init().")
            vertexai.init(project=project_id, location=location) 
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
...
""" # --- SYSTEM_PROMPT_RULE_GEN remains IDENTICAL ---

def clear_ai_vocab_cache(prompt_config_id: Optional[int] = None, input_value: Optional[str] = None) -> Dict[str, Any]:
    """
    Clears the AI Vocabulary Cache.
    - If no args, clears all keys matching AI_VOCAB_CACHE_KEY_PREFIX.
    - If prompt_config_id is given, clears keys for that specific config.
    - If prompt_config_id AND input_value are given, clears that specific entry.
    """
    log = logger.bind(action="clear_ai_vocab_cache", prompt_config_id_filter=prompt_config_id, input_value_filter_present=bool(input_value))

    if not settings.AI_VOCAB_CACHE_ENABLED:
        msg = "AI Vocab Cache is not enabled in settings."
        log.warning(msg)
        return {"status": "warning", "message": msg, "keys_deleted": 0}
    
    if not REDIS_CLIENT_AVAILABLE_FOR_CACHE or not redis_client_vocab_cache:
        msg = "Redis client for AI Vocab Cache is not available or not initialized."
        log.warning(msg)
        return {"status": "warning", "message": msg, "keys_deleted": 0}

    keys_deleted_count = 0
    try:
        if prompt_config_id is not None and input_value is not None:
            # Clear specific entry
            normalized_input_for_key = input_value.strip().lower()
            specific_key = f"{settings.AI_VOCAB_CACHE_KEY_PREFIX}:{prompt_config_id}:{normalized_input_for_key}"
            if redis_client_vocab_cache.exists(specific_key): # Check if key exists before delete
                redis_client_vocab_cache.delete(specific_key)
                keys_deleted_count = 1
                msg = f"Deleted specific AI vocab cache key: {specific_key}"
                log.info(msg)
            else:
                msg = f"Specific AI vocab cache key not found: {specific_key}"
                log.info(msg)
            return {"status": "success", "message": msg, "keys_deleted": keys_deleted_count}
        
        elif prompt_config_id is not None:
            # Clear all entries for a specific prompt_config_id
            pattern_to_delete = f"{settings.AI_VOCAB_CACHE_KEY_PREFIX}:{prompt_config_id}:*"
            log.info(f"Attempting to delete AI vocab cache keys matching pattern: {pattern_to_delete}")
            # Use a temporary client for scan_iter if redis_client_vocab_cache is shared and might have issues with iterators
            # Or, if the main client is robust enough:
            for key in redis_client_vocab_cache.scan_iter(match=pattern_to_delete):
                redis_client_vocab_cache.delete(key)
                keys_deleted_count += 1
            msg = f"Cleared AI vocabulary cache for prompt_config_id {prompt_config_id}. Keys matching '{pattern_to_delete}' deleted: {keys_deleted_count}."
            log.info(msg)
            return {"status": "success", "message": msg, "keys_deleted": keys_deleted_count}
            
        else:
            # Clear all AI vocab cache entries
            pattern_to_delete = f"{settings.AI_VOCAB_CACHE_KEY_PREFIX}:*"
            log.info(f"Attempting to delete ALL AI vocab cache keys matching pattern: {pattern_to_delete}")
            for key in redis_client_vocab_cache.scan_iter(match=pattern_to_delete):
                redis_client_vocab_cache.delete(key)
                keys_deleted_count += 1
            msg = f"Cleared ALL AI vocabulary cache. Keys matching '{pattern_to_delete}' deleted: {keys_deleted_count}."
            log.info(msg)
            return {"status": "success", "message": msg, "keys_deleted": keys_deleted_count}

    except redis.exceptions.RedisError as e_redis: # type: ignore
        log.error("Redis error during AI vocab cache clearing.", error_details=str(e_redis), exc_info=True)
        return {"status": "error", "message": f"Redis error during cache clearing: {str(e_redis)}", "keys_deleted": keys_deleted_count}
    except Exception as e:
        log.error("Unexpected error during AI vocab cache clearing.", error_details=str(e), exc_info=True)
        return {"status": "error", "message": f"Unexpected error during cache clearing: {str(e)}", "keys_deleted": keys_deleted_count}

async def generate_rule_suggestion(request: RuleGenRequest) -> RuleGenResponse:
# --- generate_rule_suggestion REMAINS IDENTICAL ---
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
        except Exception as pydantic_err: 
            log.error("Failed to validate generated rule against Pydantic schema.", pydantic_error=str(pydantic_err), raw_response=generated_json_str)
            return RuleGenResponse(error=f"Generated rule failed validation: {str(pydantic_err)}. Raw: {generated_json_str[:200]}...")

    except RateLimitError:
        log.error("OpenAI API rate limit exceeded.")
        return RuleGenResponse(error="OpenAI API rate limit exceeded. Please try again later.")
    except APIConnectionError:
        log.error("Failed to connect to OpenAI API.")
        return RuleGenResponse(error="Failed to connect to OpenAI API. Check network connectivity.")
    except APIError as api_err: 
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
    if not REDIS_SYNC_CLIENT_AVAILABLE_FOR_COUNTER: # Check specific flag for counter
        logger.debug("Sync Redis client library not available for counter, skipping AI count increment.")
        return
    if not settings.AI_INVOCATION_COUNTER_ENABLED:
        logger.debug("AI invocation counter is disabled via settings, skipping increment.")
        return
    if not settings.REDIS_URL:
        logger.warning("Redis URL not configured, cannot increment AI count.")
        return

    redis_client_instance_for_counter = None # Use a distinct name
    log = logger.bind(counter_model_type=model_type, counter_function_name=function_name)
    try:
        # This will re-import redis if it wasn't imported at the top for the cache client
        # It's a bit redundant but ensures counter works even if cache init failed.
        import redis as redis_for_counter 
        redis_client_instance_for_counter = redis_for_counter.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        counter_key = f"{settings.AI_INVOCATION_COUNTER_KEY_PREFIX}"
        field_key = f"{model_type}:{function_name}"
        redis_client_instance_for_counter.hincrby(counter_key, field_key, 1)
        log.debug("Incremented AI invocation count (sync, per-call client).", counter_field=field_key)
    except redis_for_counter.exceptions.TimeoutError as redis_timeout_err: # type: ignore
        log.warning("Redis timeout during AI count increment (sync, per-call client).", error_details=str(redis_timeout_err))
    except redis_for_counter.exceptions.ConnectionError as redis_conn_err: # type: ignore
        log.warning("Redis connection error during AI count increment (sync, per-call client).", error_details=str(redis_conn_err))
    except Exception as redis_err:
        log.warning("Failed to increment AI count in Redis (sync, per-call client).", error_details=str(redis_err), exc_info=settings.LOG_LEVEL == "DEBUG")
    finally:
        if redis_client_instance_for_counter:
            try:
                redis_client_instance_for_counter.close()
            except Exception as close_err:
                log.warning("Error closing temporary Redis client for AI counter.", error_details=str(close_err))

async def _standardize_vocabulary_gemini_async(
    input_value: str,
    prompt_config: AIPromptConfigRead,
) -> Optional[str]:

    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY:
        logger.error("Vertex AI not initialized. Cannot standardize vocabulary using Gemini.")
        return None
    if not prompt_config:
        logger.error("Prompt configuration (AIPromptConfigRead object) not provided to async Gemini call.")
        return None

    log = logger.bind(
        ai_model_used=prompt_config.model_identifier,
        ai_prompt_config_id=prompt_config.id,
        ai_prompt_config_name=prompt_config.name,
        ai_dicom_tag_keyword=prompt_config.dicom_tag_keyword,
        ai_input_value_snippet=input_value[:80] + ("..." if len(input_value) > 80 else "")
    )

    if not input_value or not isinstance(input_value, str) or not input_value.strip():
        log.warning("Invalid or empty input for async standardization.",
                    received_input_snippet=input_value[:80] if input_value else "N/A", # Log snippet
                    received_type=type(input_value).__name__)
        return None

    # --- AI Vocab Cache Check ---
    if settings.AI_VOCAB_CACHE_ENABLED and REDIS_CLIENT_AVAILABLE_FOR_CACHE and redis_client_vocab_cache:
        # Normalize input for better cache key consistency (optional, but good practice)
        # Using a hash for very long input_values might be better if they exceed Redis key limits
        # or contain problematic characters, but direct string is simpler for now.
        normalized_input_for_key = input_value.strip().lower() # Example normalization
        cache_key = f"{settings.AI_VOCAB_CACHE_KEY_PREFIX}:{prompt_config.id}:{normalized_input_for_key}"
        try:
            cached_value = redis_client_vocab_cache.get(cache_key)
            if cached_value is not None:
                log.info("AI Vocab Cache HIT.", cache_key=cache_key, cached_value=cached_value)
                # Increment counter even on cache hit, as an "AI-guided standardization" still occurred.
                # Or decide if only actual API calls should be counted. For now, let's count it as "used".
                if settings.AI_INVOCATION_COUNTER_ENABLED:
                    counter_function_name = f"standardize_{prompt_config.dicom_tag_keyword.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '')}_cached"
                    await asyncio.to_thread(_increment_invocation_count_sync, "gemini_cache_hit", counter_function_name)
                return str(cached_value) # Return cached value (already a string from Redis decode_responses=True)
        except Exception as e_cache_get:
            log.warning("Error getting from AI Vocab Cache. Proceeding to AI call.", cache_key=cache_key, error_details=str(e_cache_get))
    # --- End AI Vocab Cache Check ---

    try:
        local_gemini_model = GenerativeModel(prompt_config.model_identifier)
    except Exception as model_create_err:
        log.error("Failed to create GenerativeModel for async Gemini call.", error_details=str(model_create_err), exc_info=True)
        return None

    try:
        prompt = prompt_config.prompt_template.format(value=input_value, dicom_tag_keyword=prompt_config.dicom_tag_keyword)
    except KeyError as fmt_err:
        log.error("Prompt template formatting error. Missing key.", template_snippet=prompt_config.prompt_template[:100], missing_key=str(fmt_err), exc_info=True)
        return None
    
    log.info("AI Vocab Cache MISS. Attempting async vocabulary standardization with Gemini (DB-driven config).") # Log MISS here

    if settings.AI_INVOCATION_COUNTER_ENABLED:
        try:
            counter_function_name = f"standardize_{prompt_config.dicom_tag_keyword.lower().replace(' ', '_').replace('(', '').replace(')', '').replace(',', '')}_api_call"
            await asyncio.to_thread(_increment_invocation_count_sync, "gemini_api_call", counter_function_name)
        except Exception as e_redis_inc:
            log.warning("Submitting Redis sync increment call via to_thread failed.", error_details=str(e_redis_inc), exc_info=True)
            
    base_generation_config = {
        "max_output_tokens": settings.VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB,
        "temperature": settings.VERTEX_AI_TEMPERATURE_VOCAB,
        "top_p": settings.VERTEX_AI_TOP_P_VOCAB,
    }
    effective_model_params = prompt_config.model_parameters or {}
    current_gen_config_dict = {**base_generation_config, **effective_model_params}
    
    try:
        current_gen_config_obj = generative_models_preview.GenerationConfig(**current_gen_config_dict)
    except TypeError as te:
        log.error("Invalid type or unexpected keyword argument in generation_config from model_parameters.", config_dict_used=current_gen_config_dict, error_details=str(te), exc_info=True)
        return None

    log.debug("Using generation config for Gemini call.", gen_config_for_call=current_gen_config_dict)
    current_safety_settings = DEFAULT_SAFETY_SETTINGS if VERTEX_AI_AVAILABLE else {}
    gemini_prompt_tokens, gemini_candidates_tokens, gemini_total_tokens = "N/A", "N/A", "N/A"

    try:
        response = await local_gemini_model.generate_content_async(
            [prompt],
            generation_config=current_gen_config_obj,
            safety_settings=current_safety_settings,
            stream=False,
        )

        if hasattr(response, 'usage_metadata') and response.usage_metadata:
            gemini_prompt_tokens = getattr(response.usage_metadata, 'prompt_token_count', 'N/A')
            gemini_candidates_tokens = getattr(response.usage_metadata, 'candidates_token_count', 'N/A')
            gemini_total_tokens = getattr(response.usage_metadata, 'total_token_count', 'N/A')
        
        raw_response_text_content = ""
        if response.candidates and hasattr(response.candidates[0], 'content') and response.candidates[0].content and \
           hasattr(response.candidates[0].content, 'parts') and response.candidates[0].content.parts:
            raw_response_text_content = "".join([getattr(part, 'text', '') for part in response.candidates[0].content.parts if hasattr(part, 'text')])

        finish_reason_val = getattr(getattr(response.candidates[0], 'finish_reason', None), 'name', 'N/A') if response.candidates else 'N/A'
        
        log.debug("Raw Gemini response received.",
                  finish_reason_candidate_0=finish_reason_val,
                  raw_text_candidate_0_snippet=raw_response_text_content[:100],
                  gemini_prompt_tokens=gemini_prompt_tokens,
                  gemini_candidates_tokens=gemini_candidates_tokens,
                  gemini_total_tokens=gemini_total_tokens)

        if not response.candidates:
            log.warning("Gemini returned no candidates.")
            return None
        
        candidate = response.candidates[0]
        finish_reason_enum_val = getattr(candidate, 'finish_reason', None)

        if finish_reason_enum_val == FinishReason.MAX_TOKENS:
            log.warning("Gemini hit MAX_TOKENS limit.", max_tokens_set=current_gen_config_dict.get("max_output_tokens"), received_partial_text=raw_response_text_content)
            return None
        
        if finish_reason_enum_val != FinishReason.STOP:
             log.warning("Gemini generation stopped for a non-STOP reason.", reason=getattr(finish_reason_enum_val, 'name', 'UNKNOWN'))
        
        if hasattr(candidate, 'safety_ratings'):
             for rating in candidate.safety_ratings:
                  if hasattr(rating, 'blocked') and rating.blocked:
                       log.warning("Gemini response blocked by safety filter.", category=getattr(getattr(rating, 'category', None), 'name', 'UNKNOWN_CATEGORY'), probability=getattr(getattr(rating, 'probability', None), 'name', 'UNKNOWN_PROBABILITY'))
                       return None
        
        standardized_text = raw_response_text_content.strip()
        if not standardized_text:
            log.warning("Gemini returned an empty string after stripping.", original_raw_response_snippet=raw_response_text_content[:100], finish_reason=finish_reason_val)
            return None

        # --- Store in AI Vocab Cache ---
        if settings.AI_VOCAB_CACHE_ENABLED and REDIS_CLIENT_AVAILABLE_FOR_CACHE and redis_client_vocab_cache and standardized_text:
            # Use the same cache_key as above
            normalized_input_for_key = input_value.strip().lower() # Ensure consistency
            cache_key_to_set = f"{settings.AI_VOCAB_CACHE_KEY_PREFIX}:{prompt_config.id}:{normalized_input_for_key}"
            try:
                redis_client_vocab_cache.set(cache_key_to_set, standardized_text, ex=settings.AI_VOCAB_CACHE_TTL_SECONDS)
                log.info("Stored AI standardization result in cache.", cache_key=cache_key_to_set, value_stored=standardized_text)
            except Exception as e_cache_set:
                log.warning("Error setting AI Vocab Cache.", cache_key=cache_key_to_set, error_details=str(e_cache_set))
        # --- End Store in AI Vocab Cache ---
            
        log.info("Successfully standardized term with Gemini (DB-driven config).", standardized_term=standardized_text)
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
    except RuntimeError as e_runtime: 
        log.error("RuntimeError during async Gemini call.", error_details=str(e_runtime), exc_info=True)
        return None
    except Exception as e:
        log.error("Unexpected error during async Gemini call.", error_details=str(e), exc_info=True)
        return None

def standardize_vocabulary_gemini_sync(
    input_value: str,
    prompt_config: AIPromptConfigRead,
) -> Optional[str]:
    # --- standardize_vocabulary_gemini_sync REMAINS LARGELY IDENTICAL ---
    # It calls the async version which now has caching.
    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY: 
        logger.error("Vertex AI not initialized. Sync wrapper for Gemini cannot proceed.",
                     ai_input_value_snippet=input_value[:80] if input_value else "N/A")
        return None
    if not prompt_config:
        logger.error("Prompt configuration (AIPromptConfigRead object) not provided to sync Gemini wrapper.",
                     ai_input_value_snippet=input_value[:80] if input_value else "N/A")
        return None

    log = logger.bind( 
        sync_wrapper_call_gemini=True,
        ai_prompt_config_id=prompt_config.id,
        ai_prompt_config_name=prompt_config.name,
        ai_dicom_tag_keyword=prompt_config.dicom_tag_keyword,
        ai_input_value_snippet=input_value[:80] + ("..." if len(input_value) > 80 else "")
    )
    
    if not input_value or not isinstance(input_value, str) or not input_value.strip():
        log.debug("Empty or invalid value passed to AI standardization sync wrapper, skipping.",
                  received_value_type=type(input_value).__name__) 
        return None

    log.info("Executing sync wrapper for Gemini standardization (via thread pool, using DB-driven config).")

    def run_async_in_thread_capture_loop():
        try:
            return asyncio.run(
                _standardize_vocabulary_gemini_async(
                    input_value,
                    prompt_config
                )
            )
        except RuntimeError as e:
            log.error("RuntimeError in thread's asyncio.run for Gemini. Likely event loop issue.", error_details=str(e), exc_info=True)
            return None
        except Exception as e:
            log.error("Unexpected exception in thread's async Gemini call execution.", error_details=str(e), exc_info=True)
            return None

    try:
        future = thread_pool_executor.submit(run_async_in_thread_capture_loop)
        timeout_seconds = settings.AI_SYNC_WRAPPER_TIMEOUT
        result = future.result(timeout=timeout_seconds)
        
        log.info("Sync wrapper for Gemini (thread pool, DB-driven config) completed.",
                 result_is_none=(result is None), result_snippet=result[:50] if result else "None",
                 timeout_used_seconds=timeout_seconds)
        return result
    except concurrent.futures.TimeoutError:
        log.error("Sync wrapper for Gemini timed out.", timeout_value_seconds=timeout_seconds)
        if future.running():
            future.cancel()
            log.info("Attempted to cancel timed-out future.")
        return None
    except Exception as e:
        log.error("Error submitting/getting result from thread pool for Gemini.", error_details=str(e), exc_info=True)
        return None