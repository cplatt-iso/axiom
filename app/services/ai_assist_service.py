# filename: backend/app/services/ai_assist_service.py
import logging
import json
import os
from typing import Optional, Dict, Any

# --- Configuration and Utilities ---
from app.core.config import settings
# Import gcp_utils carefully, maybe defer? For now, keep standard import
try:
    from app.core import gcp_utils # Import the module
    from app.core.gcp_utils import ( # Import specific exceptions if needed for handling
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    GCP_UTILS_IMPORT_SUCCESS = True
except ImportError as gcp_import_err:
     # Log immediately if gcp_utils fails, as it might be needed by Vertex AI init
     logging.getLogger("ai_assist_service_startup").error(
         f"Failed to import app.core.gcp_utils: {gcp_import_err}", exc_info=True
         )
     GCP_UTILS_IMPORT_SUCCESS = False
     class SecretManagerError(Exception): pass
     class SecretNotFoundError(SecretManagerError): pass
     class PermissionDeniedError(SecretManagerError): pass


# --- OpenAI Client (Existing) ---
try:
    from openai import OpenAI, AsyncOpenAI, APIError, RateLimitError, APIConnectionError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    class OpenAI: pass
    class AsyncOpenAI: pass
    class APIError(Exception): pass
    class RateLimitError(APIError): pass
    class APIConnectionError(APIError): pass
    # Use the logger defined later
# --- End OpenAI Client ---

# --- Vertex AI Client ---
# Log before attempting Vertex AI imports
logging.getLogger("ai_assist_service_startup").info("Attempting Vertex AI library imports...")
try:
    import vertexai # type: ignore
    from vertexai.generative_models import GenerativeModel, Part, FinishReason # type: ignore
    import vertexai.preview.generative_models as generative_models # type: ignore
    from google.oauth2 import service_account # type: ignore
    from google.auth import default as google_auth_default # type: ignore
    from google.auth.exceptions import DefaultCredentialsError # type: ignore
    from google.api_core import exceptions as google_api_exceptions # type: ignore
    VERTEX_AI_AVAILABLE = True
    logging.getLogger("ai_assist_service_startup").info("Vertex AI library imports successful.")
except ImportError as vertex_import_err:
    VERTEX_AI_AVAILABLE = False
    logging.getLogger("ai_assist_service_startup").warning(
        f"Failed to import Vertex AI libraries: {vertex_import_err}. Vertex AI features unavailable.", exc_info=True
        )
    # Define dummy classes if Vertex AI library is not installed/imported
    class GenerativeModel: pass
    class Part: pass
    class FinishReason: pass
    class generative_models: pass
    class service_account: pass
    class google_auth_default: pass
    class DefaultCredentialsError(Exception): pass
    class google_api_exceptions: pass
# --- End Vertex AI Client ---

# --- Redis Client ---
try:
    import redis.asyncio as redis # type: ignore
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    # Define dummy class if redis library is not installed
    class redis: # type: ignore
        @staticmethod
        async def from_url(*args, **kwargs): return None
        def __getattr__(self, name): return lambda *args, **kwargs: None # Mock methods
    # Use the logger defined later
# --- End Redis Client ---


# --- Schemas (Existing) ---
from app.schemas.ai_assist import RuleGenRequest, RuleGenResponse, RuleGenSuggestion
# Import rule schemas carefully, avoid circular deps if possible
from app.schemas.rule import MatchOperation, ModifyAction

# --- Logging ---
# Set up logger *after* potential imports
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# Log missing optional dependencies (using the final logger)
if not OPENAI_AVAILABLE:
    logger.warning("OpenAI library was not found during import. OpenAI features will be unavailable.")
# VERTEX_AI_AVAILABLE logged during its import block
if not REDIS_AVAILABLE:
    logger.warning("redis library was not found during import. AI invocation counting will be unavailable.")

# --- Initialize OpenAI Client (Existing) ---
openai_client: Optional[AsyncOpenAI] = None
if OPENAI_AVAILABLE and settings.OPENAI_API_KEY:
    try:
        api_key = settings.OPENAI_API_KEY.get_secret_value()
        openai_client = AsyncOpenAI(api_key=api_key)
        logger.info("OpenAI client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {e}", exc_info=True)
        openai_client = None
elif OPENAI_AVAILABLE and not settings.OPENAI_API_KEY:
     logger.warning("OpenAI library is installed, but OPENAI_API_KEY is not configured. OpenAI features disabled.")
# --- End Initialize OpenAI Client ---


# --- ADDED: Log state BEFORE the Vertex AI Init block ---
try:
    # Check VERTEX_AI_AVAILABLE status AFTER its try/except block potentially ran
    logger.info(
        "PRE_VERTEX_AI_INIT_CHECK",
        vertex_ai_available_flag=VERTEX_AI_AVAILABLE,
        vertex_ai_project_setting=settings.VERTEX_AI_PROJECT,
        vertex_ai_project_setting_type=str(type(settings.VERTEX_AI_PROJECT))
    )
except NameError:
     # This shouldn't happen if the import block above executed, even if it failed
     logger.error("PRE_VERTEX_AI_INIT_CHECK: VERTEX_AI_AVAILABLE variable doesn't seem to exist?")
except Exception as e:
     logger.error(f"PRE_VERTEX_AI_INIT_CHECK: Error accessing settings: {e}", exc_info=True)
# --- End Log state ---


# --- Initialize Vertex AI Client with Detailed Logging ---
gemini_model: Optional[GenerativeModel] = None
# THE CRITICAL IF STATEMENT:
if VERTEX_AI_AVAILABLE and settings.VERTEX_AI_PROJECT:
    logger.info("VERTEX_AI_INIT: Starting Initialization...") # First log INSIDE the block
    credentials = None
    try:
        # 1. Try Secret Manager first
        if settings.VERTEX_AI_CREDENTIALS_SECRET_ID:
            logger.info(f"VERTEX_AI_INIT: Attempting Secret Manager creds (Secret ID: {settings.VERTEX_AI_CREDENTIALS_SECRET_ID})...")
            try:
                # Check if gcp_utils was imported successfully before calling
                # Use getattr to safely check for attributes if import might partially fail
                if getattr(gcp_utils, 'GCP_UTILS_IMPORT_SUCCESS', False) and getattr(gcp_utils, 'GCP_SECRET_MANAGER_AVAILABLE', False):
                    secret_json_str = gcp_utils.get_secret(
                        secret_id=settings.VERTEX_AI_CREDENTIALS_SECRET_ID,
                        project_id=settings.VERTEX_AI_PROJECT # Pass project_id for clarity
                    )
                    secret_info = json.loads(secret_json_str)
                    credentials = service_account.Credentials.from_service_account_info(secret_info)
                    logger.info("VERTEX_AI_INIT: Secret Manager creds loaded successfully.")
                else:
                    logger.error("VERTEX_AI_INIT: Cannot load Secret Manager creds because gcp_utils import failed or client unavailable.")
            except SecretManagerError as sm_err: # Catch specific errors from our util
                logger.error(f"VERTEX_AI_INIT: Failed Secret Manager cred loading via gcp_utils: {sm_err}", exc_info=True)
            except json.JSONDecodeError as json_err:
                 logger.error(f"VERTEX_AI_INIT: Failed to parse JSON from Secret Manager secret '{settings.VERTEX_AI_CREDENTIALS_SECRET_ID}': {json_err}", exc_info=True)
            except Exception as e:
                 logger.error(f"VERTEX_AI_INIT: Unexpected error loading credentials via Secret Manager: {e}", exc_info=True)

        else:
            logger.info("VERTEX_AI_INIT: Secret Manager Secret ID not configured.")

        # 2. Try local file path if Secret Manager didn't work or wasn't specified
        if not credentials and settings.VERTEX_AI_CREDENTIALS_JSON_PATH:
            key_path = settings.VERTEX_AI_CREDENTIALS_JSON_PATH
            logger.info(f"VERTEX_AI_INIT: Attempting file creds (Path: {key_path})...")
            if os.path.exists(key_path):
                 try:
                      credentials = service_account.Credentials.from_service_account_file(key_path)
                      logger.info("VERTEX_AI_INIT: File creds loaded successfully.")
                 except Exception as e:
                      logger.error(f"VERTEX_AI_INIT: Failed to load credentials from local file '{key_path}': {e}", exc_info=True)
                      credentials = None # Ensure creds are None on failure
            else:
                 logger.error(f"VERTEX_AI_INIT: Credentials file specified but not found: {key_path}")
                 credentials = None
        elif not credentials and settings.VERTEX_AI_CREDENTIALS_SECRET_ID:
             # Log if Secret Manager was configured but failed, and we are skipping file path
             logger.info("VERTEX_AI_INIT: Skipping file path check as Secret Manager creds were attempted (or failed).")
        elif not credentials:
             logger.info("VERTEX_AI_INIT: File path not configured.")


        # 3. Fallback to Application Default Credentials (ADC)
        if not credentials:
            logger.info("VERTEX_AI_INIT: Attempting ADC creds...")
            try:
                 # Specify scopes needed by Vertex AI? Usually cloud-platform is sufficient.
                 credentials, detected_project = google_auth_default(
                     scopes=["https://www.googleapis.com/auth/cloud-platform"]
                 )
                 logger.info(f"VERTEX_AI_INIT: ADC creds loaded successfully. Detected project: {detected_project}")
                 # Optionally check if detected_project matches settings.VERTEX_AI_PROJECT
                 if detected_project and detected_project != settings.VERTEX_AI_PROJECT:
                      logger.warning(f"VERTEX_AI_INIT: ADC detected project '{detected_project}' differs from settings project '{settings.VERTEX_AI_PROJECT}'. Using settings project.")
            except DefaultCredentialsError as adc_err:
                 logger.error(f"VERTEX_AI_INIT: Failed to get ADC: {adc_err}. Ensure ADC are configured correctly.", exc_info=False) # Less verbose for common ADC errors
                 credentials = None
            except Exception as adc_err:
                 logger.error(f"VERTEX_AI_INIT: Unexpected error getting ADC: {adc_err}", exc_info=True)
                 credentials = None

        # Initialize Vertex AI if we have credentials
        if credentials:
            logger.info("VERTEX_AI_INIT: Credentials obtained. Attempting vertexai.init()...", project=settings.VERTEX_AI_PROJECT, location=settings.VERTEX_AI_LOCATION)
            try:
                vertexai.init(
                    project=settings.VERTEX_AI_PROJECT,
                    location=settings.VERTEX_AI_LOCATION,
                    credentials=credentials
                )
                logger.info("VERTEX_AI_INIT: vertexai.init() successful. Loading model...", model_name=settings.VERTEX_AI_MODEL_NAME)

                gemini_model = GenerativeModel(settings.VERTEX_AI_MODEL_NAME)
                logger.info(f"VERTEX_AI_INIT: GenerativeModel loaded successfully: {gemini_model}") # Log the model object itself

            except Exception as init_err:
                logger.error(f"VERTEX_AI_INIT: Failed during vertexai.init() or GenerativeModel() loading: {init_err}", exc_info=True)
                gemini_model = None # Reset to None on error
        else:
            logger.error("VERTEX_AI_INIT: Could not obtain valid credentials. Vertex AI client NOT initialized.")
            gemini_model = None # Ensure it's None if no creds

    except Exception as e: # Catch-all for unexpected errors in the whole block
        logger.error(f"VERTEX_AI_INIT: Unexpected error during outer Vertex AI initialization block: {e}", exc_info=True)
        gemini_model = None # Ensure None on unexpected failure

# Check the condition variables *after* the block attempt
elif not VERTEX_AI_AVAILABLE:
     logger.warning("VERTEX_AI_INIT: Did not run - VERTEX_AI_AVAILABLE flag is False (Import likely failed).")
elif not settings.VERTEX_AI_PROJECT:
     logger.warning("VERTEX_AI_INIT: Did not run - VERTEX_AI_PROJECT setting is missing or empty.")
# --- End Initialize Vertex AI Client ---


# --- Initialize Redis Client (Keep Existing) ---
redis_client: Optional[redis.Redis] = None # type: ignore
if REDIS_AVAILABLE and settings.REDIS_URL and settings.AI_INVOCATION_COUNTER_ENABLED:
    logger.info("Attempting to initialize Redis client for AI invocation counting...")
    try:
        redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        logger.info(f"Redis client initialized successfully from URL: {settings.REDIS_URL}")
    except Exception as e:
        logger.error(f"Failed to initialize Redis client from URL '{settings.REDIS_URL}': {e}", exc_info=True)
        redis_client = None
elif REDIS_AVAILABLE and not settings.REDIS_URL:
    logger.warning("Redis library is installed, but REDIS_URL not configured. AI invocation counting disabled.")
elif REDIS_AVAILABLE and not settings.AI_INVOCATION_COUNTER_ENABLED:
     logger.info("AI invocation counting is disabled in settings.")
# --- End Initialize Redis Client ---


# --- Existing Rule Generation Logic (OpenAI) (Keep Existing) ---
# ... (SYSTEM_PROMPT and generate_rule_suggestion function remain the same) ...
SYSTEM_PROMPT = """
You are an expert assistant helping users configure DICOM routing and processing rules for the Axiom Flow system.
Your goal is to translate a user's natural language description of a rule into a structured JSON format that Axiom Flow can understand.

The user will provide a description of what they want the rule to do. You need to generate a JSON object containing the following possible keys: "match_criteria", "association_criteria", "tag_modifications", "applicable_sources".

1.  **match_criteria**: A list of objects, each specifying a condition based on DICOM tags.
    - Each object must have "tag" (e.g., "0008,0060" or "Modality"), "op" (one of the MatchOperation values below), and optionally "value".
    - Example: `{"tag": "Modality", "op": "eq", "value": "CT"}`
    - Available MatchOperation ("op") values: {MatchOperation.__members__.keys()}

2.  **association_criteria**: A list of objects, each specifying a condition based on DICOM association details.
    - Each object must have "parameter" (one of 'SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE'), "op", and "value".
    - Example: `{"parameter": "CALLING_AE_TITLE", "op": "eq", "value": "CT_SCANNER"}`
    - IP specific ops ("ip_eq", "ip_startswith", "ip_in_subnet") are only valid for the "SOURCE_IP" parameter.

3.  **tag_modifications**: A list of objects, each specifying an action to modify DICOM tags.
    - Each object must have an "action" key (one of the ModifyAction values below).
    - Other keys depend on the action (e.g., "tag", "value", "vr", "source_tag", "destination_tag", "pattern", "replacement", "crosswalk_map_id").
    - Always include the "action" field.
    - For "set", "copy", "move", specify "vr" (Value Representation, e.g., "PN", "DA", "LO") if known, otherwise the system might guess.
    - Examples:
        - `{"action": "set", "tag": "StudyDescription", "value": "Modified Study", "vr": "LO"}`
        - `{"action": "delete", "tag": "PatientComments"}`
        - `{"action": "copy", "source_tag": "AccessionNumber", "destination_tag": "0077,1010", "destination_vr": "SH"}`
        - `{"action": "regex_replace", "tag": "PatientName", "pattern": "\\^", "replacement": " "}`
        - `{"action": "crosswalk", "crosswalk_map_id": 1}`
    - Available ModifyAction ("action") values: {ModifyAction.__members__.keys()}

4.  **applicable_sources**: A list of strings identifying which input sources this rule should apply to. If omitted or null, it applies to all.
    - Example: `["CT_SCANNER_AE", "MRI_SCANNER_AE"]`

**Output Format:** Respond ONLY with a single JSON object containing the suggested keys ("match_criteria", "association_criteria", "tag_modifications", "applicable_sources"). Do not include any other text, explanations, or markdown formatting outside the JSON structure. If the user's request is ambiguous or cannot be translated, return a JSON object with an "error" key explaining the issue. Example error: `{"error": "Could not determine the specific DICOM tag for 'patient identifier'."}`. Use null for sections that are not applicable based on the prompt.
"""

async def generate_rule_suggestion(request: RuleGenRequest) -> RuleGenResponse:
    """
    Uses OpenAI's chat completion to generate rule suggestions based on a natural language prompt.
    (Requires OpenAI client to be configured)
    """
    if not OPENAI_AVAILABLE or not openai_client:
        logger.error("OpenAI client is not available or not configured for rule generation.")
        return RuleGenResponse(error="AI service (OpenAI) is not available or not configured.")

    logger.info(f"Generating rule suggestion via OpenAI for prompt: '{request.prompt[:50]}...'")

    try:
        completion = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo", # Consider making model configurable
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": request.prompt}
            ],
            temperature=0.2,
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        ai_response_content = completion.choices[0].message.content
        logger.debug(f"Raw OpenAI response content: {ai_response_content}")
        if not ai_response_content:
             logger.warning("OpenAI response content was empty.")
             return RuleGenResponse(error="OpenAI returned an empty response.")

        # Parse and Validate (Existing Logic)
        try:
            suggestion_dict = json.loads(ai_response_content)
            if not isinstance(suggestion_dict, dict): raise ValueError("OpenAI response was not a JSON object.")
            if "error" in suggestion_dict:
                logger.warning(f"OpenAI returned an error message: {suggestion_dict['error']}")
                return RuleGenResponse(error=suggestion_dict["error"])
            suggestion = RuleGenSuggestion(
                match_criteria=suggestion_dict.get("match_criteria"),
                association_criteria=suggestion_dict.get("association_criteria"),
                tag_modifications=suggestion_dict.get("tag_modifications"),
                applicable_sources=suggestion_dict.get("applicable_sources"),
            )
            logger.info("Successfully generated and parsed OpenAI rule suggestion.")
            return RuleGenResponse(suggestion=suggestion, explanation="Suggestion generated based on your prompt.")
        except (json.JSONDecodeError, ValueError) as parse_err:
            logger.error(f"Failed to parse/validate OpenAI response: {parse_err}. Response: {ai_response_content[:500]}", exc_info=True)
            return RuleGenResponse(error=f"AI response structure invalid: {parse_err}")
        except Exception as parse_err:
            logger.error(f"Unexpected error parsing OpenAI response: {parse_err}", exc_info=True)
            return RuleGenResponse(error=f"Error parsing AI response: {parse_err}")

    except APIConnectionError as e:
        logger.error(f"OpenAI API connection error: {e}")
        return RuleGenResponse(error=f"Could not connect to OpenAI service: {e}")
    except RateLimitError as e:
        logger.error(f"OpenAI API rate limit exceeded: {e}")
        return RuleGenResponse(error=f"OpenAI service rate limit exceeded. Please try again later. {e}")
    except APIError as e:
        logger.error(f"OpenAI API error: Status={e.status_code}, Message={e.message}", exc_info=True)
        return RuleGenResponse(error=f"OpenAI service returned an error: {e.message} (Status: {e.status_code})")
    except Exception as e:
        logger.error(f"Unexpected error during OpenAI rule generation: {e}", exc_info=True)
        return RuleGenResponse(error=f"An unexpected error occurred during AI rule generation: {e}")
# --- End Existing Rule Generation Logic ---


# --- NEW: Vocabulary Standardization Logic (Gemini) (Keep Existing) ---

# Define safety settings for Gemini - adjust as needed
DEFAULT_SAFETY_SETTINGS = {
    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
}

# Define generation config - adjust as needed
DEFAULT_GENERATION_CONFIG = {
    "max_output_tokens": 100, # Keep it short for vocab terms
    "temperature": 0.2,     # Low temp for consistency
    "top_p": 0.8,           # Nucleus sampling
    "top_k": 40,            # Top-k sampling
}

# Dictionary to map context to prompts - can be expanded
STANDARDIZATION_PROMPTS = {
    "StudyDescription": """
Given the following DICOM Study Description, provide a standardized clinical term or phrase suitable for indexing and querying.
Focus on the primary modality and body part or procedure. Respond ONLY with the standardized term. Do not add explanations.
Input Study Description: "{input_value}"
Standardized Term:""",

    "ProtocolName": """
Given the following DICOM Protocol Name, provide a standardized clinical term or phrase suitable for indexing and querying.
Focus on the primary modality and body part or procedure described in the protocol. Respond ONLY with the standardized term. Do not add explanations.
Input Protocol Name: "{input_value}"
Standardized Term:""",

    "Default": """
Standardize the following text input into a common term suitable for categorization. Respond ONLY with the standardized term.
Input: "{input_value}"
Standardized Term:"""
}

async def _increment_invocation_count(model_type: str, function_name: str):
    """Increments the AI invocation counter in Redis if enabled and available."""
    if redis_client and settings.AI_INVOCATION_COUNTER_ENABLED:
        try:
            # Use a hash for grouping counts by model/function
            counter_key = f"{settings.AI_INVOCATION_COUNTER_KEY_PREFIX}"
            field_key = f"{model_type}:{function_name}"
            await redis_client.hincrby(counter_key, field_key, 1)
            logger.debug(f"Incremented AI invocation count for {field_key}")
        except Exception as redis_err:
            # Don't let counter failure break the main logic
            logger.warning(f"Failed to increment AI invocation count in Redis for {field_key}: {redis_err}", exc_info=True)


async def standardize_vocabulary_gemini(
    input_value: str,
    context: str = "Default", # e.g., "StudyDescription", "ProtocolName"
    generation_config: Optional[Dict[str, Any]] = None,
    safety_settings: Optional[Dict[Any, Any]] = None
) -> Optional[str]:
    """
    Uses the configured Vertex AI Gemini model to standardize a given text value.

    Args:
        input_value: The string value to standardize (e.g., from a DICOM tag).
        context: Provides context for the standardization task (used to select prompt).
        generation_config: Optional override for Gemini generation parameters.
        safety_settings: Optional override for Gemini safety settings.

    Returns:
        The standardized string, or None if standardization fails or AI is unavailable.
    """
    global gemini_model # Ensure we are checking the potentially updated global var
    if not gemini_model:
        logger.error("Vertex AI Gemini model is not available or not configured for vocabulary standardization.")
        return None

    if not input_value or not isinstance(input_value, str):
        logger.warning("Invalid input value provided for standardization.")
        return None

    # Select prompt based on context
    prompt_template = STANDARDIZATION_PROMPTS.get(context, STANDARDIZATION_PROMPTS["Default"])
    prompt = prompt_template.format(input_value=input_value)

    log = logger.bind(model=settings.VERTEX_AI_MODEL_NAME, context=context, input_value_snippet=input_value[:50])
    log.info("Attempting vocabulary standardization via Gemini.")

    try:
        # Increment counter before the call
        await _increment_invocation_count("gemini", f"standardize_{context.lower()}")

        # Call Gemini API asynchronously
        response = await gemini_model.generate_content_async(
            [prompt],
            generation_config=generation_config or DEFAULT_GENERATION_CONFIG,
            safety_settings=safety_settings or DEFAULT_SAFETY_SETTINGS,
            stream=False, # We want the full response at once
        )

        # Process the response
        log.debug(f"Raw Gemini response: {response}")

        # Check for finish reason and safety ratings (important!)
        if not response.candidates:
             log.warning("Gemini response contained no candidates.")
             return None

        candidate = response.candidates[0] # Assuming only one candidate usually

        if candidate.finish_reason != FinishReason.STOP:
             log.warning(f"Gemini generation stopped prematurely. Reason: {candidate.finish_reason.name}")
             # Optionally check candidate.finish_message for details

        # Check safety ratings - if blocked, we get no content part
        if candidate.safety_ratings:
             for rating in candidate.safety_ratings:
                  # Check if rating.blocked exists and is True
                  if hasattr(rating, 'blocked') and rating.blocked:
                       log.warning(f"Gemini response blocked due to safety concerns. Category: {rating.category.name}, Probability: {rating.probability.name}")
                       return None # Or handle differently

        if not candidate.content or not candidate.content.parts:
            log.warning("Gemini response candidate has no content parts.")
            return None

        # Extract text, expecting only one part based on our simple prompt
        if len(candidate.content.parts) > 1:
             log.warning(f"Gemini response contained multiple parts ({len(candidate.content.parts)}), using only the first.")

        standardized_text = candidate.content.parts[0].text.strip()

        if not standardized_text:
             log.warning("Gemini returned an empty standardized term.")
             return None # Treat empty as failure

        log.info(f"Successfully standardized term: '{standardized_text}'")
        return standardized_text

    except google_api_exceptions.GoogleAPICallError as api_err:
        log.error(f"Vertex AI API call error during standardization: {api_err}", exc_info=True)
        return None
    except Exception as e:
        log.error(f"Unexpected error during Gemini vocabulary standardization: {e}", exc_info=True)
        return None
# --- End Vocabulary Standardization Logic ---
