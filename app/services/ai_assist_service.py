# app/services/ai_assist_service.py
import logging
import json
from typing import Optional

# --- OpenAI Client ---
try:
    from openai import OpenAI, AsyncOpenAI, APIError, RateLimitError, APIConnectionError
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    # Define dummy classes if OpenAI library is not installed
    class OpenAI: pass
    class AsyncOpenAI: pass
    class APIError(Exception): pass
    class RateLimitError(APIError): pass
    class APIConnectionError(APIError): pass
    logging.getLogger(__name__).warning("OpenAI library not found. AI Assist features will be unavailable.")
# --- End OpenAI Client ---

from app.core.config import settings
from app.schemas.ai_assist import RuleGenRequest, RuleGenResponse, RuleGenSuggestion
from app.schemas.rule import MatchCriterion, AssociationMatchCriterion, TagModification # Import rule schemas for context/validation

logger = logging.getLogger(__name__)

# --- Initialize OpenAI Client ---
openai_client = None
if OPENAI_AVAILABLE and settings.OPENAI_API_KEY:
    try:
        # Use SecretStr's get_secret_value() method
        api_key = settings.OPENAI_API_KEY.get_secret_value()
        openai_client = AsyncOpenAI(api_key=api_key)
        logger.info("OpenAI client initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {e}", exc_info=True)
        openai_client = None # Ensure client is None if initialization fails
elif OPENAI_AVAILABLE and not settings.OPENAI_API_KEY:
     logger.warning("OpenAI library is installed, but OPENAI_API_KEY is not configured in settings. AI Assist features disabled.")
# --- End Initialize OpenAI Client ---


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
    """
    if not OPENAI_AVAILABLE or not openai_client:
        logger.error("OpenAI client is not available or not configured.")
        return RuleGenResponse(error="AI service is not available or not configured.")

    logger.info(f"Generating rule suggestion for prompt: '{request.prompt[:50]}...'")

    try:
        # Use the async client
        completion = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo", # Or a newer model like gpt-4 if available/needed
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": request.prompt}
            ],
            temperature=0.2, # Lower temperature for more deterministic JSON output
            max_tokens=500, # Limit response size
            response_format={"type": "json_object"}, # Request JSON output if model supports it
        )

        ai_response_content = completion.choices[0].message.content
        logger.debug(f"Raw AI response content: {ai_response_content}")

        if not ai_response_content:
            logger.warning("AI response content was empty.")
            return RuleGenResponse(error="AI returned an empty response.")

        # Parse the JSON response from the AI
        try:
            suggestion_dict = json.loads(ai_response_content)
            if not isinstance(suggestion_dict, dict):
                raise ValueError("AI response was not a JSON object.")

            if "error" in suggestion_dict:
                logger.warning(f"AI returned an error message: {suggestion_dict['error']}")
                return RuleGenResponse(error=suggestion_dict["error"])

            # Validate the structure against our Pydantic suggestion schema
            suggestion = RuleGenSuggestion(
                match_criteria=suggestion_dict.get("match_criteria"),
                association_criteria=suggestion_dict.get("association_criteria"),
                tag_modifications=suggestion_dict.get("tag_modifications"),
                applicable_sources=suggestion_dict.get("applicable_sources"),
            )

            # TODO: Add more rigorous validation here if needed,
            # potentially trying to parse match_criteria/tag_modifications
            # using the actual Pydantic rule schemas for deeper validation.
            # This can be complex due to the union types.

            logger.info("Successfully generated and parsed rule suggestion.")
            return RuleGenResponse(
                suggestion=suggestion,
                explanation="Suggestion generated based on your prompt.", # Basic explanation
                # Confidence score isn't directly available from standard chat completion
            )

        except json.JSONDecodeError as json_err:
            logger.error(f"Failed to decode JSON response from AI: {json_err}. Response: {ai_response_content[:500]}")
            return RuleGenResponse(error=f"AI returned invalid JSON: {json_err}")
        except ValueError as val_err:
            logger.error(f"Failed to validate AI response structure: {val_err}. Response: {ai_response_content[:500]}")
            return RuleGenResponse(error=f"AI response structure invalid: {val_err}")
        except Exception as parse_err:
            logger.error(f"Unexpected error parsing AI response: {parse_err}", exc_info=True)
            return RuleGenResponse(error=f"Error parsing AI response: {parse_err}")

    except APIConnectionError as e:
        logger.error(f"OpenAI API connection error: {e}")
        return RuleGenResponse(error=f"Could not connect to AI service: {e}")
    except RateLimitError as e:
        logger.error(f"OpenAI API rate limit exceeded: {e}")
        return RuleGenResponse(error=f"AI service rate limit exceeded. Please try again later. {e}")
    except APIError as e:
        logger.error(f"OpenAI API error: Status={e.status_code}, Message={e.message}", exc_info=True)
        return RuleGenResponse(error=f"AI service returned an error: {e.message} (Status: {e.status_code})")
    except Exception as e:
        logger.error(f"Unexpected error during AI rule generation: {e}", exc_info=True)
        return RuleGenResponse(error=f"An unexpected error occurred: {e}")
