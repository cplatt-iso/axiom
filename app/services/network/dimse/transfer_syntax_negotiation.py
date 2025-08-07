# app/services/network/dimse/transfer_syntax_negotiation.py
"""
Robust transfer syntax negotiation utilities for DICOM SCU operations.
Provides fallback strategies and context validation for improved PACS compatibility.
"""

import structlog
from typing import List, Dict, Optional, Any, Tuple, Set
from pydicom.dataset import Dataset
from pydicom.uid import UID
from pynetdicom.presentation import PresentationContext, build_context
from pynetdicom.association import Association
from pynetdicom import _config
from pydicom import uid

logger = structlog.get_logger(__name__)

# Define transfer syntax priority lists (most compatible first)
UNIVERSAL_TRANSFER_SYNTAXES = [
    uid.ImplicitVRLittleEndian,  # 1.2.840.10008.1.2 - Most compatible
]

CONSERVATIVE_TRANSFER_SYNTAXES = [
    uid.ImplicitVRLittleEndian,     # 1.2.840.10008.1.2 - Most compatible
    uid.ExplicitVRLittleEndian,    # 1.2.840.10008.1.2.1 - Very common
]

STANDARD_TRANSFER_SYNTAXES = [
    uid.ImplicitVRLittleEndian,     # 1.2.840.10008.1.2 - Most compatible
    uid.ExplicitVRLittleEndian,    # 1.2.840.10008.1.2.1 - Very common
    uid.ExplicitVRBigEndian,       # 1.2.840.10008.1.2.2 - Less common but standard
]

COMPRESSION_TRANSFER_SYNTAXES = [
    uid.ImplicitVRLittleEndian,     # Always include fallback
    uid.ExplicitVRLittleEndian,
    uid.RLELossless,               # 1.2.840.10008.1.2.5 - Lossless compression
    uid.JPEG2000Lossless,          # 1.2.840.10008.1.2.4.90 - Modern lossless
]

# Full list including compressed formats (use with caution)
EXTENDED_TRANSFER_SYNTAXES = [
    uid.ImplicitVRLittleEndian,     # Always first - fallback
    uid.ExplicitVRLittleEndian,    # Second priority
    uid.ExplicitVRBigEndian,
    uid.RLELossless,
    uid.JPEG2000Lossless,
    uid.JPEG2000,                  # Lossy but widely supported
    uid.JPEGBaseline8Bit,          # 1.2.840.10008.1.2.4.50
    uid.JPEGExtended12Bit,         # 1.2.840.10008.1.2.4.51
]

class TransferSyntaxStrategy:
    """Strategy for transfer syntax negotiation with fallback options."""
    
    UNIVERSAL = "universal"
    CONSERVATIVE = "conservative" 
    STANDARD = "standard"
    COMPRESSION = "compression"
    EXTENDED = "extended"

    STRATEGY_MAP = {
        UNIVERSAL: UNIVERSAL_TRANSFER_SYNTAXES,
        CONSERVATIVE: CONSERVATIVE_TRANSFER_SYNTAXES,
        STANDARD: STANDARD_TRANSFER_SYNTAXES,
        COMPRESSION: COMPRESSION_TRANSFER_SYNTAXES,
        EXTENDED: EXTENDED_TRANSFER_SYNTAXES,
    }

def get_transfer_syntaxes_for_strategy(strategy: str) -> List[str]:
    """Get transfer syntaxes for a given strategy."""
    return TransferSyntaxStrategy.STRATEGY_MAP.get(strategy, CONSERVATIVE_TRANSFER_SYNTAXES).copy()

def detect_dataset_transfer_syntax(dataset: Dataset) -> Optional[str]:
    """
    Detect the current transfer syntax of a dataset.
    Returns the transfer syntax UID or None if not determinable.
    """
    try:
        if hasattr(dataset, 'file_meta') and dataset.file_meta:
            ts_uid = getattr(dataset.file_meta, 'TransferSyntaxUID', None)
            if ts_uid:
                return str(ts_uid)
        
        # Fallback: check if dataset has implicit/explicit characteristics
        # This is heuristic and may not be 100% accurate
        if hasattr(dataset, 'is_implicit_VR'):
            if dataset.is_implicit_VR:
                return uid.ImplicitVRLittleEndian
            else:
                return uid.ExplicitVRLittleEndian
                
    except Exception as e:
        logger.warning("Could not detect dataset transfer syntax", error=str(e))
    
    return None

def create_presentation_contexts_with_fallback(
    sop_class_uid: str, 
    strategies: List[str] = None,
    max_contexts_per_strategy: int = 3
) -> List[PresentationContext]:
    """
    Create presentation contexts with multiple transfer syntax strategies.
    
    Args:
        sop_class_uid: The SOP Class UID to negotiate
        strategies: List of strategies to try (in order)
        max_contexts_per_strategy: Maximum contexts per strategy to avoid hitting limits
        
    Returns:
        List of presentation contexts to propose
    """
    if strategies is None:
        strategies = [
            TransferSyntaxStrategy.CONSERVATIVE,
            TransferSyntaxStrategy.UNIVERSAL,
            TransferSyntaxStrategy.STANDARD
        ]
    
    contexts = []
    context_id = 1
    
    log = logger.bind(sop_class_uid=sop_class_uid, strategies=strategies)
    
    for strategy in strategies:
        transfer_syntaxes = get_transfer_syntaxes_for_strategy(strategy)
        
        # Limit transfer syntaxes per strategy to avoid too many contexts
        limited_syntaxes = transfer_syntaxes[:max_contexts_per_strategy]
        
        for ts in limited_syntaxes:
            if context_id > 127:  # pynetdicom limit
                log.warning("Reached maximum presentation context limit", max_contexts=127)
                break
                
            # Create PresentationContext using build_context helper
            try:
                context = build_context(sop_class_uid, [ts])
                context.context_id = context_id
                contexts.append(context)
                
                log.debug("Added presentation context", 
                         context_id=context_id, 
                         strategy=strategy,
                         transfer_syntax=ts)
                         
                context_id += 2  # Odd numbers only for SCU
                
            except Exception as e:
                log.warning("Failed to create presentation context", 
                           context_id=context_id,
                           strategy=strategy,
                           transfer_syntax=ts,
                           error=str(e))
                context_id += 2
                continue
            
        if context_id > 127:
            break
    
    log.debug("Created presentation contexts", 
              context_count=len(contexts),
              strategies_used=strategies)
    
    return contexts

def analyze_accepted_contexts(association: Association) -> Dict[str, Any]:
    """
    Analyze which presentation contexts were accepted by the peer.
    
    Returns:
        Dictionary with analysis results including accepted contexts,
        transfer syntaxes, and recommendations
    """
    if not association or not association.is_established:
        return {"error": "Association not established"}
    
    accepted_contexts = []
    rejected_contexts = []
    
    for context in association.accepted_contexts:
        try:
            sop_name = UID(context.abstract_syntax).name
        except:
            sop_name = "Unknown SOP Class"
        
        # Handle transfer_syntax which might be a list or string
        ctx_ts = context.transfer_syntax
        if isinstance(ctx_ts, list):
            ts_for_display = ctx_ts[0] if ctx_ts else "Unknown"
        else:
            ts_for_display = ctx_ts
            
        accepted_contexts.append({
            "context_id": context.context_id,
            "abstract_syntax": context.abstract_syntax,
            "sop_name": sop_name,
            "transfer_syntax": ts_for_display,
            "transfer_syntax_name": _get_transfer_syntax_name(ts_for_display)
        })
    
    # Try to get rejected contexts if the attribute exists
    # Note: pynetdicom may not expose requested_contexts in all versions
    if hasattr(association, 'requested_contexts') and association.requested_contexts:
        for context in association.requested_contexts:
            if not any(ac.context_id == context.context_id for ac in association.accepted_contexts):
                ctx_ts = context.transfer_syntax
                if isinstance(ctx_ts, list):
                    ts_for_display = ctx_ts[0] if ctx_ts else "N/A"
                else:
                    ts_for_display = ctx_ts if ctx_ts else "N/A"
                    
                rejected_contexts.append({
                    "context_id": context.context_id,
                    "abstract_syntax": context.abstract_syntax,
                    "transfer_syntax": ts_for_display
                })
    else:
        # If we can't access requested contexts, we can't determine rejections
        logger.debug("Association object does not have requested_contexts attribute or it's empty")
    
    analysis = {
        "accepted_count": len(accepted_contexts),
        "rejected_count": len(rejected_contexts),
        "accepted_contexts": accepted_contexts,
        "rejected_contexts": rejected_contexts,
        "peer_ae_title": association.acceptor.ae_title,
        "peer_supports_compression": any(
            ctx["transfer_syntax"] in [uid.RLELossless, uid.JPEG2000Lossless, uid.JPEG2000]
            for ctx in accepted_contexts
        )
    }
    
    return analysis

def _get_transfer_syntax_name(ts_uid: str) -> str:
    """Get human-readable name for transfer syntax UID."""
    try:
        return UID(ts_uid).name
    except:
        # Manual mapping for common ones
        name_map = {
            uid.ImplicitVRLittleEndian: "Implicit VR Little Endian",
            uid.ExplicitVRLittleEndian: "Explicit VR Little Endian", 
            uid.ExplicitVRBigEndian: "Explicit VR Big Endian",
            uid.RLELossless: "RLE Lossless",
            uid.JPEG2000Lossless: "JPEG 2000 Lossless",
            uid.JPEG2000: "JPEG 2000",
        }
        return name_map.get(ts_uid, f"Unknown ({ts_uid})")

def find_compatible_transfer_syntax(
    dataset: Dataset, 
    accepted_contexts: List[PresentationContext],
    sop_class_uid: str
) -> Optional[Tuple[PresentationContext, str]]:
    """
    Find the best compatible presentation context and transfer syntax for a dataset.
    
    Args:
        dataset: The DICOM dataset to send
        accepted_contexts: Contexts accepted by the peer
        sop_class_uid: The SOP class UID of the dataset
        
    Returns:
        Tuple of (PresentationContext, reason) or None if no compatible context found
    """
    log = logger.bind(sop_class_uid=sop_class_uid)
    
    # Filter contexts for this SOP class
    compatible_contexts = [
        ctx for ctx in accepted_contexts 
        if ctx.abstract_syntax == sop_class_uid
    ]
    
    if not compatible_contexts:
        log.error("No accepted presentation contexts for SOP class")
        return None
    
    # Detect dataset's current transfer syntax
    dataset_ts = detect_dataset_transfer_syntax(dataset)
    log.debug("Dataset transfer syntax detected", dataset_ts=dataset_ts)
    
    # Priority 1: Exact match with dataset's transfer syntax
    if dataset_ts:
        for ctx in compatible_contexts:
            # transfer_syntax is a list, so check if dataset_ts is in it
            ctx_transfer_syntaxes = ctx.transfer_syntax if isinstance(ctx.transfer_syntax, list) else [ctx.transfer_syntax]
            if dataset_ts in ctx_transfer_syntaxes:
                log.info("Found exact transfer syntax match", 
                        context_id=ctx.context_id,
                        transfer_syntax=_get_transfer_syntax_name(dataset_ts))
                return ctx, "Exact transfer syntax match"
    
    # Priority 2: Implicit VR Little Endian (most compatible)
    for ctx in compatible_contexts:
        ctx_transfer_syntaxes = ctx.transfer_syntax if isinstance(ctx.transfer_syntax, list) else [ctx.transfer_syntax]
        if uid.ImplicitVRLittleEndian in ctx_transfer_syntaxes:
            log.info("Using Implicit VR Little Endian fallback",
                    context_id=ctx.context_id)
            return ctx, "Universal compatibility fallback"
    
    # Priority 3: Explicit VR Little Endian
    for ctx in compatible_contexts:
        ctx_transfer_syntaxes = ctx.transfer_syntax if isinstance(ctx.transfer_syntax, list) else [ctx.transfer_syntax]
        if uid.ExplicitVRLittleEndian in ctx_transfer_syntaxes:
            log.info("Using Explicit VR Little Endian fallback",
                    context_id=ctx.context_id)
            return ctx, "Common fallback"
    
    # Priority 4: Any available context
    first_ctx = compatible_contexts[0]
    first_ts = first_ctx.transfer_syntax[0] if isinstance(first_ctx.transfer_syntax, list) else first_ctx.transfer_syntax
    log.warning("Using first available context as last resort",
               context_id=first_ctx.context_id,
               transfer_syntax=_get_transfer_syntax_name(first_ts))
    return first_ctx, "Last resort - first available context"

def validate_negotiation_success(
    association: Association,
    required_sop_classes: List[str]
) -> Dict[str, Any]:
    """
    Validate that all required SOP classes were successfully negotiated.
    
    Args:
        association: The established association
        required_sop_classes: List of SOP class UIDs that must be supported
        
    Returns:
        Dictionary with validation results
    """
    if not association or not association.is_established:
        return {
            "success": False,
            "error": "Association not established",
            "missing_sop_classes": required_sop_classes
        }
    
    accepted_sop_classes = [
        ctx.abstract_syntax for ctx in association.accepted_contexts
    ]
    
    missing_sop_classes = [
        sop for sop in required_sop_classes 
        if sop not in accepted_sop_classes
    ]
    
    validation = {
        "success": len(missing_sop_classes) == 0,
        "accepted_sop_classes": accepted_sop_classes,
        "missing_sop_classes": missing_sop_classes,
        "total_accepted_contexts": len(association.accepted_contexts),
        "peer_ae_title": association.acceptor.ae_title
    }
    
    if validation["success"]:
        logger.info("Presentation context negotiation successful",
                   accepted_contexts=validation["total_accepted_contexts"],
                   peer_ae=validation["peer_ae_title"])
    else:
        logger.error("Presentation context negotiation failed",
                    missing_sop_classes=missing_sop_classes,
                    peer_ae=validation["peer_ae_title"])
    
    return validation

def create_optimized_contexts_for_sop_class(
    sop_class_uid: str,
    preferred_transfer_syntaxes: Optional[List[str]] = None,
    max_contexts: int = 5
) -> List[PresentationContext]:
    """
    Create optimized presentation contexts for a single SOP class.
    
    Args:
        sop_class_uid: The SOP class UID
        preferred_transfer_syntaxes: Preferred transfer syntaxes in priority order
        max_contexts: Maximum number of contexts to create
        
    Returns:
        List of optimized presentation contexts
    """
    if preferred_transfer_syntaxes is None:
        # Use conservative approach by default
        preferred_transfer_syntaxes = CONSERVATIVE_TRANSFER_SYNTAXES
    
    contexts = []
    context_id = 1
    
    log = logger.bind(sop_class_uid=sop_class_uid)
    
    # Limit to avoid too many contexts
    limited_syntaxes = preferred_transfer_syntaxes[:max_contexts]
    
    for ts in limited_syntaxes:
        if context_id > 127:
            log.warning("Reached maximum context ID limit")
            break
            
        try:
            context = build_context(sop_class_uid, [ts])
            context.context_id = context_id
            contexts.append(context)
            
            log.debug("Created presentation context",
                     context_id=context_id,
                     transfer_syntax=_get_transfer_syntax_name(ts))
                     
            context_id += 2  # Context IDs must be odd
            
        except Exception as e:
            log.warning("Failed to create context",
                       context_id=context_id,
                       transfer_syntax=ts,
                       error=str(e))
            context_id += 2
    
    return contexts

def get_transfer_syntax_recommendation(
    failed_contexts: List[str],
    peer_ae_title: str
) -> Dict[str, Any]:
    """
    Provide recommendations for transfer syntax negotiation based on failures.
    """
    recommendations = {
        "peer_ae_title": peer_ae_title,
        "failed_transfer_syntaxes": failed_contexts,
        "suggestions": []
    }
    
    # Analyze failure patterns
    if uid.ExplicitVRLittleEndian in failed_contexts:
        recommendations["suggestions"].append({
            "strategy": "Use Implicit VR Little Endian only",
            "rationale": "Peer may not support Explicit VR formats",
            "recommended_syntaxes": [uid.ImplicitVRLittleEndian]
        })
    
    if len(failed_contexts) > 3:
        recommendations["suggestions"].append({
            "strategy": "Use conservative strategy",
            "rationale": "Peer appears to have limited transfer syntax support",
            "recommended_syntaxes": CONSERVATIVE_TRANSFER_SYNTAXES
        })
    
    if not recommendations["suggestions"]:
        recommendations["suggestions"].append({
            "strategy": "Use universal strategy",
            "rationale": "Start with most compatible transfer syntax",
            "recommended_syntaxes": UNIVERSAL_TRANSFER_SYNTAXES
        })
    
    return recommendations

def handle_cstore_context_error(
    sop_class_uid: str,
    required_transfer_syntax: str,
    association: Association
) -> Dict[str, Any]:
    """
    Handle the specific case where C-STORE fails due to missing presentation context.
    
    Args:
        sop_class_uid: The SOP class that failed
        required_transfer_syntax: The transfer syntax that was required
        association: The current association
        
    Returns:
        Dictionary with error analysis and recommendations
    """
    log = logger.bind(
        sop_class_uid=sop_class_uid,
        required_transfer_syntax=required_transfer_syntax
    )
    
    # Analyze what contexts were actually accepted
    accepted_contexts = []
    for ctx in association.accepted_contexts:
        if ctx.abstract_syntax == sop_class_uid:
            # Handle transfer_syntax which might be a list or string
            ctx_ts = ctx.transfer_syntax
            if isinstance(ctx_ts, list):
                ts_for_display = ctx_ts[0] if ctx_ts else "Unknown"
            else:
                ts_for_display = ctx_ts
                
            accepted_contexts.append({
                "context_id": ctx.context_id,
                "transfer_syntax": ts_for_display,
                "transfer_syntax_name": _get_transfer_syntax_name(ts_for_display)
            })
    
    error_analysis = {
        "sop_class_uid": sop_class_uid,
        "sop_class_name": _get_sop_class_name(sop_class_uid),
        "required_transfer_syntax": required_transfer_syntax,
        "required_transfer_syntax_name": _get_transfer_syntax_name(required_transfer_syntax),
        "accepted_contexts_for_sop": accepted_contexts,
        "total_accepted_contexts": len(association.accepted_contexts),
        "recommendations": []
    }
    
    if not accepted_contexts:
        # No contexts accepted for this SOP class at all
        error_analysis["recommendations"].append({
            "type": "sop_class_not_supported",
            "message": f"Peer does not support {error_analysis['sop_class_name']}",
            "suggested_action": "Check if peer supports this SOP class or use different storage destination"
        })
    else:
        # SOP class is supported but not with the required transfer syntax
        available_syntaxes = [ctx["transfer_syntax"] for ctx in accepted_contexts]
        error_analysis["recommendations"].append({
            "type": "transfer_syntax_mismatch", 
            "message": f"Dataset requires {error_analysis['required_transfer_syntax_name']} but peer only accepts: {', '.join([_get_transfer_syntax_name(ts) for ts in available_syntaxes])}",
            "suggested_action": "Convert dataset to a supported transfer syntax before sending",
            "supported_transfer_syntaxes": available_syntaxes
        })
    
    log.error("C-STORE presentation context error", **error_analysis)
    return error_analysis

def _get_sop_class_name(sop_class_uid: str) -> str:
    """Get human-readable name for SOP class UID."""
    try:
        return UID(sop_class_uid).name
    except:
        # Manual mapping for common SOP classes
        name_map = {
            "1.2.840.10008.5.1.4.1.1.2": "CT Image Storage",
            "1.2.840.10008.5.1.4.1.1.1": "Computed Radiography Image Storage", 
            "1.2.840.10008.5.1.4.1.1.4": "MR Image Storage",
            "1.2.840.10008.5.1.4.1.1.6.1": "Ultrasound Image Storage",
            "1.2.840.10008.5.1.4.1.1.20": "Nuclear Medicine Image Storage",
            "1.2.840.10008.5.1.4.1.1.1.1": "Digital X-Ray Image Storage - For Presentation",
            "1.2.840.10008.5.1.4.1.1.1.1.1": "Digital X-Ray Image Storage - For Processing",
        }
        return name_map.get(sop_class_uid, f"Unknown SOP Class ({sop_class_uid})")

def suggest_fallback_strategy(
    failed_strategy: str,
    error_details: Optional[str] = None
) -> str:
    """
    Suggest a fallback strategy based on the failed strategy and error details.
    """
    strategy_fallbacks = {
        TransferSyntaxStrategy.EXTENDED: TransferSyntaxStrategy.COMPRESSION,
        TransferSyntaxStrategy.COMPRESSION: TransferSyntaxStrategy.STANDARD,
        TransferSyntaxStrategy.STANDARD: TransferSyntaxStrategy.CONSERVATIVE,
        TransferSyntaxStrategy.CONSERVATIVE: TransferSyntaxStrategy.UNIVERSAL,
        TransferSyntaxStrategy.UNIVERSAL: TransferSyntaxStrategy.UNIVERSAL  # Can't fallback further
    }
    
    suggested = strategy_fallbacks.get(failed_strategy, TransferSyntaxStrategy.CONSERVATIVE)
    
    logger.info("Suggesting fallback strategy",
               failed_strategy=failed_strategy,
               suggested_strategy=suggested,
               error_details=error_details)
    
    return suggested
