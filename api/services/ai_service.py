"""AI integration service for Gemini."""

import json
import logging
import time
from typing import Optional, List, Dict
from google import genai

logger = logging.getLogger("patterns")


def build_ai_prompt(table: str, source_platform: str, source_project: str,
                   target_warehouse: Optional[str], time_data: List[Dict],
                   partition_stats: List[Dict], table_metadata: Optional[Dict] = None,
                   database: str = None, schema: str = None, period: str = 'weekly') -> str:
    """Build optimized prompt for Gemini AI with detailed context.
    
    Args:
        time_data: List of time-based stats (weekly or monthly)
        period: 'weekly' or 'monthly' to indicate the aggregation period
    """
    # Detect period type from data keys if not explicitly provided
    if time_data and not period:
        first_item = time_data[0]
        if 'month_start' in first_item:
            period = 'monthly'
        else:
            period = 'weekly'
    
    period_label = 'Month' if period == 'monthly' else 'Week'
    period_label_plural = 'Months' if period == 'monthly' else 'Weeks'
    period_label_lower = period_label.lower()
    period_label_plural_lower = period_label_plural.lower()
    
    total_periods = len(time_data)
    total_queries = sum(t["total_queries"] for t in time_data)
    total_star = sum(t["star_queries"] for t in time_data)
    
    recent_count = 2 if period == 'monthly' else 4
    recent_periods = [t for t in time_data[-recent_count:] if time_data]
    recent_queries = sum(t["total_queries"] for t in recent_periods) if recent_periods else 0
    
    if table_metadata:
        size_bytes = table_metadata.get('size_bytes')
        row_count = table_metadata.get('row_count', 'Unknown')
        
        if isinstance(size_bytes, (int, float)) and size_bytes is not None:
            size_str = f"{round(size_bytes / 1024 / 1024, 2)} MB"
        else:
            size_str = "Unknown"
        
        if isinstance(row_count, (int, float)):
            row_count_str = f"{int(row_count):,} rows"
        else:
            row_count_str = str(row_count)
        
        columns_raw = table_metadata.get('columns') or []
        # Handle both JSON string and already-parsed list/dict formats
        if isinstance(columns_raw, str):
            try:
                columns = json.loads(columns_raw) if columns_raw else []
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse columns JSON in build_ai_prompt: %s", e)
                columns = []
        else:
            columns = columns_raw if columns_raw else []
        
        col_list = ", ".join([f"{c['name']} ({c.get('type', 'unknown')})" for c in columns]) if columns else "Unknown"
        
        table_info = f"""📊 TABLE METADATA:
- Size: {size_str}
- Row Count: {row_count_str}
- Columns ({len(columns)} total): {col_list}
"""
    else:
        table_info = ""
    
    top_filters = []
    for stat in partition_stats[:5]:
        top_filters.append(f"  - {stat.get('column', 'unknown')}: {stat.get('filter_type', 'unknown')} ({stat.get('total_count', 0)} uses)")
    filter_summary = "\n".join(top_filters) if top_filters else "  - No significant filter patterns detected"
    
    return f"""You are a data warehouse optimization expert. Analyze query patterns and suggest practical optimizations for {target_warehouse if target_warehouse else "the target system"}.

KEY RULES:
- Prioritize recent {period_label_plural_lower} (last {recent_count}) over older data
- Consider sample size: <50 queries total or <10/{period_label_lower} = insufficient for strong conclusions
- Skip optimizations for small tables or marginal benefits
- Warn if SELECT * is common (>20% AND >50 total SELECT * queries)
- Focus on {target_warehouse}-specific features when target is specified
- NO SQL/DDL code - only conceptual recommendations

═══════════════════════════════════════════════════════════
DATA
═══════════════════════════════════════════════════════════

TARGET: {target_warehouse if target_warehouse else "Generic"}
TABLE: {f"{database}.{schema}.{table}" if database and schema else table}
Source: {source_platform} / {source_project}
{table_info}

USAGE SUMMARY:
- Period: {total_periods} {period_label_plural_lower}
- Queries: {total_queries} total, {total_star} SELECT * ({round(100 * total_star / total_queries, 1) if total_queries > 0 else 0}%)
  Sample: {"SUFFICIENT" if total_queries >= 50 else f"LOW ({total_queries} queries)"}
- Recent ({recent_count} {period_label_plural_lower}): {recent_queries} queries ({round(100 * recent_queries / total_queries, 1) if total_queries > 0 else 0}%)
- Avg: {round(total_queries / total_periods, 1) if total_periods > 0 else 0} queries/{period_label_lower}

{period_label_plural.upper()} PATTERNS (prioritize recent):
{json.dumps(time_data, indent=2)}

TOP FILTERS (partition/cluster candidates):
{filter_summary}

FULL FILTER STATS:
{json.dumps(partition_stats, indent=2)}

OUTPUT FORMAT:

**Summary**
System: {target_warehouse if target_warehouse else "Generic"}
Table: {f"{database}.{schema}.{table}" if database and schema else table}
Size: [from TABLE METADATA above]
Query Focus: [main patterns from recent {period_label_plural_lower}]

**Recommendations**
| Area | Suggestion | Why |
|------|-----------|-----|
| [Type] | [Recommendation] | [Reason] |

Relevant areas: Clustering, Partitioning, Sorting, Query Patterns, Caching, Materialized Views, Statistics, Storage Format

Rules:
- Only suggest if: supported by system, matches patterns, significant benefit, sufficient sample size
- State confidence based on sample size
- No SQL/DDL code
{f"- Use {target_warehouse}-specific features" if target_warehouse else ""}"""


def query_gemini_ai(prompt: str, target_warehouse: Optional[str], api_key: str, model_name: str) -> str:
    """Query Gemini AI with optimized configuration and retry logic."""
    if not api_key:
        raise ValueError("Gemini API key not configured in config.json")
    
    # System instruction emphasizes expertise and prioritizing recent data
    system_instruction = "You are a data warehouse optimization expert specializing in query performance, partitioning, clustering, and cost optimization. You provide practical, actionable recommendations based on real query patterns."
    if target_warehouse:
        system_instruction += f" You have deep expertise in {target_warehouse}, including its syntax, specific features, best practices, and cost optimization strategies."
    
    # Retry logic with exponential backoff
    max_retries = 3
    base_delay = 2  # seconds
    
    for attempt in range(max_retries):
        try:
            logger.info("Attempting Gemini API call (attempt %d/%d, model: %s)", attempt + 1, max_retries, model_name)
            
            client = genai.Client(api_key=api_key)
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config={
                    "temperature": 0.3,
                    "max_output_tokens": 8192,  # Increased from 2048 to handle longer responses
                }
            )
            
            if response:
                # Log response structure for debugging
                logger.debug("Response type: %s", type(response))
                logger.debug("Response attributes: %s", dir(response))
                
                if hasattr(response, 'text') and response.text:
                    return response.text
                
                # Check for candidates (alternative response structure)
                if hasattr(response, 'candidates') and response.candidates:
                    logger.debug("Response has candidates: %d", len(response.candidates))
                    candidate = response.candidates[0]
                    
                    # Check for finish_reason first to understand why response might be empty
                    if hasattr(candidate, 'finish_reason'):
                        finish_reason = str(candidate.finish_reason)
                        if 'MAX_TOKENS' in finish_reason:
                            logger.error("Gemini finish_reason: %s - Output was truncated. Consider reducing input or increasing max_output_tokens.", finish_reason)
                        else:
                            logger.error("Gemini finish_reason: %s", finish_reason)
                    
                    if hasattr(candidate, 'content') and candidate.content:
                        parts = getattr(candidate.content, 'parts', None)
                        if parts and len(parts) > 0:
                            text = ''.join(part.text for part in parts if hasattr(part, 'text'))
                            if text:
                                return text
                        elif parts is None:
                            logger.error("Content parts is None - response was likely empty or blocked")
                
                # Check if blocked by safety filters
                if hasattr(response, 'prompt_feedback'):
                    logger.error("Prompt feedback: %s", response.prompt_feedback)
                
                logger.error("Gemini API returned empty or invalid response. Response: %s", str(response)[:500])
                raise ValueError("Gemini API returned empty response")
            else:
                logger.error("Gemini API returned None response")
                raise ValueError("Gemini API returned None response")
            
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            is_overload = "503" in error_msg or "overloaded" in error_msg.lower() or "UNAVAILABLE" in error_msg
            is_empty_response = "empty response" in error_msg.lower() or "None response" in error_msg.lower()
            is_last_attempt = attempt == max_retries - 1
            
            # Log detailed error info
            logger.debug("Exception type: %s, message: %s", error_type, error_msg)
            
            # Retry on overload or empty response (but not on last attempt)
            if (is_overload or is_empty_response) and not is_last_attempt:
                delay = base_delay * (2 ** attempt)  # Exponential backoff: 2s, 4s, 8s
                reason = "overloaded" if is_overload else "empty response"
                logger.warning("Gemini API %s (attempt %d/%d). Retrying in %ds...", reason, attempt + 1, max_retries, delay)
                time.sleep(delay)
            else:
                # Not a retryable error or last attempt failed
                logger.error("Gemini API error [%s]: %s", error_type, error_msg)
                if is_last_attempt and is_empty_response:
                    logger.error("Persistent empty responses may indicate: 1) Model overload, 2) Safety filter blocking, 3) Prompt too large, 4) API quota exceeded")
                raise ValueError(f"Gemini API failed after {attempt + 1} attempts: {error_msg}")
    
    raise ValueError("Gemini API failed after maximum retries")

