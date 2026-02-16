"""
AI Assistant for BVPro Web GUI.
Wraps the Anthropic API with tool_use for querying InfluxDB data,
reading profiles, and (for authorized users) changing settings.
"""

import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

import anthropic

from ai_tools import TOOL_DEFINITIONS, TOOL_FUNCTIONS

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')

# Chat history file
CHAT_HISTORY_FILE = os.path.join(os.path.dirname(__file__), 'chat_history.json')

# Rate limiting: per-user message counts
_rate_limits = {}  # {username: {'count': int, 'reset_at': float}}
RATE_LIMIT_PER_HOUR = int(os.environ.get('AI_RATE_LIMIT_PER_USER', '30'))


def _check_rate_limit(username: str) -> bool:
    """Return True if under rate limit, False if exceeded."""
    now = time.time()
    entry = _rate_limits.get(username)
    if not entry or now > entry['reset_at']:
        _rate_limits[username] = {'count': 1, 'reset_at': now + 3600}
        return True
    if entry['count'] >= RATE_LIMIT_PER_HOUR:
        return False
    entry['count'] += 1
    return True


def _build_system_prompt(user_context: dict) -> str:
    """Build the system prompt with user context."""
    role = user_context.get('role', 'viewer')
    houses = user_context.get('accessible_houses', [])
    buildings = user_context.get('accessible_buildings', [])
    username = user_context.get('username', '')

    # Build entity list
    entity_lines = []
    for h in houses:
        name = h.get('friendly_name', h.get('id', ''))
        hid = h.get('id', '')
        entity_lines.append(f"  - House: {name} (ID: {hid})")
    for b in buildings:
        name = b.get('friendly_name', b.get('id', ''))
        bid = b.get('id', '')
        entity_lines.append(f"  - Building: {name} (ID: {bid})")
    entities = '\n'.join(entity_lines) if entity_lines else '  (none)'

    today = datetime.now(SWEDISH_TZ).strftime('%Y-%m-%d')

    # Current page context
    viewing = user_context.get('viewing_entity')
    if viewing:
        if viewing.get('house_id'):
            viewing_line = f"- Currently viewing: House \"{viewing.get('name', viewing['house_id'])}\" (ID: {viewing['house_id']})"
        elif viewing.get('building_id'):
            viewing_line = f"- Currently viewing: Building \"{viewing.get('name', viewing['building_id'])}\" (ID: {viewing['building_id']})"
        else:
            viewing_line = ''
    else:
        viewing_line = '- Currently viewing: Dashboard (no specific entity selected)'

    role_instructions = {
        'admin': 'You have full access. You can read all data and change settings for any entity.',
        'user': 'You can view data for your assigned entities and change settings for your own house.',
        'viewer': 'You have read-only access. You cannot change any settings.',
    }

    return f"""You are the heating system AI assistant. You help users understand their heating data, energy consumption, and system status.

## System context
- BVPro monitors residential houses (via HomeSide API) and commercial buildings (via Arrigo BMS)
- Data is stored in InfluxDB: temperatures, energy consumption, heating/DHW separation
- Energy separation splits total energy into heating vs domestic hot water (DHW) using a calibrated k-value
- The energy pipeline runs daily at 08:00: imports meter data from Dropbox, then runs separation
- Today's date: {today}

## User context
- Username: {username}
- Role: {role}
- {role_instructions.get(role, 'Read-only access.')}
{viewing_line}
- Accessible entities:
{entities}

## Instructions
- Answer in Swedish by default, unless the user writes in English
- Be concise and helpful. Use data from tools, never fabricate numbers
- When referring to entities, use their friendly name (not the ID) unless the user asks for IDs
- For energy data, always mention the time period and units (kWh)
- When a user asks about missing data, use diagnose_missing_separation or check_data_gaps
- If you cannot resolve an issue, offer to create a support ticket
- For date ranges, use Swedish time (CET/CEST)
- Round numbers to 1 decimal place
- If the user asks about an entity they don't have access to, say so politely"""


def _check_tool_access(tool_def: dict, params: dict, user_context: dict) -> Optional[str]:
    """
    Check if the user has permission to call this tool.
    Returns None if allowed, or an error message if denied.
    """
    access = tool_def.get('requires_access', 'none')
    role = user_context.get('role', 'viewer')
    houses = {h['id'] for h in user_context.get('accessible_houses', [])}
    buildings = {b['id'] for b in user_context.get('accessible_buildings', [])}

    if access == 'none':
        return None

    if access == 'admin':
        if role != 'admin':
            return 'This action requires admin access.'
        return None

    # Entity access checks
    entity_id = params.get('entity_id') or params.get('house_id') or params.get('building_id')
    entity_type = params.get('entity_type', 'house')

    if access in ('house', 'house_edit'):
        hid = params.get('house_id') or params.get('entity_id')
        if hid and hid not in houses:
            return f'You do not have access to house {hid}.'
        if access == 'house_edit':
            if role == 'viewer':
                return 'Viewers cannot change settings.'
            if role == 'user' and not user_context.get('can_edit_fn', lambda *a: False)(hid):
                return f'You do not have edit permission for {hid}.'
        return None

    if access == 'building':
        bid = params.get('building_id') or params.get('entity_id')
        if bid and bid not in buildings:
            return f'You do not have access to building {bid}.'
        return None

    if access == 'entity':
        if entity_type == 'building':
            if entity_id and entity_id not in buildings:
                return f'You do not have access to building {entity_id}.'
        else:
            if entity_id and entity_id not in houses:
                return f'You do not have access to house {entity_id}.'
        return None

    return None


def _get_tool_def_by_name(name: str) -> Optional[dict]:
    for td in TOOL_DEFINITIONS:
        if td['name'] == name:
            return td
    return None


class BVProAssistant:
    """Anthropic-powered assistant with tool_use for BVPro data."""

    def __init__(self, api_key: str, model: str = None):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model or os.environ.get('AI_MODEL_DEFAULT', 'claude-haiku-4-5-20251001')
        self.max_tokens = int(os.environ.get('AI_MAX_TOKENS', '1024'))

    def _get_anthropic_tools(self, user_context: dict) -> list:
        """Build Anthropic tool definitions, filtered by role."""
        role = user_context.get('role', 'viewer')
        tools = []
        for td in TOOL_DEFINITIONS:
            # Skip admin tools for non-admins
            if td.get('requires_access') == 'admin' and role != 'admin':
                continue
            # Skip write tools for viewers
            if td.get('write') and role == 'viewer':
                continue
            tools.append({
                'name': td['name'],
                'description': td['description'],
                'input_schema': td['input_schema'],
            })
        return tools

    def chat(self, messages: list, user_context: dict) -> str:
        """
        Send a conversation to Claude with tools.

        Args:
            messages: List of {'role': 'user'|'assistant', 'content': str}
            user_context: Dict with role, accessible_houses, accessible_buildings, etc.

        Returns:
            Assistant's text response.
        """
        system_prompt = _build_system_prompt(user_context)
        tools = self._get_anthropic_tools(user_context)

        # Convert our message format to Anthropic format
        api_messages = []
        for msg in messages:
            api_messages.append({
                'role': msg['role'],
                'content': msg['content'],
            })

        # Tool use loop (max 5 rounds)
        for _ in range(5):
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system_prompt,
                tools=tools,
                messages=api_messages,
            )

            # Check if we got a text response (no more tool calls)
            if response.stop_reason == 'end_turn':
                # Extract text from content blocks
                text_parts = []
                for block in response.content:
                    if block.type == 'text':
                        text_parts.append(block.text)
                return '\n'.join(text_parts)

            # Process tool uses
            tool_results = []
            text_parts = []
            has_tool_use = False

            for block in response.content:
                if block.type == 'text':
                    text_parts.append(block.text)
                elif block.type == 'tool_use':
                    has_tool_use = True
                    tool_name = block.name
                    tool_input = block.input
                    tool_use_id = block.id

                    # Check access
                    tool_def = _get_tool_def_by_name(tool_name)
                    if tool_def:
                        access_error = _check_tool_access(tool_def, tool_input, user_context)
                        if access_error:
                            tool_results.append({
                                'type': 'tool_result',
                                'tool_use_id': tool_use_id,
                                'content': json.dumps({'error': access_error}),
                            })
                            continue

                    # Handle support ticket specially
                    if tool_name == 'create_support_ticket':
                        user_context['_support_ticket'] = {
                            'summary': tool_input.get('summary', ''),
                            'details': tool_input.get('details', ''),
                        }
                        tool_results.append({
                            'type': 'tool_result',
                            'tool_use_id': tool_use_id,
                            'content': json.dumps({'success': True, 'message': 'Support ticket will be created.'}),
                        })
                        continue

                    # Execute tool
                    func = TOOL_FUNCTIONS.get(tool_name)
                    if func:
                        try:
                            result = func(**tool_input)
                            result_str = json.dumps(result, default=str, ensure_ascii=False)
                            # Truncate very large results
                            if len(result_str) > 8000:
                                result_str = result_str[:8000] + '...(truncated)'
                            tool_results.append({
                                'type': 'tool_result',
                                'tool_use_id': tool_use_id,
                                'content': result_str,
                            })
                        except Exception as e:
                            tool_results.append({
                                'type': 'tool_result',
                                'tool_use_id': tool_use_id,
                                'content': json.dumps({'error': str(e)}),
                            })
                    else:
                        tool_results.append({
                            'type': 'tool_result',
                            'tool_use_id': tool_use_id,
                            'content': json.dumps({'error': f'Unknown tool: {tool_name}'}),
                        })

            if not has_tool_use:
                # No tool calls, just return text
                return '\n'.join(text_parts) if text_parts else ''

            # Add assistant message with tool use blocks, then tool results
            api_messages.append({
                'role': 'assistant',
                'content': response.content,
            })
            api_messages.append({
                'role': 'user',
                'content': tool_results,
            })

        # If we hit max rounds, return whatever text we have
        return 'Jag kunde inte slutfora analysen. Prova att formulera fragan annorlunda.'


# =========================================================================
# Chat history persistence
# =========================================================================

def _load_all_history() -> dict:
    """Load all chat history from file."""
    if not os.path.exists(CHAT_HISTORY_FILE):
        return {}
    try:
        with open(CHAT_HISTORY_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def _save_all_history(data: dict):
    """Save all chat history to file."""
    with open(CHAT_HISTORY_FILE, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_conversation(username: str, conversation_id: str = None) -> list:
    """Load a user's conversation history."""
    data = _load_all_history()
    user_data = data.get(username, {})

    if conversation_id and conversation_id in user_data.get('conversations', {}):
        return user_data['conversations'][conversation_id].get('messages', [])

    return []


def save_conversation(username: str, conversation_id: str, messages: list,
                      support_ticket: bool = False):
    """Save a conversation to persistent storage."""
    data = _load_all_history()

    if username not in data:
        data[username] = {'conversations': {}}

    if 'conversations' not in data[username]:
        data[username]['conversations'] = {}

    data[username]['conversations'][conversation_id] = {
        'messages': messages,
        'updated_at': datetime.now(SWEDISH_TZ).isoformat(),
        'support_ticket': support_ticket,
    }

    # Keep only last 20 conversations per user
    convs = data[username]['conversations']
    if len(convs) > 20:
        sorted_ids = sorted(convs.keys(),
                           key=lambda k: convs[k].get('updated_at', ''),
                           reverse=True)
        for old_id in sorted_ids[20:]:
            del convs[old_id]

    _save_all_history(data)


def get_user_conversations(username: str) -> list:
    """Get list of a user's conversations (newest first)."""
    data = _load_all_history()
    user_data = data.get(username, {})
    convs = user_data.get('conversations', {})

    result = []
    for cid, cdata in convs.items():
        msgs = cdata.get('messages', [])
        first_user_msg = ''
        for m in msgs:
            if m['role'] == 'user':
                first_user_msg = m['content'][:80]
                break

        result.append({
            'id': cid,
            'preview': first_user_msg,
            'updated_at': cdata.get('updated_at', ''),
            'message_count': len(msgs),
            'support_ticket': cdata.get('support_ticket', False),
        })

    result.sort(key=lambda x: x['updated_at'], reverse=True)
    return result[:10]


# =========================================================================
# Singleton
# =========================================================================

_assistant = None
_admin_assistant = None


def get_assistant(admin: bool = False) -> Optional[BVProAssistant]:
    """Get or create the assistant instance."""
    global _assistant, _admin_assistant

    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        return None

    if admin:
        if _admin_assistant is None:
            model = os.environ.get('AI_MODEL_ADMIN', 'claude-sonnet-4-5-20250929')
            _admin_assistant = BVProAssistant(api_key=api_key, model=model)
        return _admin_assistant
    else:
        if _assistant is None:
            _assistant = BVProAssistant(api_key=api_key)
        return _assistant
