/* ============================================================
   AI Assistant Chat Panel
   ============================================================ */

(function() {
    'use strict';

    let currentConversationId = null;
    let isSending = false;

    // ---- Panel open/close ----

    window.toggleChat = function() {
        const panel = document.getElementById('chat-panel');
        const btn = document.querySelector('.nav-chat-btn');
        if (!panel) return;

        const isOpen = panel.classList.toggle('open');
        document.body.classList.toggle('chat-open', isOpen);
        if (btn) btn.classList.toggle('active', isOpen);
        localStorage.setItem('chatPanelOpen', isOpen ? '1' : '0');

        if (isOpen) {
            document.getElementById('chat-input').focus();
        }
    };

    // Restore panel state on load
    document.addEventListener('DOMContentLoaded', function() {
        if (localStorage.getItem('chatPanelOpen') === '1') {
            const panel = document.getElementById('chat-panel');
            const btn = document.querySelector('.nav-chat-btn');
            if (panel) {
                panel.classList.add('open');
                document.body.classList.add('chat-open');
                if (btn) btn.classList.add('active');
            }
        }
    });

    // ---- Sending messages ----

    window.chatSendMessage = function() {
        if (isSending) return;

        const input = document.getElementById('chat-input');
        const message = (input.value || '').trim();
        if (!message) return;

        input.value = '';
        chatAutoResize(input);
        appendMessage('user', message);
        showTyping();
        setSending(true);

        var payload = {
            message: message,
            conversation_id: currentConversationId,
        };
        // Include current page entity context if available
        var ctx = window.chatEntityContext || {};
        if (ctx.house_id) payload.house_id = ctx.house_id;
        if (ctx.building_id) payload.building_id = ctx.building_id;
        if (ctx.entity_name) payload.entity_name = ctx.entity_name;

        fetch('/api/assistant/chat', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload),
        })
        .then(function(r) {
            if (!r.ok) {
                return r.json().then(function(d) { throw new Error(d.error || 'Server error'); });
            }
            return r.json();
        })
        .then(function(data) {
            hideTyping();
            currentConversationId = data.conversation_id;
            appendMessage('assistant', data.response);
        })
        .catch(function(err) {
            hideTyping();
            appendError(err.message || 'Kunde inte nå assistenten.');
        })
        .finally(function() {
            setSending(false);
        });
    };

    window.chatSendSuggestion = function(btn) {
        var input = document.getElementById('chat-input');
        input.value = btn.textContent;
        chatSendMessage();
    };

    window.chatHandleKey = function(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            chatSendMessage();
        }
    };

    window.chatAutoResize = function(el) {
        el.style.height = 'auto';
        el.style.height = Math.min(el.scrollHeight, 120) + 'px';
    };

    window.chatNewConversation = function() {
        currentConversationId = null;
        var container = document.getElementById('chat-messages');
        // Keep only the welcome message
        var welcome = container.querySelector('.chat-welcome');
        container.innerHTML = '';
        if (welcome) container.appendChild(welcome);
        else {
            container.innerHTML = '<div class="chat-welcome"><p><strong>Ny konversation.</strong> Stall din fraga!</p></div>';
        }
    };

    // ---- DOM helpers ----

    function appendMessage(role, text) {
        var container = document.getElementById('chat-messages');
        // Remove welcome on first message
        var welcome = container.querySelector('.chat-welcome');
        if (welcome) welcome.remove();

        var msgDiv = document.createElement('div');
        msgDiv.className = 'chat-msg chat-msg-' + role;

        var bubble = document.createElement('div');
        bubble.className = 'chat-msg-bubble';

        if (role === 'assistant') {
            bubble.innerHTML = renderMarkdown(text);
        } else {
            bubble.textContent = text;
        }

        msgDiv.appendChild(bubble);
        container.appendChild(msgDiv);
        scrollToBottom();
    }

    function appendError(text) {
        var container = document.getElementById('chat-messages');
        var div = document.createElement('div');
        div.className = 'chat-error';
        div.textContent = text;
        container.appendChild(div);
        scrollToBottom();
    }

    function showTyping() {
        var container = document.getElementById('chat-messages');
        var existing = container.querySelector('.chat-typing');
        if (existing) return;

        var div = document.createElement('div');
        div.className = 'chat-typing';
        div.innerHTML = '<div class="chat-typing-dot"></div><div class="chat-typing-dot"></div><div class="chat-typing-dot"></div>';
        container.appendChild(div);
        scrollToBottom();
    }

    function hideTyping() {
        var el = document.querySelector('.chat-typing');
        if (el) el.remove();
    }

    function scrollToBottom() {
        var container = document.getElementById('chat-messages');
        container.scrollTop = container.scrollHeight;
    }

    function setSending(val) {
        isSending = val;
        var btn = document.getElementById('chat-send-btn');
        if (btn) btn.disabled = val;
    }

    // ---- Simple markdown rendering ----

    function renderMarkdown(text) {
        if (!text) return '';

        // Escape HTML
        var html = text
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');

        // Code blocks (```)
        html = html.replace(/```(\w*)\n([\s\S]*?)```/g, function(_, lang, code) {
            return '<pre><code>' + code.trim() + '</code></pre>';
        });

        // Inline code
        html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

        // Bold
        html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

        // Italic
        html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

        // Unordered lists
        html = html.replace(/^[-*] (.+)$/gm, '<li>$1</li>');
        html = html.replace(/(<li>.*<\/li>\n?)+/g, function(match) {
            return '<ul>' + match + '</ul>';
        });

        // Ordered lists
        html = html.replace(/^\d+\. (.+)$/gm, '<li>$1</li>');

        // Paragraphs (double newlines)
        html = html.replace(/\n\n/g, '</p><p>');
        html = '<p>' + html + '</p>';

        // Clean up empty paragraphs
        html = html.replace(/<p>\s*<\/p>/g, '');

        // Single newlines to <br> inside paragraphs (but not before/after block elements)
        html = html.replace(/<\/p><p>/g, '</p>\n<p>');

        return html;
    }

})();
