// BricksShop multi-agent chat widget.
//
// Floating launcher → panel with message list + input. Talks to the
// FastAPI backend at /chat and /chat/add-to-cart. Inline product cards
// are rendered from `[[P:product_id]]` markers the assistant emits, plus
// the structured `products` array the API returns.
//
// Cart integration: clicking "Add to cart" on a chat product calls
// /chat/add-to-cart (which writes an add_to_cart event into the landing
// volume) AND mirrors the item into the existing localStorage cart so
// the cart badge / cart page see it immediately.

(function () {
  const SESSION_KEY = "ecom.chat.session";
  const STORAGE_OPEN = "ecom.chat.open";

  function uid(prefix) {
    return prefix + "_" + Math.random().toString(36).slice(2, 10);
  }

  function getOrCreateChatSession() {
    let id = localStorage.getItem(SESSION_KEY);
    if (!id) {
      id = uid("chat");
      localStorage.setItem(SESSION_KEY, id);
    }
    return id;
  }

  function getUserId() {
    return localStorage.getItem("ecom.user_id") || null;
  }

  function getCartId() {
    try {
      const cart = window.Tracker && Tracker.getCart ? Tracker.getCart() : null;
      return cart && cart.cart_id ? cart.cart_id : null;
    } catch (_) {
      return null;
    }
  }

  // Mirror an item into the existing localStorage cart WITHOUT emitting a
  // tracker add_to_cart event — the server already wrote one when it
  // processed /chat/add-to-cart, and we don't want the pipeline to see
  // the same purchase twice.
  function mirrorIntoLocalCart(product, qty) {
    try {
      const KEY = "ecom.cart";
      const raw = localStorage.getItem(KEY);
      let cart = raw ? JSON.parse(raw) : null;
      if (!cart || !Array.isArray(cart.items)) {
        cart = {
          cart_id: "c_" + Math.random().toString(36).slice(2, 12),
          items: [],
        };
      }
      const hit = cart.items.find((it) => it.product_id === product.product_id);
      if (hit) {
        hit.qty += qty;
      } else {
        cart.items.push({
          product_id: product.product_id,
          qty: qty,
          price: product.price,
          category: product.category,
        });
      }
      localStorage.setItem(KEY, JSON.stringify(cart));
    } catch (_) {
      // Non-fatal; server-side event is already recorded.
    }
  }

  function el(html) {
    const t = document.createElement("template");
    t.innerHTML = html.trim();
    return t.content.firstChild;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  // Replace [[P:id]] markers with a small chip the user can click.
  function renderReply(text, products) {
    const byId = {};
    (products || []).forEach((p) => (byId[p.product_id] = p));
    const escaped = escapeHtml(text);
    return escaped.replace(/\[\[P:([a-zA-Z0-9_-]+)\]\]/g, (_, id) => {
      const p = byId[id];
      if (!p) return `<span class="chat-chip">${id}</span>`;
      return (
        `<span class="chat-chip" data-pid="${escapeHtml(id)}">` +
        `${escapeHtml(p.name)}</span>`
      );
    });
  }

  function productCard(p) {
    const price = `$${Number(p.price).toFixed(2)} ${escapeHtml(p.currency || "USD")}`;
    return el(
      `<div class="chat-card" data-pid="${escapeHtml(p.product_id)}">
         <div class="chat-card-body">
           <div class="chat-card-name">${escapeHtml(p.name)}</div>
           <div class="chat-card-meta">${escapeHtml(p.category)} · ${price}</div>
         </div>
         <button class="btn btn-sm btn-primary chat-card-add" type="button">Add</button>
       </div>`
    );
  }

  function bubble(role, html) {
    const cls = role === "user" ? "chat-bubble chat-user" : "chat-bubble chat-assistant";
    return el(`<div class="${cls}">${html}</div>`);
  }

  // ------------------------------------------------------------------
  function ChatWidget() {
    this.sessionId = getOrCreateChatSession();
    this.root = el(
      `<div id="chat-widget" class="chat-widget chat-closed">
         <button class="chat-launcher" type="button" aria-label="Open chat">
           <span aria-hidden="true">💬</span>
           <span class="chat-launcher-label">Ask BricksShop</span>
         </button>
         <div class="chat-panel" role="dialog" aria-label="BricksShop assistant">
           <div class="chat-header">
             <strong>BricksShop assistant</strong>
             <button class="chat-close" type="button" aria-label="Close">×</button>
           </div>
           <div class="chat-messages" aria-live="polite"></div>
           <form class="chat-form" autocomplete="off">
             <input class="chat-input" type="text" placeholder="What are you shopping for?" maxlength="2000" required />
             <button class="btn btn-primary chat-send" type="submit">Send</button>
           </form>
         </div>
       </div>`
    );
    this.messagesEl = this.root.querySelector(".chat-messages");
    this.inputEl = this.root.querySelector(".chat-input");
    this.formEl = this.root.querySelector(".chat-form");
    this.launcherEl = this.root.querySelector(".chat-launcher");
    this.closeEl = this.root.querySelector(".chat-close");

    this.launcherEl.addEventListener("click", () => this.open());
    this.closeEl.addEventListener("click", () => this.close());
    this.formEl.addEventListener("submit", (e) => {
      e.preventDefault();
      this.send(this.inputEl.value);
    });
    this.messagesEl.addEventListener("click", (e) => {
      const btn = e.target.closest(".chat-card-add");
      if (!btn) return;
      const card = btn.closest(".chat-card");
      if (!card) return;
      this.addToCart(card.dataset.pid, btn);
    });

    if (localStorage.getItem(STORAGE_OPEN) === "1") {
      this.open(/*greet=*/ false);
    }
  }

  ChatWidget.prototype.open = function (greet) {
    this.root.classList.remove("chat-closed");
    this.root.classList.add("chat-open");
    localStorage.setItem(STORAGE_OPEN, "1");
    if (greet !== false && !this.messagesEl.children.length) {
      this.appendAssistantHtml(
        "Hi! I'm your BricksShop assistant. Ask me about <strong>electronics</strong>, " +
          "<strong>appliances</strong>, or <strong>furniture</strong> — for example, " +
          "<em>“show me a TV under $1000”</em>."
      );
    }
    setTimeout(() => this.inputEl.focus(), 50);
  };

  ChatWidget.prototype.close = function () {
    this.root.classList.add("chat-closed");
    this.root.classList.remove("chat-open");
    localStorage.setItem(STORAGE_OPEN, "0");
  };

  ChatWidget.prototype.appendUser = function (text) {
    this.messagesEl.appendChild(bubble("user", escapeHtml(text)));
    this.scroll();
  };

  ChatWidget.prototype.appendAssistantHtml = function (html) {
    this.messagesEl.appendChild(bubble("assistant", html));
    this.scroll();
  };

  ChatWidget.prototype.appendProducts = function (products) {
    if (!products || !products.length) return;
    const wrap = el(`<div class="chat-cards"></div>`);
    products.forEach((p) => wrap.appendChild(productCard(p)));
    this.messagesEl.appendChild(wrap);
    this.scroll();
  };

  ChatWidget.prototype.appendThinking = function () {
    const node = el(
      `<div class="chat-bubble chat-assistant chat-thinking">
         <span class="chat-dot"></span><span class="chat-dot"></span><span class="chat-dot"></span>
       </div>`
    );
    this.messagesEl.appendChild(node);
    this.scroll();
    return node;
  };

  ChatWidget.prototype.scroll = function () {
    this.messagesEl.scrollTop = this.messagesEl.scrollHeight;
  };

  ChatWidget.prototype.send = async function (text) {
    text = (text || "").trim();
    if (!text) return;
    this.inputEl.value = "";
    this.appendUser(text);
    const thinking = this.appendThinking();
    try {
      const resp = await fetch("/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: this.sessionId,
          user_id: getUserId(),
          message: text,
        }),
      });
      thinking.remove();
      if (!resp.ok) {
        const detail = await resp.text();
        this.appendAssistantHtml(
          `<em>Something went wrong (${resp.status}).</em> ${escapeHtml(detail.slice(0, 200))}`
        );
        return;
      }
      const data = await resp.json();
      this.appendAssistantHtml(renderReply(data.reply || "", data.products));
      this.appendProducts(data.products);
    } catch (err) {
      thinking.remove();
      this.appendAssistantHtml(
        `<em>Network error.</em> ${escapeHtml(String(err && err.message ? err.message : err))}`
      );
    }
  };

  ChatWidget.prototype.addToCart = async function (productId, btn) {
    if (!productId) return;
    btn.disabled = true;
    btn.textContent = "…";
    try {
      const resp = await fetch("/chat/add-to-cart", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session_id: this.sessionId,
          user_id: getUserId(),
          cart_id: getCartId(),
          product_id: productId,
          quantity: 1,
        }),
      });
      if (!resp.ok) {
        btn.disabled = false;
        btn.textContent = "Add";
        const detail = await resp.text();
        this.appendAssistantHtml(
          `<em>Couldn't add that (${resp.status}).</em> ${escapeHtml(detail.slice(0, 200))}`
        );
        return;
      }
      const data = await resp.json();
      const p = data.product;
      mirrorIntoLocalCart(p, data.quantity || 1);
      if (window.App && App.refreshCartBadge) App.refreshCartBadge();
      btn.textContent = "Added ✓";
      this.appendAssistantHtml(
        `Added <strong>${escapeHtml(p.name)}</strong> to your cart.`
      );
    } catch (err) {
      btn.disabled = false;
      btn.textContent = "Add";
      this.appendAssistantHtml(
        `<em>Network error adding to cart.</em> ${escapeHtml(String(err && err.message ? err.message : err))}`
      );
    }
  };

  // Bootstrap once DOM is ready.
  function init() {
    if (document.getElementById("chat-widget")) return;
    const widget = new ChatWidget();
    document.body.appendChild(widget.root);
    window.BricksShopChat = widget;
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
