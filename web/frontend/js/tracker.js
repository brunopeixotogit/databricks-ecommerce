// Event tracker — emits page_view / add_to_cart / purchase / abandon_cart
// in a shape compatible with src/common/schemas.py::EVENT_SCHEMA.
//
// Identity model:
//   user_id    — generated on first visit, stored in localStorage. Null
//                would also be valid (anon), but a stable id makes the
//                Silver dim_user_360 mart more interesting.
//   session_id — generated per session; rolled over after 30 min idle so
//                the Silver re-derived session_id_silver lines up with
//                Bronze's session boundaries.
//   cart_id    — generated when the cart becomes non-empty; cleared on
//                purchase or after an abandon_cart fires.
//
// The tracker batches events in memory and flushes every 4s (or on a
// hard cap of 25 events, or on pagehide) to /events/batch. A pagehide
// flush uses sendBeacon so the browser does not race with navigation.

(function () {
  const FLUSH_INTERVAL_MS = 4000;
  const FLUSH_BATCH_MAX = 25;
  const SESSION_IDLE_MINUTES = 30;
  const SCHEMA_VERSION = "1.0.0";
  const ENDPOINT_BATCH = "/events/batch";

  const LS_USER = "ecom.user_id";
  const LS_SESSION = "ecom.session";       // {id, last_active_iso}
  const LS_CART = "ecom.cart";             // {cart_id, items: [{product_id, qty, price, category}]}

  // ---------- identity ----------
  function getOrCreateUserId() {
    let uid = localStorage.getItem(LS_USER);
    if (!uid) {
      uid = "u_" + cryptoUuid().slice(0, 12);
      localStorage.setItem(LS_USER, uid);
    }
    return uid;
  }

  function getOrCreateSessionId() {
    const now = new Date();
    const raw = localStorage.getItem(LS_SESSION);
    if (raw) {
      try {
        const cached = JSON.parse(raw);
        const lastActive = new Date(cached.last_active_iso);
        const idleMin = (now - lastActive) / 60000;
        if (idleMin < SESSION_IDLE_MINUTES) {
          cached.last_active_iso = now.toISOString();
          localStorage.setItem(LS_SESSION, JSON.stringify(cached));
          return cached.id;
        }
      } catch (_) { /* fall through to new session */ }
    }
    const sid = "s_" + cryptoUuid().slice(0, 12);
    localStorage.setItem(LS_SESSION, JSON.stringify({ id: sid, last_active_iso: now.toISOString() }));
    return sid;
  }

  function touchSession() {
    const raw = localStorage.getItem(LS_SESSION);
    if (!raw) return;
    try {
      const cached = JSON.parse(raw);
      cached.last_active_iso = new Date().toISOString();
      localStorage.setItem(LS_SESSION, JSON.stringify(cached));
    } catch (_) { /* ignore */ }
  }

  // ---------- cart ----------
  function getCart() {
    const raw = localStorage.getItem(LS_CART);
    if (!raw) return null;
    try { return JSON.parse(raw); } catch (_) { return null; }
  }

  function setCart(cart) {
    if (!cart || !cart.items || cart.items.length === 0) {
      localStorage.removeItem(LS_CART);
    } else {
      localStorage.setItem(LS_CART, JSON.stringify(cart));
    }
  }

  function clearCart() {
    localStorage.removeItem(LS_CART);
  }

  // ---------- device detection ----------
  function detectDevice() {
    const ua = (navigator.userAgent || "").toLowerCase();
    if (/(tablet|ipad)/.test(ua)) return "tablet";
    if (/(mobi|android|iphone)/.test(ua)) return "mobile";
    return "desktop";
  }

  // ---------- helpers ----------
  function cryptoUuid() {
    if (window.crypto && window.crypto.randomUUID) {
      return window.crypto.randomUUID().replace(/-/g, "");
    }
    // RFC4122-ish fallback.
    return Array.from(crypto.getRandomValues(new Uint8Array(16)))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  function nowIso() {
    return new Date().toISOString().replace(/\.\d+Z$/, "Z");
  }

  function buildEvent(eventType, fields) {
    return Object.assign(
      {
        event_id: "e_" + cryptoUuid(),
        event_type: eventType,
        event_ts: nowIso(),
        user_id: getOrCreateUserId(),
        session_id: getOrCreateSessionId(),
        device: detectDevice(),
        // user_agent / ip filled in by the backend from the request
        country: navigator.language ? navigator.language.split("-").pop().toUpperCase() : null,
        page_url: location.pathname + location.search,
        referrer: document.referrer || null,
        product_id: null,
        category: null,
        price: null,
        quantity: null,
        cart_id: null,
        order_id: null,
        payment_method: null,
        discount_code: null,
        properties: { source: "web" },
        schema_version: SCHEMA_VERSION,
      },
      fields || {}
    );
  }

  // ---------- buffer / flush ----------
  const buffer = [];
  let flushTimer = null;

  function scheduleFlush() {
    if (flushTimer) return;
    flushTimer = setTimeout(() => {
      flushTimer = null;
      flushBuffer();
    }, FLUSH_INTERVAL_MS);
  }

  function flushBuffer(useBeacon) {
    if (buffer.length === 0) return;
    const events = buffer.splice(0, buffer.length);
    const payload = JSON.stringify({ events });
    if (useBeacon && navigator.sendBeacon) {
      const blob = new Blob([payload], { type: "application/json" });
      navigator.sendBeacon(ENDPOINT_BATCH, blob);
      return;
    }
    fetch(ENDPOINT_BATCH, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: payload,
      keepalive: true,
    }).catch((err) => {
      console.warn("event batch upload failed", err);
      // Re-queue on failure so the next flush retries.
      buffer.unshift(...events);
    });
  }

  function emit(event) {
    buffer.push(event);
    touchSession();
    if (buffer.length >= FLUSH_BATCH_MAX) {
      flushBuffer();
    } else {
      scheduleFlush();
    }
  }

  // ---------- public API ----------
  const Tracker = {
    pageView() {
      emit(buildEvent("page_view"));
    },

    addToCart(product, quantity) {
      const qty = Math.max(1, parseInt(quantity || 1, 10));
      let cart = getCart();
      if (!cart) {
        cart = { cart_id: "c_" + cryptoUuid().slice(0, 10), items: [] };
      }
      const existing = cart.items.find((it) => it.product_id === product.id);
      if (existing) {
        existing.qty += qty;
      } else {
        cart.items.push({
          product_id: product.id,
          qty,
          price: product.price,
          category: product.category,
        });
      }
      setCart(cart);
      emit(buildEvent("add_to_cart", {
        product_id: product.id,
        category: product.category,
        price: product.price,
        quantity: qty,
        cart_id: cart.cart_id,
      }));
    },

    purchase({ paymentMethod, country }) {
      const cart = getCart();
      if (!cart || cart.items.length === 0) return null;
      const orderId = "o_" + cryptoUuid().slice(0, 14);
      // One purchase event per line — matches Bronze where fact_orders is
      // built by aggregating purchase events on order_id.
      cart.items.forEach((it) => {
        emit(buildEvent("purchase", {
          product_id: it.product_id,
          category: it.category,
          price: it.price,
          quantity: it.qty,
          cart_id: cart.cart_id,
          order_id: orderId,
          payment_method: paymentMethod || "credit_card",
          country: country || null,
        }));
      });
      flushBuffer();      // make sure the order shows up promptly
      clearCart();
      return orderId;
    },

    abandonCart() {
      const cart = getCart();
      if (!cart || cart.items.length === 0) return;
      emit(buildEvent("abandon_cart", {
        cart_id: cart.cart_id,
      }));
    },

    getCart,
    clearCart,
    flush: flushBuffer,
  };

  // Auto-fire page_view on every page load.
  document.addEventListener("DOMContentLoaded", Tracker.pageView);

  // Best-effort cart abandonment — fires when the tab is closed or hidden
  // for navigation away from the site, with a non-empty cart in storage.
  window.addEventListener("pagehide", () => {
    Tracker.abandonCart();
    flushBuffer(true);
  });

  window.Tracker = Tracker;
})();
