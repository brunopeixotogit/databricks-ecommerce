// Tiny shared helper exposed as window.App. Keeps a few snippets out of
// the per-page <script> blocks so they don't drift.
(function () {
  const App = {
    refreshCartBadge() {
      const badge = document.getElementById("cart-count");
      if (!badge) return;
      const cart = Tracker.getCart();
      const count = cart ? cart.items.reduce((s, it) => s + it.qty, 0) : 0;
      if (count > 0) {
        badge.textContent = String(count);
        badge.classList.remove("d-none");
      } else {
        badge.classList.add("d-none");
      }
    },
  };
  window.App = App;
})();
