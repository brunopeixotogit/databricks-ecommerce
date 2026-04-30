// Static product catalog — kept in sync with web/backend/simulator.py PRODUCTS.
// One source of truth for the demo would be a /api/products endpoint, but the
// pipeline is what we're showcasing here, not the catalog plumbing.

window.CATALOG = [
  { id: "p_e01", name: "Wireless Earbuds Pulse",     category: "electronics", price: 199.0, blurb: "12-hour battery, ANC, IPX4." },
  { id: "p_e02", name: "Bluetooth Speaker Kuma",     category: "electronics", price:  89.0, blurb: "Compact 360° speaker." },
  { id: "p_e03", name: "OLED 4K Smart TV 55\"",       category: "electronics", price: 1299.0, blurb: "HDR10+, 120Hz, AirPlay 2." },
  { id: "p_e04", name: "Mirrorless Camera X20",      category: "electronics", price: 449.0, blurb: "24MP, 4K30, dual SD." },
  { id: "p_a01", name: "Air Fryer Sora 5L",          category: "appliances",  price: 299.0, blurb: "Digital touch, 8 presets." },
  { id: "p_a02", name: "Side-by-Side Refrigerator",  category: "appliances",  price: 1099.0, blurb: "Frost-free, 540L, ice maker." },
  { id: "p_a03", name: "Robot Vacuum Lumi",          category: "appliances",  price: 159.0, blurb: "Lidar mapping, app control." },
  { id: "p_a04", name: "Front-Load Washer 11kg",     category: "appliances",  price: 749.0, blurb: "Steam refresh, A++." },
  { id: "p_f01", name: "Linen Sofa Atlas 3-Seat",    category: "furniture",   price: 349.0, blurb: "Reversible chaise, soft linen." },
  { id: "p_f02", name: "Walnut Side Table",          category: "furniture",   price:  89.0, blurb: "Solid walnut, brass legs." },
  { id: "p_f03", name: "Queen Bed Frame Hyle",       category: "furniture",   price: 1299.0, blurb: "Upholstered, slats included." },
  { id: "p_f04", name: "Ergonomic Office Chair",     category: "furniture",   price: 459.0, blurb: "Mesh back, adjustable arms." },
];

window.findProduct = function (id) {
  return window.CATALOG.find((p) => p.id === id) || null;
};
