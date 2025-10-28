const input = document.getElementById('search-input');
const dropdown = document.getElementById('dropdown');
const recommendations = document.getElementById('recommendations');
const resetBtn = document.getElementById('reset-btn');

let tables = [];
let selectedIndex = -1;
let selectedTable = null;

input.addEventListener('input', async () => {
  const q = input.value.trim();
  if (!q) {
    dropdown.style.display = 'none';
    recommendations.style.display = 'none';
    selectedTable = null;
    selectedIndex = -1;
    return;
  }

  const res = await fetch(`/search_api?q=${encodeURIComponent(q)}`);
  tables = await res.json();

  if (tables.length === 0) {
    dropdown.style.display = 'none';
    recommendations.style.display = 'none';
    selectedTable = null;
    selectedIndex = -1;
    return;
  }

  dropdown.innerHTML = '';
  tables.forEach((item, index) => {
    const div = document.createElement('div');
    div.textContent = item.table;
    div.setAttribute('role', 'option');
    div.tabIndex = 0;
    div.addEventListener('click', () => selectTable(index));
    div.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        selectTable(index);
      }
    });
    dropdown.appendChild(div);
  });
  selectedIndex = -1;
  dropdown.style.display = 'block';
  recommendations.style.display = 'none';
  selectedTable = null;
});

input.addEventListener('keydown', (e) => {
  if (dropdown.style.display === 'block') {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      selectedIndex = (selectedIndex + 1) % tables.length;
      updateFocus();
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      selectedIndex = (selectedIndex - 1 + tables.length) % tables.length;
      updateFocus();
    } else if (e.key === 'Enter' && selectedIndex >= 0) {
      e.preventDefault();
      selectTable(selectedIndex);
    }
  }
});

resetBtn.addEventListener('click', () => {
  input.value = '';
  dropdown.style.display = 'none';
  recommendations.style.display = 'none';
  selectedTable = null;
  selectedIndex = -1;
  input.focus();
});

function updateFocus() {
  const options = dropdown.querySelectorAll('div');
  options.forEach((opt, i) => {
    if (i === selectedIndex) {
      opt.focus();
    }
  });
}

async function selectTable(index) {
  selectedTable = tables[index].table;
  input.value = selectedTable;
  dropdown.style.display = 'none';

  const res = await fetch(`/recommendations_api?table=${encodeURIComponent(selectedTable)}`);
  const recs = await res.json();

  if (recs.length === 0) {
    recommendations.style.display = 'none';
    recommendations.innerHTML = '';
    return;
  }

  let html = `<h3>Recommendations for "${selectedTable}"</h3>`;
  recs.forEach(rec => {
    html += `<p><strong>Recommendation:</strong> ${escapeHtml(rec.recommendation)}</p>`;
    html += `<p><em>Reason:</em> ${escapeHtml(rec.reason)}</p>`;
    html += `<hr/>`;
  });
  recommendations.innerHTML = html;
  recommendations.style.display = 'block';
}

function escapeHtml(text) {
  return text.replace(/[&<>"']/g, (m) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  })[m]);
}
