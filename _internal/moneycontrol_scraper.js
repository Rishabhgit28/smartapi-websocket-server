// moneycontrol_scraper.js
const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

(async () => {
  console.log('🌐  Launching browser…');
  const browser = await puppeteer.launch({ headless: 'new', timeout: 60_000 });
  const page = await browser.newPage();

  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123 Safari/537.36'
  );
  await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

  try {
    console.log('➡️  Opening Moneycontrol “Latest Results”…');
    await safeGoto(page, 'https://www.moneycontrol.com/markets/earnings/latest-results');

    await dismissGoogleVignette(page);
    console.log('🔄 Switching to QoQ mode…');
    await ensureQoQ(page);

    await page.waitForFunction(
      () => [...document.querySelectorAll('li')].some(li => li.innerText.includes('Revenue')),
      { timeout: 15000 }
    );

    console.log('🔁 Scrolling & waiting for content…');
    await autoScroll(page, 8);
    await new Promise(r => setTimeout(r, 3000));

    console.log('🔍 Extracting data…');
    const results = await page.evaluate(() => {
      const cards = Array.from(document.querySelectorAll('li')).filter(li =>
        li.innerText.includes('Revenue') && li.innerText.includes('Net Profit')
      );

      return cards.map(card => {
        const name = card.querySelector('h3 a')?.innerText.trim() || '';
        const date = card.querySelector('time')?.innerText.trim() || '';

        // 🔹 NEW: extract the period (usually in <th> of thead)
        const period = card.querySelector('table.commonTable thead th')?.innerText.trim() || '';

        let revenue = '', net = '', revGrowth = '', netGrowth = '';
        card.querySelectorAll('table tr').forEach(row => {
          const tds = row.querySelectorAll('td');
          if (tds.length >= 4) {
            const label = (tds[0]?.innerText || '').trim().toLowerCase();
            const value = (tds[1]?.innerText || '').trim();
            const growth = (tds[3]?.innerText || '').trim();
            if (label.includes('revenue')) { revenue = value; revGrowth = growth; }
            if (label.includes('net'))     { net     = value; netGrowth = growth; }
          }
        });

        return name && revenue ? { name, period, date, revenue, net, revGrowth, netGrowth } : null;
      }).filter(Boolean);
    });

    if (!results.length) {
      await page.screenshot({ path: path.join(__dirname, 'debug_screenshot.png'), fullPage: true });
      throw new Error('No rows found – saved debug_screenshot.png');
    }

    const jsonOut = path.join(__dirname, 'earnings.json');
    fs.writeFileSync(jsonOut, JSON.stringify(results, null, 2));
    console.log(`✅ Saved ${results.length} rows to JSON → ${jsonOut}`);
  } catch (err) {
    console.error('❌ Script error:', err.message || err);
  } finally {
    await browser.close();
    console.log('🧹 Browser closed.');
  }
})();

/* ──────────────── Helpers ──────────────── */

async function safeGoto(page, url) {
  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });
  } catch (e) {
    console.warn('⚠️ networkidle2 timed out, retrying with domcontentloaded…');
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
  }
}

async function ensureQoQ(page) {
  const qoqInput = 'input[type="radio"][id="option0"]';
  const qoqLabel = 'label[for="option0"]';

  await page.waitForSelector(qoqInput, { visible: true, timeout: 15000 });
  const checked = await page.$eval(qoqInput, el => el.checked);
  if (checked) { console.log('✅ Already in QoQ mode.'); return; }

  console.log('📌 Toggling QoQ via label click…');
  await page.evaluate(sel => document.querySelector(sel)?.click(), qoqLabel);
  await page.waitForFunction(
    sel => document.querySelector(sel)?.checked === true,
    { timeout: 10000 },
    qoqInput
  );
  await new Promise(r => setTimeout(r, 3000));
}

async function autoScroll(page, times = 5) {
  for (let i = 0; i < times; i++) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight));
    await new Promise(r => setTimeout(r, 1500));
  }
}

async function dismissGoogleVignette(page) {
  try {
    await page.waitForSelector('iframe[src*="#google_vignette"]', { timeout: 4000 });
    await page.keyboard.press('Escape');
    console.log('🗙 Closed Google vignette ad.');
  } catch (_) { /* ignore */ }
}
