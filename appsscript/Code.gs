/**
 * PulseForge Apps Script API
 * 
 * Serves market data from the PulseForge Data Google Sheet as JSON.
 * Deploy as a Web App (Anyone can access) to get a public URL.
 *
 * Sheet ID: 1x0H-uz5NSy1dm9QDBkpOEGmMoYnery3EqDlsTw1e624
 * 
 * Tab structure:
 *   Pulse       — date, score, signal, description
 *   Quotes      — ticker, price, change_pct, volume, signal, notes
 *   Sectors     — name, symbol, price, change_pct
 *   Watchlist   — ticker, price, change_pct, volume, signal, notes
 *   Volatility  — date, value, sma
 *   Predictions — name, direction, confidence, horizon, rationale, timestamp
 *   Macro       — note, timestamp
 *   OptionPlays — ticker, strategy, strike, expiry, entry, target, stop, timestamp
 */

const SHEET_ID = '1x0H-uz5NSy1dm9QDBkpOEGmMoYnery3EqDlsTw1e624';

function doGet(e) {
  try {
    const data = buildResponse();
    const output = ContentService
      .createTextOutput(JSON.stringify(data))
      .setMimeType(ContentService.MimeType.JSON);
    return output;
  } catch (err) {
    const errResponse = { error: err.toString(), timestamp: new Date().toISOString() };
    return ContentService
      .createTextOutput(JSON.stringify(errResponse))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function buildResponse() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  
  return {
    pulse: getPulseData(ss),
    quotes: getSheetAsObjects(ss, 'Quotes'),
    sectors: getSheetAsObjects(ss, 'Sectors'),
    watchlist: getSheetAsObjects(ss, 'Watchlist'),
    volatility: getVolatilityData(ss),
    predictions: getSheetAsObjects(ss, 'Predictions'),
    macro: getMacroData(ss),
    optionPlays: getOptionPlaysData(ss),
    last_updated: new Date().toISOString()
  };
}

/**
 * Read a sheet and return an array of objects (row → {header: value})
 */
function getSheetAsObjects(ss, sheetName) {
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return [];
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];
  
  const headers = data[0];
  const rows = data.slice(1);
  
  return rows
    .filter(row => row.some(cell => cell !== '' && cell !== null))
    .map(row => {
      const obj = {};
      headers.forEach((h, i) => {
        if (h) obj[h] = row[i] === '' ? null : row[i];
      });
      return obj;
    });
}

/**
 * Build pulse data in the format PulseForge expects:
 * { dates: [...], scores: [...], last_updated: ... }
 */
function getPulseData(ss) {
  const sheet = ss.getSheetByName('Pulse');
  if (!sheet) return { dates: [], scores: [], signals: [], last_updated: null };
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { dates: [], scores: [], signals: [], last_updated: null };
  
  // headers: date, score, signal, description
  const rows = data.slice(1).filter(r => r[0] !== '');
  
  const dates = rows.map(r => {
    const d = r[0];
    if (d instanceof Date) return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
    return String(d);
  });
  const scores = rows.map(r => parseFloat(r[1]) || 0);
  const signals = rows.map(r => String(r[2] || ''));
  const descriptions = rows.map(r => String(r[3] || ''));
  
  // Current (last row)
  const lastRow = rows[rows.length - 1];
  const currentScore = parseFloat(lastRow[1]) || 0;
  const currentSignal = String(lastRow[2] || '');
  
  return {
    dates,
    scores,
    signals,
    descriptions,
    current_score: currentScore,
    current_signal: currentSignal,
    last_updated: new Date().toISOString()
  };
}

/**
 * Build volatility data with vix_history and vix_sma sub-objects
 */
function getVolatilityData(ss) {
  const sheet = ss.getSheetByName('Volatility');
  if (!sheet) return { vix_history: { dates: [], values: [] }, vix_sma: { dates: [], values: [] } };
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { vix_history: { dates: [], values: [] }, vix_sma: { dates: [], values: [] } };
  
  // headers: date, value, sma
  const rows = data.slice(1).filter(r => r[0] !== '');
  
  const dates = rows.map(r => {
    const d = r[0];
    if (d instanceof Date) return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
    return String(d);
  });
  const values = rows.map(r => parseFloat(r[1]) || null);
  const smaValues = rows.map(r => r[2] !== '' ? parseFloat(r[2]) : null).filter((_, i) => rows[i][2] !== '');
  const smaDates = rows.filter(r => r[2] !== '').map(r => {
    const d = r[0];
    if (d instanceof Date) return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
    return String(d);
  });
  
  // Current VIX
  const currentVix = values[values.length - 1];
  
  return {
    vix_history: { dates, values },
    vix_sma: { dates: smaDates, values: smaValues },
    current_vix: currentVix,
    last_updated: new Date().toISOString()
  };
}

/**
 * Build macro data — just an array of notes strings
 */
function getMacroData(ss) {
  const sheet = ss.getSheetByName('Macro');
  if (!sheet) return { notes: [], last_updated: null };
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { notes: [], last_updated: null };
  
  // headers: note, timestamp
  const rows = data.slice(1).filter(r => r[0] !== '');
  const notes = rows.map(r => String(r[0]));
  
  return {
    notes,
    last_updated: new Date().toISOString()
  };
}

/**
 * Build option plays data — categorized by active/potential/closed
 */
function getOptionPlaysData(ss) {
  const sheet = ss.getSheetByName('OptionPlays');
  if (!sheet) return { active_plays: [], potential_plays: [], closed_plays: [], last_updated: null };
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { active_plays: [], potential_plays: [], closed_plays: [], last_updated: null };
  
  // headers: ticker, strategy, strike, expiry, entry, target, stop, status, timestamp
  const headers = data[0];
  const rows = data.slice(1)
    .filter(r => r.some(c => c !== ''))
    .map(row => {
      const obj = {};
      headers.forEach((h, i) => { if (h) obj[h] = row[i] === '' ? null : row[i]; });
      return obj;
    });
  
  return {
    active_plays: rows.filter(r => r.status === 'active'),
    potential_plays: rows.filter(r => r.status === 'potential' || !r.status),
    closed_plays: rows.filter(r => r.status === 'closed'),
    last_updated: new Date().toISOString()
  };
}

/**
 * Test function — run from the script editor to verify it works
 */
function testBuildResponse() {
  const result = buildResponse();
  Logger.log(JSON.stringify(result, null, 2));
  Logger.log('Pulse dates count: ' + result.pulse.dates.length);
  Logger.log('Quotes count: ' + result.quotes.length);
  Logger.log('Sectors count: ' + result.sectors.length);
  Logger.log('Watchlist count: ' + result.watchlist.length);
  Logger.log('Volatility dates count: ' + result.volatility.vix_history.dates.length);
}
