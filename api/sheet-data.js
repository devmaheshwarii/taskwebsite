const SHEET_ID = process.env.GOOGLE_SHEET_ID || '1UCjOC7ChKyaWHIbp1XA_7IjeAO1cLtuUCXlVFDw0dtg';
const SHEET_RANGE = process.env.GOOGLE_SHEET_RANGE || 'A:H';
const SHEET_GID = process.env.GOOGLE_SHEET_GID || '0';
const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 300);

const EXPECTED_HEADERS = [
    'timestamp',
    'user',
    'task_id',
    'task',
    'due',
    'category',
    'follow_up',
    'status'
];

function normalizeHeader(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '_');
}

function decodeGoogleDateValue(value) {
    if (value === null || value === undefined) {
        return '';
    }

    const str = String(value);
    const match = str.match(/^Date\((\d+),(\d+),(\d+)(?:,(\d+),(\d+),(\d+))?\)$/);
    if (!match) {
        return str;
    }

    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const hour = Number(match[4] || 0);
    const minute = Number(match[5] || 0);
    const second = Number(match[6] || 0);

    const parsed = new Date(year, month, day, hour, minute, second);
    return Number.isNaN(parsed.getTime()) ? str : parsed.toISOString();
}

function parseGvizJson(rawText) {
    const start = rawText.indexOf('{');
    const end = rawText.lastIndexOf('}');
    if (start < 0 || end < 0 || end <= start) {
        throw new Error('Invalid GViz response format.');
    }

    return JSON.parse(rawText.slice(start, end + 1));
}

function rowsFromGviz(gvizPayload) {
    const table = gvizPayload?.table;
    const cols = table?.cols || [];
    const rows = table?.rows || [];

    const headers = cols.map(col => normalizeHeader(col?.label || col?.id || ''));

    return rows.map(row => {
        const values = row?.c || [];
        const item = {};

        for (let i = 0; i < headers.length; i += 1) {
            const key = headers[i] || EXPECTED_HEADERS[i] || `col_${i}`;
            const cellValue = values[i] ? values[i].v : '';
            item[key] = decodeGoogleDateValue(cellValue);
        }

        for (let i = 0; i < EXPECTED_HEADERS.length; i += 1) {
            const header = EXPECTED_HEADERS[i];
            if (!(header in item)) {
                item[header] = '';
            }
        }

        return item;
    });
}

function rowsFromSheetsApi(values) {
    if (!Array.isArray(values) || values.length === 0) {
        return [];
    }

    const [headerRow, ...dataRows] = values;
    const headers = (headerRow || []).map(normalizeHeader);

    return dataRows.map(row => {
        const item = {};
        for (let i = 0; i < headers.length; i += 1) {
            const key = headers[i] || EXPECTED_HEADERS[i] || `col_${i}`;
            item[key] = row[i] ?? '';
        }

        for (let i = 0; i < EXPECTED_HEADERS.length; i += 1) {
            const header = EXPECTED_HEADERS[i];
            if (!(header in item)) {
                item[header] = '';
            }
        }

        return item;
    });
}

async function fetchFromSheetsApi() {
    const apiKey = process.env.GOOGLE_SHEETS_API_KEY;
    if (!apiKey) {
        return null;
    }

    const endpoint = `https://sheets.googleapis.com/v4/spreadsheets/${encodeURIComponent(SHEET_ID)}/values/${encodeURIComponent(SHEET_RANGE)}?key=${encodeURIComponent(apiKey)}`;
    const response = await fetch(endpoint);
    if (!response.ok) {
        const body = await response.text();
        throw new Error(`Sheets API request failed (${response.status}): ${body}`);
    }

    const payload = await response.json();
    return rowsFromSheetsApi(payload.values || []);
}

async function fetchFromPublicGviz() {
    const endpoint = `https://docs.google.com/spreadsheets/d/${encodeURIComponent(SHEET_ID)}/gviz/tq?tqx=out:json&gid=${encodeURIComponent(SHEET_GID)}`;
    const response = await fetch(endpoint);
    if (!response.ok) {
        const body = await response.text();
        throw new Error(`GViz request failed (${response.status}): ${body}`);
    }

    const text = await response.text();
    const parsed = parseGvizJson(text);
    return rowsFromGviz(parsed);
}

module.exports = async function handler(req, res) {
    if (req.method !== 'GET') {
        res.setHeader('Allow', 'GET');
        return res.status(405).json({ error: 'Method not allowed' });
    }

    try {
        const rows = (await fetchFromSheetsApi()) || (await fetchFromPublicGviz());

        res.setHeader('Cache-Control', `s-maxage=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`);
        return res.status(200).json({
            rows,
            meta: {
                rowCount: rows.length,
                fetchedAt: new Date().toISOString(),
                source: process.env.GOOGLE_SHEETS_API_KEY ? 'google-sheets-api' : 'gviz-public'
            }
        });
    } catch (error) {
        return res.status(500).json({
            error: 'Failed to fetch Google Sheet data',
            details: error.message
        });
    }
};
