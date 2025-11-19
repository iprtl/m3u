const https = require('https');
const zlib = require('zlib');

const CONFIG = {
  jsonEpgUrl: 'https://github.com/iprtl/m3u/epgshare01-to-json/raw/refs/heads/live/epg.json.gz',
  daysPast: 1,
  daysFuture: 0
};

module.exports = async (req, res) => {
  try {
    const playlistUrl = req.query.playlist;

    if (!playlistUrl) {
      return res.status(400).json({ 
        error: 'Missing playlist parameter',
        usage: '?playlist=YOUR_PLAYLIST_URL'
      });
    }

    console.log('Starting EPG build from JSON...');

    const tvgIds = await extractTvgIds(playlistUrl);
    console.log(`Found ${tvgIds.size} tvg-ids`);

    const epgData = await downloadAndParseJson(CONFIG.jsonEpgUrl);

    if (!epgData || !epgData.v) {
      return res.status(500).json({ error: 'Invalid EPG data format' });
    }

    console.log(`EPG format version: ${epgData.v}`);
    console.log(`String pool size: ${epgData.sp ? epgData.sp.length : 0}`);
    console.log(`Channels: ${epgData.c ? epgData.c.length : 0}`);
    console.log(`Programmes: ${epgData.p ? epgData.p.length : 0}`);

    const epgXml = buildEpgFromJson(epgData, tvgIds);
    console.log('Built filtered EPG');

    const compressed = zlib.gzipSync(epgXml);

    res.setHeader('Content-Type', 'application/gzip');
    res.setHeader('Content-Disposition', 'attachment; filename="epg.xml.gz"');
    res.send(compressed);

  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: error.message });
  }
};

async function extractTvgIds(url) {
  const content = await fetchUrl(url);
  const tvgIds = new Set();
  const regex = /tvg-id="([^"]+)"/g;
  let match;

  while ((match = regex.exec(content)) !== null) {
    if (match[1]) tvgIds.add(match[1]);
  }

  return tvgIds;
}

async function downloadAndParseJson(url) {
  return new Promise((resolve, reject) => {
    console.log('Downloading JSON EPG...');

    const handleResponse = (res) => {

      if (res.statusCode === 302 || res.statusCode === 301) {
        console.log('Following redirect...');
        https.get(res.headers.location, handleResponse).on('error', reject);
        return;
      }

      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }

      const chunks = [];
      const gunzip = zlib.createGunzip();

      res.pipe(gunzip);

      gunzip.on('data', chunk => chunks.push(chunk));

      gunzip.on('end', () => {
        try {
          const jsonText = Buffer.concat(chunks).toString('utf-8');
          console.log(`Decompressed: ${(jsonText.length / 1024 / 1024).toFixed(1)} MB`);
          const data = JSON.parse(jsonText);
          resolve(data);
        } catch (err) {
          reject(new Error(`Parse error: ${err.message}`));
        }
      });

      gunzip.on('error', (err) => {
        reject(new Error(`Gunzip error: ${err.message}`));
      });

      res.on('error', (err) => {
        reject(new Error(`Download error: ${err.message}`));
      });
    };

    https.get(url, handleResponse).on('error', reject);
  });
}

function buildEpgFromJson(epgData, tvgIds) {
  const output = ['<?xml version="1.0" encoding="utf-8"?>', '<tv>'];
  const seenChannels = new Set();
  const seenProgrammes = new Set();

  let stats = { channels: 0, programmes: 0, filtered: 0 };

  const stringPool = epgData.sp || [];

  if (epgData.c && Array.isArray(epgData.c)) {
    for (const channel of epgData.c) {
      const channelId = channel.i;

      if (!tvgIds.has(channelId)) continue;
      if (seenChannels.has(channelId)) continue;
      seenChannels.add(channelId);

      let xml = `<channel id="${safeEscapeXml(channelId)}">`;
      if (channel.n) xml += `<display-name>${safeEscapeXml(channel.n)}</display-name>`;
      if (channel.ic) xml += `<icon src="${safeEscapeXml(channel.ic)}" />`;
      xml += `</channel>`;

      output.push(`  ${xml}`);
      stats.channels++;
    }
  }

  console.log(`Added ${stats.channels} channels, now processing programmes...`);

  const now = Date.now();
  const pastCutoff = now - (CONFIG.daysPast * 24 * 60 * 60 * 1000);

  if (epgData.p && Array.isArray(epgData.p)) {
    let processedCount = 0;

    for (const prog of epgData.p) {
      processedCount++;

      if (processedCount % 50000 === 0) {
        console.log(`  Processed ${processedCount} programmes, added ${stats.programmes}...`);
      }

      const channelIdx = prog[0];
      const start = prog[1];
      const stop = prog[2];
      const title = prog[3];
      const subtitle = prog[4] || null;
      const desc = prog[5] || null;

      const channelId = stringPool[channelIdx];

      if (!channelId || !tvgIds.has(channelId)) {
        stats.filtered++;
        continue;
      }

      if (CONFIG.daysPast > 0 && stop) {
        const stopDate = parseXmltvTime(stop);
        if (stopDate && stopDate.getTime() < pastCutoff) {
          stats.filtered++;
          continue;
        }
      }

      const progKey = `${channelId}_${start}_${stop}`;
      if (seenProgrammes.has(progKey)) continue;
      seenProgrammes.add(progKey);

      let xml = `<programme channel="${safeEscapeXml(channelId)}" start="${start} +0000" stop="${stop} +0000">`;
      if (title) xml += `<title>${safeEscapeXml(title)}</title>`;
      if (subtitle) xml += `<sub-title>${safeEscapeXml(subtitle)}</sub-title>`;
      if (desc) xml += `<desc>${safeEscapeXml(desc)}</desc>`;
      xml += `</programme>`;

      output.push(`  ${xml}`);
      stats.programmes++;
    }
  }

  output.push('</tv>');

  console.log(`Stats: ${stats.channels} channels, ${stats.programmes} programmes, ${stats.filtered} filtered`);

  return output.join('\n');
}

function parseXmltvTime(timeStr) {
  if (!timeStr) return null;
  try {
    const datePart = timeStr.substring(0, 14);
    return new Date(
      parseInt(datePart.substring(0, 4)),
      parseInt(datePart.substring(4, 6)) - 1,
      parseInt(datePart.substring(6, 8)),
      parseInt(datePart.substring(8, 10)),
      parseInt(datePart.substring(10, 12)),
      parseInt(datePart.substring(12, 14))
    );
  } catch {
    return null;
  }
}

function escapeXml(str) {
  if (!str) return '';
  return str.toString()
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function unescapeXml(str) {
  if (!str) return '';
  return str.toString()
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

function safeEscapeXml(str) {
  if (!str) return '';

  return escapeXml(unescapeXml(str));
}

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
      res.on('error', reject);
    }).on('error', reject);
  });
}
