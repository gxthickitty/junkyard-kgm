const axios = require('axios');
const xml2js = require('xml2js');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const readline = require('readline');

const CONFIG = {
    static: 'https://static.kogstatic.com/',
    dev: 'https://dev.kogstatic.com/'
};

const ALLOWED_EXTENSIONS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.mp3', '.wav', '.ogg']);
const CONCURRENCY = 10; // Download 10 files simultaneously
const SKIP_PATTERNS = /manifest|\.xml$|\.json$/i;

function sanitize(name) {
    return name.replace(/[<>:"/\\|?*]/g, '_');
}

function ask(question) {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => rl.question(question, (ans) => {
        rl.close();
        resolve(ans.toLowerCase().trim());
    }));
}

// Check if file exists and has same size/date
function shouldSkipFile(localPath, remoteSize, remoteDate) {
    if (!fs.existsSync(localPath)) return false;
    
    const stats = fs.statSync(localPath);
    const localSize = stats.size;
    const localMtime = stats.mtime.getTime();
    const remoteMtime = new Date(remoteDate).getTime();
    
    // Skip if same size and modification time
    return localSize === parseInt(remoteSize) && Math.abs(localMtime - remoteMtime) < 1000;
}

async function downloadFile(baseUrl, item, outputDir) {
    const key = item.Key[0];
    const ext = path.extname(key).toLowerCase();
    
    // Skip directories, non-media files, and manifests
    if (key.endsWith('/') || 
        !ALLOWED_EXTENSIONS.has(ext) || 
        SKIP_PATTERNS.test(key)) {
        return null;
    }
    
    const parts = key.split('/').map(sanitize);
    const fileName = parts.pop();
    const subDirs = path.join(outputDir, ...parts);
    const localFilePath = path.join(subDirs, fileName);
    
    if (shouldSkipFile(localFilePath, item.Size[0], item.LastModified[0])) {
        return { key, status: 'skipped' };
    }
    
    if (fs.existsSync(subDirs) && !fs.lstatSync(subDirs).isDirectory()) {
        fs.renameSync(subDirs, subDirs + '_old');
    }
    fs.mkdirSync(subDirs, { recursive: true });
    
    try {
        const fileResponse = await axios({
            url: `${baseUrl}${key}`, // No need to encode if baseUrl handles it
            method: 'GET',
            responseType: 'stream',
            timeout: 60000,
            maxRedirects: 5
        });
        
        await pipeline(fileResponse.data, fs.createWriteStream(localFilePath));
        
        const mtime = new Date(item.LastModified[0]);
        fs.utimesSync(localFilePath, mtime, mtime);
        
        return { key, status: 'downloaded' };
    } catch (err) {
        return { key, status: 'failed', error: err.message };
    }
}

async function processQueue(queue, baseUrl, outputDir) {
    const results = { downloaded: 0, skipped: 0, failed: 0 };

    for (let i = 0; i < queue.length; i += CONCURRENCY) {
        const batch = queue.slice(i, i + CONCURRENCY);
        const promises = batch.map(item => downloadFile(baseUrl, item, outputDir));
        
        const batchResults = await Promise.all(promises);
        
        batchResults.forEach(result => {
            if (!result) return;
            
            if (result.status === 'downloaded') {
                results.downloaded++;
                console.log(`✓ ${result.key}`);
            } else if (result.status === 'skipped') {
                results.skipped++;
            } else if (result.status === 'failed') {
                results.failed++;
                console.error(`✗ ${result.key}: ${result.error}`);
            }
        });
        const processed = Math.min(i + CONCURRENCY, queue.length);
        console.log(`Progress: ${processed}/${queue.length} (↓${results.downloaded} ≈${results.skipped} ✗${results.failed})`);
    }
    
    return results;
}

async function downloadFiles() {
    const choice = await ask('Download from (dev/static)? ');
    const baseUrl = CONFIG[choice];
    
    if (!baseUrl) {
        console.error('Invalid environment selected.');
        return;
    }
    
    const outputDir = path.join(__dirname, 'downloads', choice);
    let allFiles = [];
    let isTruncated = true;
    let nextMarker = null;
    
    console.log('Fetching file list...');
    
    try {
        while (isTruncated) {
            const listUrl = nextMarker 
                ? `${baseUrl}?marker=${encodeURIComponent(nextMarker)}` 
                : baseUrl;
            
            const response = await axios.get(listUrl);
            const result = await (new xml2js.Parser()).parseStringPromise(response.data);
            const bucketData = result.ListBucketResult;
            const contents = bucketData.Contents;
            
            if (!contents) break;
            
            allFiles = allFiles.concat(contents);
            
            isTruncated = bucketData.IsTruncated && bucketData.IsTruncated[0] === 'true';
            if (isTruncated) {
                nextMarker = contents[contents.length - 1].Key[0];
                console.log(`Fetched ${allFiles.length} items...`);
            }
        }
        
        console.log(`\nFound ${allFiles.length} total items. Starting downloads...\n`);
        const results = await processQueue(allFiles, baseUrl, outputDir);
        
        console.log('\n=== Summary ===');
        console.log(`Downloaded: ${results.downloaded}`);
        console.log(`Skipped (already exist): ${results.skipped}`);
        console.log(`Failed: ${results.failed}`);
        console.log('Task finished.');
        
    } catch (error) {
        console.error('Fatal:', error.message);
    }
}

downloadFiles();
