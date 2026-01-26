# KogStatic Bucket Downloader

A Node.js utility for enumerating and downloading publicly accessible assets from Kogama CDN buckets.  
The script supports both the production (`static`) and development (`dev`) environments and mirrors the remote directory structure locally while filtering by file type.

This tool is intended for inspection, archival, or research purposes where access is permitted.

> [!WARNING]
> As of 26th January 2026, the `dev.kogstatic` is no longer an accessible endpoint.
---

## Features

- Supports both **static** and **dev** CDN buckets
- Iterates through bucket listings using XML pagination (`IsTruncated` + `marker`)
- Downloads files as streams for stability and low memory usage
- Preserves original directory hierarchy
- Restores remote `LastModified` timestamps locally
- Filters downloads by allowed file extensions
- Sanitizes filenames to ensure filesystem compatibility
- Gracefully handles partial failures without aborting the entire run

---

## Supported Environments

The script prompts for the environment at runtime:

- `static` → `https://static.kogstatic.com/`
- `dev` → `https://dev.kogstatic.com/`

The **dev** environment is often more permissive and may expose:
- Unreleased assets
- Test builds
- Legacy or staging resources

Use it deliberately and responsibly.

---

## Allowed File Types

Only files with the following extensions are downloaded:

- Images: `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`
- Video: `.mp4`
- Audio: `.mp3`, `.wav`, `.ogg`

This can be adjusted by modifying the `ALLOWED_EXTENSIONS` set in the script.

---

## Requirements

- Node.js v18 or newer (for `stream/promises`)
- Internet access to the target CDN

### Dependencies

Installed automatically via npm:

- `axios`
- `xml2js`

---

## Installation

Clone the repository or copy the script, then install dependencies:

```bash
npm install axios xml2js
```

Then you can run with ``node main.js`` <3   

---

## How It Works

1. Fetches the bucket listing as XML
2. Parses the response using xml2js
3.  Iterates over each object (Contents)

Skips:

Directory markers   
Disallowed file extensions  
Creates matching local directories   
Streams each file to disk  

Applies original modification timestamps   
Continues until IsTruncated is false

Errors during individual downloads are logged but do not stop the process.
