#!/usr/bin/env python3
import requests
import gzip
import json
import re
from io import BytesIO

URL = "https://epgshare01.online/epgshare01/epg_ripper_ALL_SOURCES1.xml.gz"
OUTPUT = "epg.json.gz"

def stream_convert():
    print(f"Fetching {URL}...")
    response = requests.get(URL, stream=True)
    
    print("Decompressing and parsing...")
    
    # Decompress in chunks
    decompressor = gzip.GzipFile(fileobj=BytesIO(response.content))
    xml_content = decompressor.read().decode('utf-8')
    
    print("Extracting data...")
    
    # String pool for channel IDs
    string_pool = {}
    channels = []
    programmes = []
    
    # Extract channels with regex (streaming-style)
    channel_pattern = re.compile(r'<channel[^>]*id="([^"]+)"[^>]*>(.*?)</channel>', re.DOTALL)
    for match in channel_pattern.finditer(xml_content):
        channel_id = match.group(1)
        channel_xml = match.group(2)
        
        if channel_id not in string_pool:
            string_pool[channel_id] = len(string_pool)
        
        # Extract display name
        name_match = re.search(r'<display-name[^>]*>([^<]+)</display-name>', channel_xml)
        name = name_match.group(1) if name_match else channel_id
        
        # Extract icon
        icon_match = re.search(r'<icon[^>]*src="([^"]+)"', channel_xml)
        icon = icon_match.group(1) if icon_match else None
        
        channel_data = {"i": channel_id, "n": name}
        if icon:
            channel_data["ic"] = icon
        
        channels.append(channel_data)
    
    print(f"Found {len(channels)} channels")
    
    # Extract programmes with regex
    prog_pattern = re.compile(
        r'<programme[^>]*channel="([^"]+)"[^>]*start="([^"]+)"[^>]*stop="([^"]+)"[^>]*>(.*?)</programme>',
        re.DOTALL
    )
    
    count = 0
    for match in prog_pattern.finditer(xml_content):
        count += 1
        if count % 10000 == 0:
            print(f"  Processed {count} programmes...")
        
        channel = match.group(1)
        start = match.group(2)[:14]  # Strip timezone
        stop = match.group(3)[:14]
        content = match.group(4)
        
        # Get channel index from pool
        channel_idx = string_pool.get(channel, 0)
        
        # Extract title
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', content)
        title = title_match.group(1) if title_match else ""
        
        # Extract subtitle
        subtitle_match = re.search(r'<sub-title[^>]*>([^<]+)</sub-title>', content)
        subtitle = subtitle_match.group(1) if subtitle_match else None
        
        # Extract description
        desc_match = re.search(r'<desc[^>]*>([^<]+)</desc>', content)
        desc = desc_match.group(1) if desc_match else None
        
        # Build programme array
        prog = [channel_idx, start, stop, title]
        
        if subtitle:
            prog.append(subtitle)
        elif desc:
            prog.append(None)
            prog.append(desc)
        
        # Remove trailing Nones
        while prog and prog[-1] is None:
            prog.pop()
        
        programmes.append(prog)
    
    print(f"Found {len(programmes)} programmes")
    
    # Build string pool array
    string_pool_array = [''] * len(string_pool)
    for s, idx in string_pool.items():
        string_pool_array[idx] = s
    
    # Create JSON structure
    epg = {
        "v": 1,
        "sp": string_pool_array,
        "c": channels,
        "p": programmes
    }
    
    print(f"Writing to {OUTPUT}...")
    with gzip.open(OUTPUT, 'wt', encoding='utf-8') as f:
        json.dump(epg, f, separators=(',', ':'), ensure_ascii=False)
    
    print(f"Done! Output: {OUTPUT}")

if __name__ == "__main__":
    stream_convert()
