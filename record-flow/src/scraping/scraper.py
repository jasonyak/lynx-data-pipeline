
import asyncio
import hashlib
import json
import logging
import os
import time
from urllib.parse import urlparse, urljoin
import os
try:
    from PIL import Image
except ImportError:
    Image = None
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebsiteScraper:
    def __init__(self, output_base="data/scraped_content"):
        self.output_base = output_base
        self.max_depth = 3
        self.max_pages = 15 # Reduced from 50 to focus on high-quality pages
        self.timeout_ms = 30000  # 30 seconds
        
        # Targeted Heuristic Scraping
        self.priority_keywords = ["about", "program", "curriculum", "tuition", "contact", "gallery", "admission", "staff", "team", "philosophy", "schedule"]
        self.ignored_keywords = ["blog", "news", "event", "calendar", "login", "portal", "parent-portal", "career", "job", "policy", "terms", "privacy"]

    def _get_url_hash(self, url):
        return hashlib.md5(url.encode()).hexdigest()

    def _get_domain(self, url):
        netloc = urlparse(url).netloc
        if netloc.startswith("www."):
            return netloc[4:]
        return netloc

    def _is_valid_asset(self, url):
        """Basic check if url is likely a file/asset we want."""
        lower_url = url.lower()
        if lower_url.endswith(".pdf"):
            return "pdf"
        if any(lower_url.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            return "image"
        return None

    def _resize_image_if_needed(self, file_path):
        """Resizes image to max width 1000px if larger. Returns True if successful."""
        if not Image:
            return True # Skip if Pillow not installed (log warning elsewhere if needed)
            
        try:
            with Image.open(file_path) as img:
                width, height = img.size
                if width > 1000:
                    new_height = int(height * (1000 / width))
                    logger.debug(f"Resizing image {file_path} from {width}x{height} to 1000x{new_height}")
                    img = img.resize((1000, new_height), Image.Resampling.LANCZOS)
                    img.save(file_path)
            return True
        except Exception as e:
            logger.debug(f"Failed to resize image {file_path}: {e}")
            return False

    def _verify_asset(self, file_path, asset_type):
        """
        Stage 1 Verification (Heuristics).
        Returns True if passed, False otherwise.
        """
        try:
            size_bytes = os.path.getsize(file_path)
            
            if asset_type == "pdf":
                if size_bytes < 5 * 1024: # 5KB
                    logger.debug(f"Rejected PDF (too small: {size_bytes}b): {file_path}")
                    return False
                return True
            
            elif asset_type == "image":
                if size_bytes < 10 * 1024: # 10KB
                    logger.debug(f"Rejected Image (too small: {size_bytes}b): {file_path}")
                    return False
                
                # Verify dimensions (needs pillow, optional but requested "dimensions check")
                # Since we didn't add Pillow to dependencies explicitly in validaiton plan, 
                # we will rely on size for now or try to use standard lib ? 
                # Actually, standard lib doesn't do image dims easily.
                # Assuming user has basic env, but for safety in this strict environment,
                # let's stick to file size for now unless we add Pillow to requirements.
                # The user "stage 1" request specifically mentioned dimensions though.
                # Let's try to import PIL, if fails, skip dimension check.
                try:
                    from PIL import Image
                    with Image.open(file_path) as img:
                        width, height = img.size
                        ratio = width / height if height else 0
                        
                        if width < 200 or height < 200:
                            logger.debug(f"Rejected Image (too small dims {width}x{height}): {file_path}")
                            return False
                        
                        if ratio > 3.0 or ratio < 0.33:
                             logger.debug(f"Rejected Image (extreme aspect ratio {ratio:.2f}): {file_path}")
                             return False
                             
                             return False
                             
                             
                except Exception as e:
                     logger.debug(f"Could not verify image dimensions: {e}")
                     return False

                return True
                
        except Exception as e:
            logger.error(f"Error checking asset {file_path}: {e}")
            return False
        return False

    async def _download_asset(self, page, url, save_path):
        try:
            # We can use page.request to fetch assets in context
            response = await page.request.get(url, timeout=10000)
            if response.status == 200:
                body = await response.body()
                with open(save_path, "wb") as f:
                    f.write(body)
                return True
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
        return False

    async def scrape_async(self, start_url):
        start_time = time.time()
        domain = self._get_domain(start_url)
        # url_hash removed per user request to flatten structure
        
        # Setup output directory
        base_dir = os.path.join(self.output_base, domain)
        assets_dir = os.path.join(base_dir, "assets")
        os.makedirs(assets_dir, exist_ok=True)
        
        # If metadata exists, maybe skip? (For now, we overwrite as per plan implied "deduplication" meant "check if done")
        # But Plan said: "Checks if a hash of the URL already exists...". 
        # Implementing basic skip if metadata exists to be efficient.
        manifest_path = os.path.join(base_dir, "metadata.json")
        if os.path.exists(manifest_path):
             logger.info(f"Scraping already done for {start_url}. Skipping.")
             with open(manifest_path, 'r') as f:
                 return json.load(f)

        visited_urls = set()
        queue = [(start_url, 0)] # (url, depth)
        collected_assets = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Set User Agent (Newer Chrome)
            await page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            })

            while queue and len(visited_urls) < self.max_pages:
                current_url, depth = queue.pop(0)
                
                if current_url in visited_urls:
                    continue
                visited_urls.add(current_url)
                
                logger.debug(f"Crawling {current_url} (Depth {depth})")
                try:
                    # Relaxed wait condition to prevent timeouts on continuous network activity
                    await page.goto(current_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    
                    # Scroll to bottom to encourage lazy loading
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000) # Wait for initial lazy loads
                    
                    # Slow scroll back up just in case elements need to be in viewport
                    # (Simplified to just waiting a bit for now to avoid complexity/time)
                    
                    # 1. Extract Text Content (for LLM)
                    # Get readable text (simple approach for now: innerText of body)
                    # 1. Extract Text Content (for LLM)
                    # Get readable text - prioritize semantic tags to reduce noise
                    page_text = await page.evaluate('''() => {
                        const tags = ['main', 'article', '#content', '.content', '#main', '.main'];
                        for (const selector of tags) {
                            const el = document.querySelector(selector);
                            if (el && el.innerText.length > 200) return el.innerText;
                        }
                        return document.body.innerText;
                    }''')
                    
                    # Save text content
                    text_filename = f"content_{hashlib.md5(current_url.encode()).hexdigest()}.txt"
                    text_path = os.path.join(base_dir, text_filename)
                    
                    if page_text and len(page_text) > 100:
                         with open(text_path, "w") as f:
                             f.write(f"URL: {current_url}\n\n{page_text}")
                         collected_assets.append({"type": "text", "original_url": current_url, "local_path": text_path})
                    else:
                         logger.debug(f"Text content too short ({len(page_text) if page_text else 0} chars). Preview: {page_text[:50] if page_text else 'None'}")

                    # 2. Extract Assets from current page
                    # Images
                    img_elements = await page.evaluate('''() => {
                        return Array.from(document.querySelectorAll('img')).map(img => {
                            // Try multiple sources for the image url
                            let src = img.src || img.getAttribute('data-src') || img.getAttribute('data-original') || img.currentSrc;
                            
                            // Handle srcset: pick the largest if src is empty or irrelevant? 
                            // Usually browser populates 'currentSrc' which is best.
                            if (!src && img.srcset) {
                                // naive pick first
                                src = img.srcset.split(',')[0].trim().split(' ')[0];
                            }
                            
                            return {
                                src: src,
                                width: img.naturalWidth,
                                height: img.naturalHeight
                            };
                        });
                    }''')
                    
                    # PDFs
                    pdf_links = await page.evaluate('''() => {
                        return Array.from(document.querySelectorAll('a[href$=".pdf"]')).map(a => a.href);
                    }''')
                    
                    # Process Images
                    logger.debug(f"Found {len(img_elements)} potential images on {current_url}")
                    for img in img_elements:
                        src = img.get('src')
                        if not src or not src.startswith('http'): continue
                        
                        asset_name = f"img_{hashlib.md5(src.encode()).hexdigest()}.jpg" # Normalize validation might be tricky with extensions, ensure uniqueness
                        if src.endswith('.png'): asset_name = asset_name.replace('.jpg', '.png')
                        elif src.endswith('.webp'): asset_name = asset_name.replace('.jpg', '.webp')
                        
                        save_path = os.path.join(assets_dir, asset_name)
                        
                        if not os.path.exists(save_path):
                             if await self._download_asset(page, src, save_path):
                                 # Verify Stage 1
                                 if self._verify_asset(save_path, "image"):
                                     self._resize_image_if_needed(save_path)
                                     collected_assets.append({"type": "image", "original_url": src, "local_path": save_path})
                                 else:
                                     os.remove(save_path) # Delete rejected

                    # Process PDFs
                    for pdf_url in pdf_links:
                        if not pdf_url.startswith('http'): continue
                        asset_name = f"doc_{hashlib.md5(pdf_url.encode()).hexdigest()}.pdf"
                        save_path = os.path.join(assets_dir, asset_name)
                        
                        if not os.path.exists(save_path):
                            if await self._download_asset(page, pdf_url, save_path):
                                # Verify Stage 1
                                if self._verify_asset(save_path, "pdf"):
                                    collected_assets.append({"type": "pdf", "original_url": pdf_url, "local_path": save_path})
                                else:
                                    os.remove(save_path)

                    if depth < self.max_depth:
                        links = await page.evaluate('''() => {
                            return Array.from(document.querySelectorAll('a[href]')).map(a => a.href);
                        }''')
                        
                        logger.debug(f"Found {len(links)} links on {current_url}")
                        
                        for link in links:
                            # Normalize
                            link = link.split('#')[0].rstrip('/')
                            if not link or not link.startswith('http'): continue
                            
                            # Domain check
                            if self._get_domain(link) != domain:
                                continue
                                
                            # Avoid already visited/queued
                            if link in visited_urls or link in [x[0] for x in queue]:
                                continue
                                
                            # --- HEURISTIC FILTERING ---
                            link_lower = link.lower()
                            
                            # 1. Strict Blocklist
                            if any(ignored in link_lower for ignored in self.ignored_keywords):
                                continue
                                
                            # 2. Priority Allowlist (Always accept priority pages)
                            is_priority = any(p in link_lower for p in self.priority_keywords)
                            
                            # 3. Acceptance Logic
                            # - Accept if Priority Keyword match
                            # - Accept if Depth is 0 (direct children of home likely important)
                            if is_priority or depth == 0:
                                queue.append((link, depth + 1))
                                    
                except Exception as e:
                    logger.warning(f"Failed to process {current_url}: {e}")
                    
            await browser.close()
            
        # Dedupe collected assets list by url
        seen_assets = set()
        unique_assets = []
        for asset in collected_assets:
            if asset['original_url'] not in seen_assets:
                seen_assets.add(asset['original_url'])
                unique_assets.append(asset)

        result = {
            "root_url": start_url,
            "timestamp": time.time(),
            "pages_crawled": len(visited_urls),
            "assets_found": len(unique_assets),
            "assets": unique_assets
        }
        
        # Save manifest
        with open(manifest_path, 'w') as f:
            json.dump(result, f, indent=2)
            
        logger.info(f"Scraping complete for {start_url}. Found {len(unique_assets)} assets.")
        return result

    def scrape(self, url):
        """Synchronous wrapper for async scrape"""
        return asyncio.run(self.scrape_async(url))

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python scraper.py <url>")
        sys.exit(1)
        
    url = sys.argv[1]
    
    # Ensure we can import from src
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))) # Add record-flow root
    from src.analysis.local_ai import LocalRefiner
    
    scraper = WebsiteScraper()
    print(f"Scraping {url}...")
    result = scraper.scrape(url)
    
    if result and result.get("assets_found", 0) > 0:
        print("Refining results with Local AI...")
        refiner = LocalRefiner()
        
        # Images
        all_imgs = [a['local_path'] for a in result['assets'] if a['type'] == 'image']
        top_imgs = refiner.rank_images(all_imgs, top_n=10)
        
        # PDFs
        all_pdfs = [a['local_path'] for a in result['assets'] if a['type'] == 'pdf']
        top_pdfs = refiner.filter_pdfs(all_pdfs, top_n=5)
        
        # Text
        all_txt = [a['local_path'] for a in result['assets'] if a['type'] == 'text']
        domain_dir = os.path.dirname(all_txt[0]) if all_txt else None
        clean_text_path = None
        if domain_dir:
             clean_text_path = os.path.join(domain_dir, "cleaned_content.txt")
             clean_text_path = refiner.refine_text(all_txt, clean_text_path)
             
        # Update result for display
        result['verified_images'] = top_imgs
        result['pdf_assets'] = top_pdfs
        result['derived_body_text_path'] = clean_text_path
        # Remove raw assets from display to avoid clutter
        del result['assets']
        
    print(json.dumps(result, indent=2))
