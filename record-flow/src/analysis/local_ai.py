
import os
import torch
import logging
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
from collections import Counter
import re
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None


logger = logging.getLogger(__name__)

class LocalRefiner:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")
        self.clip_model_name = "openai/clip-vit-base-patch32"
        self._clip_model = None
        self._clip_processor = None
        logger.info(f"LocalRefiner initialized on device: {self.device}")

    @property
    def clip_model(self):
        if self._clip_model is None:
            logger.debug(f"Loading CLIP model {self.clip_model_name}...")
            self._clip_model = CLIPModel.from_pretrained(self.clip_model_name).to(self.device)
            self._clip_processor = CLIPProcessor.from_pretrained(self.clip_model_name)
        return self._clip_model

    @property
    def clip_processor(self):
        if self._clip_processor is None:
            self.clip_model # Triggers load
        return self._clip_processor

    def rank_images(self, image_paths, top_n=10):
        """
        Ranks images based on relevance to daycare prompts using CLIP.
        Returns top_n image paths.
        """
        if not image_paths:
            logger.debug("rank_images called with empty list")
            return []
            
        # Deduplicate paths (scraper might report same file multiple times if found on multiple pages)
        unique_paths = list(set(image_paths))
            
        # Filter out obvious junk first (very small files that might have slipped through)
        valid_paths = [p for p in unique_paths if os.path.exists(p) and os.path.getsize(p) > 5000]
        if not valid_paths:
            return []

        logger.debug(f"Ranking {len(valid_paths)} unique images (from {len(image_paths)} occurrences) with CLIP...")
        
        positive_prompts = [
            "daycare classroom with educational toys", "safe fenced outdoor playground", 
            "children napping on cots", "clean toddler bathroom with small sinks",
            "children eating healthy food at table", "children art work on walls",
            "secure entrance gate with keypad", "teacher reading to circle of kids",
            "bright montessori shelf with materials", "soft play area for infants",
            "happy children playing together", "daycare building exterior"
        ]
        negative_prompts = [
            "icon", "logo", "website banner", "text document", "flyer", "map", 
            "blurry image", "stock photo of business people", "closeup of food only",
            "empty parking lot", "abstract background pattern", "clipart vector graphic"
        ]
        
        text_inputs = positive_prompts + negative_prompts
        
        try:
            images = []
            loaded_paths = []
            for p in valid_paths:
                try:
                    images.append(Image.open(p).convert("RGB"))
                    loaded_paths.append(p)
                except Exception as e:
                    logger.debug(f"Failed to load image for scoring {p}: {e}")

            if not images:
                return []

            inputs = self.clip_processor(
                text=text_inputs, images=images, return_tensors="pt", padding=True
            ).to(self.device)

            with torch.no_grad():
                outputs = self.clip_model(**inputs)
                # logits_per_image: [num_images, num_text_prompts]
                logits_per_image = outputs.logits_per_image 
                probs = logits_per_image.softmax(dim=1)

            # Score = Sum(Positive PROBS) - Sum(Negative PROBS)
            # Positive indices: 0 to len(positive)-1
            # Negative indices: len(positive) to end
            pos_indices = list(range(len(positive_prompts)))
            neg_indices = list(range(len(positive_prompts), len(text_inputs)))

            scores = []
            for idx, p_path in enumerate(loaded_paths):
                pos_score = probs[idx][pos_indices].sum().item()
                neg_score = probs[idx][neg_indices].sum().item()
                final_score = pos_score - neg_score
                scores.append((p_path, final_score))
            
            # Sort by score descending
            scores.sort(key=lambda x: x[1], reverse=True)
            
            # Select top N
            top_images = [x[0] for x in scores[:top_n]]
            logger.debug(f"Top 3 Image Scores: {[f'{os.path.basename(x[0])}: {x[1]:.2f}' for x in scores[:3]]}")
            
            return top_images

        except Exception as e:
            logger.error(f"Error filtering images with CLIP: {e}", exc_info=True)
            # Fallback: just return the first N
            return valid_paths[:top_n]

    def filter_pdfs(self, pdf_assets, top_n=5):
        """
        Simple keyword-based scoring for PDFs.
        Expects a list of asset dicts: [{'local_path': ..., 'original_url': ...}, ...]
        Returns a list of local paths.
        """
        if not pdf_assets:
            return []

        priority_keywords = ["handbook", "policy", "parent", "tuition", "rates", "enroll", "schedule", "calendar"]
        low_priority = ["menu", "lunch", "flyer", "news", "update"]

        scored_pdfs = []
        # Support both list of strings (old behavior, just in case) and list of dicts (new behavior)
        for asset in pdf_assets:
            if isinstance(asset, str):
                path = asset
                check_str = os.path.basename(asset).lower()
            else:
                path = asset.get('local_path')
                check_str = asset.get('original_url', '').lower()
                if not check_str: # Fallback if original_url missing
                    check_str = os.path.basename(path).lower()
            
            score = 0
            for kw in priority_keywords:
                if kw in check_str:
                    score += 2
            for kw in low_priority:
                if kw in check_str:
                    score -= 1
            scored_pdfs.append((path, score))
        
        # Sort descending
        scored_pdfs.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in scored_pdfs[:top_n]]

    def refine_text(self, text_files, output_path, pdf_files=None):
        """
        Consolidates text from multiple files, removing repeating boilerplate lines.
        Saves result to output_path.
        Returns output_path if successful, None otherwise.
        """
        if not text_files:
            return None

        # 1. Read all files
        all_lines_map = {} # filename -> [lines]
        line_counts = Counter()
        
        for p in text_files:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    content = f.read()
                    # Skip the URL line if present (e.g. "URL: ...")
                    lines = content.split('\n')
                    cleaned_lines = [l.strip() for l in lines if l.strip() and not l.startswith("URL:")]
                    all_lines_map[p] = cleaned_lines
                    for l in cleaned_lines:
                        line_counts[l] += 1
            except Exception as e:
                logger.warning(f"Failed to read text file {p}: {e}")

        # 2. Identify boilerplate (lines appearing in > 50% of files)
        threshold = max(2, len(text_files) * 0.5)
        boilerplate_lines = {line for line, count in line_counts.items() if count > threshold}
        
        logger.debug(f"Identified {len(boilerplate_lines)} boilerplate lines (appearing in >{threshold} files).")

        # 3. Construct unique body with Aggressive Filtering
        
        # Heuristic Noise Filters
        noise_patterns = [
            r"copyright \d{4}", r"all rights reserved", r"privacy policy", r"terms of use", 
            r"cookie policy", r"subscribe to our newsletter", r"follow us on", 
            r"menu", r"navigation", r"skip to content", r"search this site",
            r"sign in", r"log in", r"cart \(\d+\)"
        ]
        noise_regex = re.compile("|".join(noise_patterns), re.IGNORECASE)

        final_text = []
        for p in text_files:
            file_lines = all_lines_map.get(p, [])
            unique_lines = [l for l in file_lines if l not in boilerplate_lines]
            
            # Apply aggressive line filtering
            filtered_lines = []
            for line in unique_lines:
                # 1. Skip if matches noise regex
                if noise_regex.search(line):
                    continue
                # 2. Skip very short lines (likely nav items), unless they look like headers (ends with colon or all caps brief)
                if len(line.split()) < 4: # Fewer than 4 words
                     # Check if it might be a header?
                     if not (line.endswith(':') or (line.isupper() and len(line) > 5)):
                         continue
                         
                filtered_lines.append(line)
                
            if filtered_lines:
                final_text.append(f"--- Source: {os.path.basename(p)} ---")
                # Limit per-file contribution to avoid one file dominating? (Optional, let's stick to global limit for now)
                final_text.extend(filtered_lines)
                final_text.append("\n")

        # Track website text stats (approximation)
        website_chars = sum(len(s) for s in final_text)

        # 4. Integrate PDF Text
        if pdf_files and PdfReader:
            for pdf_path in pdf_files:
                try:
                    reader = PdfReader(pdf_path)
                    pdf_text = []
                    for page in reader.pages:
                        extracted = page.extract_text()
                        if extracted:
                            pdf_text.append(extracted)
                    
                    if pdf_text:
                        final_text.append(f"\n--- Source: {os.path.basename(pdf_path)} (PDF) ---\n")
                        # Basic cleanup: compact multiple newlines
                        full_pdf_body = "\n".join(pdf_text)
                        full_pdf_body = re.sub(r'\n{3,}', '\n\n', full_pdf_body)
                        final_text.append(full_pdf_body)
                        final_text.append("\n")
                except Exception as e:
                    logger.warning(f"Failed to extract text from PDF {pdf_path}: {e}")
                    
        full_content = "\n".join(final_text)
        
        # Calculate stats
        total_raw_chars = len(full_content)
        pdf_chars = total_raw_chars - website_chars
        # Note: pdf_chars calculation is approximate because of the join("\n"), 
        # but close enough for logging purposes.
        
        # Enforce max length (100k chars) to prevent context overflow, but allow rich context
        if len(full_content) > 100000:
            full_content = full_content[:100000] + "\n...[Truncated]..."
            
        final_chars = len(full_content)
        pdf_count = len(pdf_files) if pdf_files else 0
        
        logger.info(f"Refined Content: Website (~{website_chars} chars) + {pdf_count} PDFs (~{pdf_chars} chars) -> Total {total_raw_chars} chars -> Sent to Gemini: {final_chars} chars")
        
        # Save
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(full_content)
            return output_path
        except Exception as e:
            logger.error(f"Failed to save cleaned text: {e}")
            return None
