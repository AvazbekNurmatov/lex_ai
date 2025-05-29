import scrapy
import os
import json
import pandas as pd
from tqdm import tqdm
import logging
import re
from scrapy.crawler import CrawlerProcess


class LexNewSpider2(scrapy.Spider):
    name = "lex_new2"
    allowed_domains = ["lex.uz"]
    start_urls = [
        "https://lex.uz/uz/docs/-6445145",
        "https://lex.uz/uz/docs/-7484334",
        "https://lex.uz/uz/docs/-7484454",
        "https://lex.uz/uz/docs/-7485096",
        "https://lex.uz/uz/docs/-7484114",
        "https://lex.uz/uz/docs/-7488166",
        "https://lex.uz/uz/docs/-7487610",
        "https://lex.uz/uz/docs/-7488164",
        "https://lex.uz/uz/docs/-7486279",
        "https://lex.uz/uz/docs/-7486412",
        "https://lex.uz/uz/docs/-7485775",
        "https://lex.uz/uz/docs/-7486257",
        "https://lex.uz/uz/docs/-7487706",
        "https://lex.uz/uz/docs/-7488160",
        "https://lex.uz/uz/docs/-7491792",
        "https://lex.uz/uz/docs/-7491334",
        "https://lex.uz/uz/docs/-7495608",
        "https://lex.uz/uz/docs/-7484184"
    ]

    def __init__(self):
        # Initialize data storage
        self.scraped_data = []

    def count_words(self, text):
        """Count words in text, handling None and empty strings"""
        if not text or not text.strip():
            return 0
        return len(text.strip().split())

    def has_class(self, element, class_name):
        """Check if element has a specific class"""
        classes = element.css('::attr(class)').get()
        if classes:
            return class_name in classes.split()
        return False

    def parse(self, response):
        # Extract law act ID from URL
        law_act_id = re.search(r'-(\d+)$', response.url)
        if law_act_id:
            law_act_id = law_act_id.group(1)
        else:
            law_act_id = "unknown"

        print(f"\n=== Processing URL: {response.url} ===")
        print(f"Law Act ID: {law_act_id}")

        # Find all div elements with class "ACT_TEXT lx_elem" or "CLAUSE_DEFAULT lx_elem"
        act_text_divs = response.css('div.ACT_TEXT.lx_elem, div.CLAUSE_DEFAULT.lx_elem')
        print(f"Found {len(act_text_divs)} ACT_TEXT and CLAUSE_DEFAULT elements")

        # If no elements found, try alternative selectors
        if not act_text_divs:
            print("No ACT_TEXT or CLAUSE_DEFAULT elements found, trying alternative selectors...")
            # Try just .ACT_TEXT
            act_text_divs = response.css('div.ACT_TEXT')
            print(f"Found {len(act_text_divs)} elements with just ACT_TEXT class")

            # Try any div with onmousemove containing lx_mo
            if not act_text_divs:
                act_text_divs = response.css('div[onmousemove*="lx_mo"]')
                print(f"Found {len(act_text_divs)} elements with lx_mo in onmousemove")

        # Temporary list to hold entries before consolidation
        temp_entries = []

        for i, div in enumerate(act_text_divs):
            print(f"\n--- Processing div {i + 1} ---")

            # Check if this is a CLAUSE_DEFAULT element
            is_clause_default = self.has_class(div, 'CLAUSE_DEFAULT')
            print(f"Is CLAUSE_DEFAULT: {is_clause_default}")

            # Extract paragraph ID from onmousemove attribute
            onmousemove = div.css('::attr(onmousemove)').get()
            print(f"onmousemove attribute: {onmousemove}")

            paragraph_id = None
            if onmousemove:
                # Extract ID from onmousemove="lx_mo(event,-7490852)"
                id_match = re.search(r'lx_mo\(event,-(\d+)\)', onmousemove)
                if id_match:
                    paragraph_id = id_match.group(1)
                    print(f"Extracted paragraph ID: {paragraph_id}")

            # Extract text content from the <a> tag
            text_content = div.css('a::text').get()
            print(f"Text content: {text_content[:100] if text_content else 'None'}...")

            # If no text in <a> tag, try getting all text from div
            if not text_content:
                text_content = div.css('::text').getall()
                if text_content:
                    text_content = ' '.join([t.strip() for t in text_content if t.strip()])
                    print(f"Alternative text extraction: {text_content[:100] if text_content else 'None'}...")

            # Clean the text content
            if text_content:
                text_content = text_content.strip()

            # Add data even if some fields are missing for debugging
            if paragraph_id or text_content:
                data_entry = {
                    'law_act_id': law_act_id,
                    'paragraph_id': paragraph_id or 'no_id',
                    'text': text_content or 'no_text',
                    'is_clause_default': is_clause_default
                }
                temp_entries.append(data_entry)
                print(f"Added temporary entry: {data_entry}")

        # Now consolidate entries with short text (less than 30 words) and handle CLAUSE_DEFAULT elements
        print(f"\n=== Consolidating short texts (< 30 words) and handling CLAUSE_DEFAULT elements ===")
        consolidated_entries = []
        pending_clause_default = None  # Store CLAUSE_DEFAULT text to prepend to next entry

        for i, entry in enumerate(temp_entries):
            word_count = self.count_words(entry['text'])
            is_clause_default = entry.get('is_clause_default', False)
            print(f"Entry {i + 1}: {word_count} words, CLAUSE_DEFAULT: {is_clause_default} - '{entry['text'][:50]}...'")

            if is_clause_default:
                # Store CLAUSE_DEFAULT text to prepend to the next entry
                print(f"  -> Storing CLAUSE_DEFAULT text to add to next entry: '{entry['text']}'")
                pending_clause_default = entry['text']
                continue  # Skip adding this entry, it will be prepended to the next one

            # Prepare the text for this entry
            current_text = entry['text']

            # If we have pending CLAUSE_DEFAULT text, prepend it
            if pending_clause_default:
                current_text = pending_clause_default + ' ' + current_text
                print(f"  -> Prepending CLAUSE_DEFAULT text: '{pending_clause_default}' to current entry")
                print(f"  -> Combined text: '{current_text[:100]}...'")
                pending_clause_default = None  # Reset after using

            # Check if this entry should be merged with previous (only for short text, not CLAUSE_DEFAULT)
            should_merge_with_previous = word_count < 30

            # If should merge and there's a previous entry to merge with
            if should_merge_with_previous and consolidated_entries:
                print(f"  -> Merging with previous entry (< 30 words)")

                # Get the last consolidated entry
                last_entry = consolidated_entries[-1]

                # Combine texts with a space
                combined_text = last_entry['text'] + ' ' + current_text

                # Update the last entry with combined text
                # Keep the paragraph_id of the previous entry
                consolidated_entries[-1] = {
                    'law_act_id': entry['law_act_id'],
                    'paragraph_id': last_entry['paragraph_id'],  # Keep previous paragraph_id
                    'text': combined_text.strip()
                }

                print(f"  -> Combined text now has {self.count_words(combined_text)} words")
                print(f"  -> Kept paragraph_id: {last_entry['paragraph_id']}")

            else:
                # Add as new entry (either >= 30 words or first entry)
                # Remove the is_clause_default field from final entry
                final_entry = {
                    'law_act_id': entry['law_act_id'],
                    'paragraph_id': entry['paragraph_id'],
                    'text': current_text
                }
                consolidated_entries.append(final_entry)

                if word_count >= 30:
                    print(f"  -> Added as new entry (>= 30 words)")
                else:
                    print(f"  -> Added as new entry (first entry)")

        # Handle case where the last entry was a CLAUSE_DEFAULT (no next entry to attach to)
        if pending_clause_default:
            print(
                f"  -> Warning: Last entry was CLAUSE_DEFAULT with no next entry to attach to: '{pending_clause_default}'")
            # Add it as a separate entry
            final_entry = {
                'law_act_id': law_act_id,
                'paragraph_id': 'clause_default_orphan',
                'text': pending_clause_default
            }
            consolidated_entries.append(final_entry)
            print(f"  -> Added orphaned CLAUSE_DEFAULT as separate entry")

        # Add consolidated entries to main data storage
        self.scraped_data.extend(consolidated_entries)

        print(f"\nOriginal entries: {len(temp_entries)}")
        print(f"Consolidated entries: {len(consolidated_entries)}")
        print(f"Total entries so far: {len(self.scraped_data)}")

        # Log progress
        self.logger.info(
            f"Processed {response.url} - Found {len(act_text_divs)} ACT_TEXT/CLAUSE_DEFAULT elements, consolidated to {len(consolidated_entries)} entries")

    def closed(self, reason):
        """Called when the spider closes - save data to CSV"""
        print(f"\n=== Spider finished with reason: {reason} ===")
        print(f"Total scraped data entries: {len(self.scraped_data)}")

        if self.scraped_data:
            # Create DataFrame
            df = pd.DataFrame(self.scraped_data)

            # Save to existing saved.csv file (or create it if it doesn't exist)
            output_file = 'saved.csv'

            try:
                df.to_csv(output_file, index=False, encoding='utf-8')

                # Verify file was created
                if os.path.exists(output_file):
                    file_size = os.path.getsize(output_file)
                    current_dir = os.getcwd()
                    full_path = os.path.abspath(output_file)

                    print(f"✓ Successfully saved {len(self.scraped_data)} records to {output_file}")
                    print(f"✓ File location: {full_path}")
                    print(f"✓ Current directory: {current_dir}")
                    print(f"✓ File size: {file_size} bytes")

                    # Check if file existed before and show appropriate message
                    print(f"✓ Data saved to saved.csv (overwrote existing file if it existed)")

                    # Show first few rows as preview
                    print(f"\nFirst 3 rows preview:")
                    print(df.head(3).to_string())

                    # Show statistics about word counts
                    word_counts = [self.count_words(text) for text in df['text']]
                    print(f"\nText statistics:")
                    print(f"  Average words per entry: {sum(word_counts) / len(word_counts):.1f}")
                    print(f"  Minimum words: {min(word_counts)}")
                    print(f"  Maximum words: {max(word_counts)}")
                    print(f"  Entries with < 30 words: {sum(1 for wc in word_counts if wc < 30)}")

                else:
                    print(f"✗ File was not created at {output_file}")

            except Exception as e:
                print(f"✗ Error saving CSV: {e}")
                # Try saving as backup JSON in current directory
                try:
                    backup_file = 'lex_scraped_data_backup.json'
                    with open(backup_file, 'w', encoding='utf-8') as f:
                        json.dump(self.scraped_data, f, ensure_ascii=False, indent=2)
                    print(f"✓ Saved backup as JSON: {os.path.abspath(backup_file)}")
                except Exception as e2:
                    print(f"✗ Error saving backup: {e2}")

            self.logger.info(f"Scraped {len(self.scraped_data)} records and saved to current directory")
        else:
            print("✗ No data was scraped - check the HTML structure")
            self.logger.warning("No data was scraped")


def run_spider():
    """Function to run the spider"""
    # Print where the file will be saved
    print("=== File will be saved to current directory ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"File will be saved as: {os.path.join(os.getcwd(), 'saved.csv')}")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    # Configure Scrapy settings
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1,  # Be respectful to the server
        'RANDOMIZE_DOWNLOAD_DELAY': 0.5,
        'CONCURRENT_REQUESTS': 1,  # Process one at a time to be respectful
        'COOKIES_ENABLED': True,
        'LOG_LEVEL': 'INFO'
    })

    # Run the spider
    process.crawl(LexNewSpider2)
    process.start()


if __name__ == "__main__":
    run_spider()