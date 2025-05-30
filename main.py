import scrapy
import os
import json
import pandas as pd
import pickle
from tqdm import tqdm
import logging
import re
from scrapy.crawler import CrawlerProcess


class LexNewSpider2(scrapy.Spider):
    name = "lex_new2"
    allowed_domains = ["lex.uz"]

    def __init__(self):
        # Load IDs from CSV file
        self.load_ids_from_csv()
        # Initialize data storage - now a dictionary with law_act_id as key
        self.scraped_data = {}

    def load_ids_from_csv(self):
        """Load the first 100 IDs from numbers.csv"""
        print(f"Current working directory: {os.getcwd()}")
        print(f"Looking for CSV file: {os.path.abspath('numbers.csv')}")
        print(f"CSV file exists: {os.path.exists('numbers.csv')}")

        # List files in current directory for debugging
        files_in_dir = os.listdir('.')
        csv_files = [f for f in files_in_dir if f.endswith('.csv')]
        print(f"Files in current directory: {len(files_in_dir)} total")
        print(f"CSV files found: {csv_files}")

        try:
            # Read CSV file
            df = pd.read_csv('numbers.csv')

            print(f"Successfully loaded CSV file!")
            print(f"CSV shape: {df.shape}")
            print(f"CSV columns: {list(df.columns)}")
            print(f"First few rows:")
            print(df.head())

            # Try to extract numbers from the CSV
            # Handle different possible column names and structures
            ids_list = []

            if len(df.columns) == 1:
                # Single column CSV
                column_name = df.columns[0]
                print(f"Using single column: {column_name}")
                ids_list = df[column_name].tolist()
            elif 'id' in df.columns:
                print("Using 'id' column")
                ids_list = df['id'].tolist()
            elif 'ID' in df.columns:
                print("Using 'ID' column")
                ids_list = df['ID'].tolist()
            elif 'number' in df.columns:
                print("Using 'number' column")
                ids_list = df['number'].tolist()
            elif 'numbers' in df.columns:
                print("Using 'numbers' column")
                ids_list = df['numbers'].tolist()
            else:
                # Use first column as default
                column_name = df.columns[0]
                print(f"Using first column as default: {column_name}")
                ids_list = df[column_name].tolist()

            print(f"Extracted {len(ids_list)} IDs from CSV")
            if ids_list:
                print(f"First 5 IDs: {ids_list[:5]}")
                print(f"Sample ID type: {type(ids_list[0])}")

            # Take first 100 IDs
            first_100_ids = ids_list[:100]

            # Convert to strings and clean if necessary
            cleaned_ids = []
            for id_val in first_100_ids:
                if pd.isna(id_val):  # Skip NaN values
                    continue
                elif isinstance(id_val, (int, float)):
                    cleaned_ids.append(str(int(id_val)))
                elif isinstance(id_val, str):
                    # Remove any non-numeric characters
                    clean_id = ''.join(filter(str.isdigit, id_val))
                    if clean_id:
                        cleaned_ids.append(clean_id)
                else:
                    print(f"Skipping invalid ID: {id_val} (type: {type(id_val)})")

            print(f"Cleaned {len(cleaned_ids)} valid IDs")
            if cleaned_ids:
                print(f"First 5 cleaned IDs: {cleaned_ids[:5]}")
                print(f"Last 5 cleaned IDs: {cleaned_ids[-5:]}")

            # Create start_urls
            self.start_urls = [f"https://lex.uz/uz/docs/-{id_num}" for id_num in cleaned_ids]

            print(f"Created {len(self.start_urls)} URLs")
            print(f"First 3 URLs: {self.start_urls[:3]}")
            print(f"Last 3 URLs: {self.start_urls[-3:]}")

            if not self.start_urls:
                raise ValueError("No valid IDs found in CSV file")

        except FileNotFoundError:
            print("ERROR: numbers.csv file not found in current directory!")
            print("Available CSV files:", [f for f in os.listdir('.') if f.endswith('.csv')])
            print("Falling back to original 3 URLs...")
            self.start_urls = [
                "https://lex.uz/uz/docs/-6445145",
                "https://lex.uz/uz/docs/-7484334",
                "https://lex.uz/uz/docs/-7484454"
            ]
        except Exception as e:
            print(f"ERROR loading CSV file: {e}")
            print(f"Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            print("Falling back to original 3 URLs...")
            self.start_urls = [
                "https://lex.uz/uz/docs/-6445145",
                "https://lex.uz/uz/docs/-7484334",
                "https://lex.uz/uz/docs/-7484454"
            ]

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

        # Initialize storage for this specific law act
        self.scraped_data[law_act_id] = []

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

        # Store consolidated entries for this law act
        self.scraped_data[law_act_id] = consolidated_entries

        print(f"\nOriginal entries: {len(temp_entries)}")
        print(f"Consolidated entries: {len(consolidated_entries)}")

        # Save individual CSV file for this law act immediately
        self.save_individual_csv(law_act_id, consolidated_entries)

        # Log progress
        self.logger.info(
            f"Processed {response.url} - Found {len(act_text_divs)} ACT_TEXT/CLAUSE_DEFAULT elements, consolidated to {len(consolidated_entries)} entries")

    def save_individual_csv(self, law_act_id, data_entries):
        """Save data for individual law act to separate CSV file"""
        if not data_entries:
            print(f"No data to save for law act {law_act_id}")
            return

        try:
            # Create DataFrame
            df = pd.DataFrame(data_entries)

            # Create filename
            output_file = f'{law_act_id}.csv'

            # Save to CSV
            df.to_csv(output_file, index=False, encoding='utf-8')

            # Verify file was created
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file)
                print(f"✓ Successfully saved {len(data_entries)} records to {output_file}")
                print(f"✓ File size: {file_size} bytes")

                # Show statistics about word counts
                word_counts = [self.count_words(text) for text in df['text']]
                print(f"✓ Text statistics for {law_act_id}:")
                print(f"    Average words per entry: {sum(word_counts) / len(word_counts):.1f}")
                print(f"    Minimum words: {min(word_counts)}")
                print(f"    Maximum words: {max(word_counts)}")
                print(f"    Entries with < 30 words: {sum(1 for wc in word_counts if wc < 30)}")
            else:
                print(f"✗ File was not created: {output_file}")

        except Exception as e:
            print(f"✗ Error saving CSV for {law_act_id}: {e}")
            # Try saving as backup JSON
            try:
                backup_file = f'{law_act_id}_backup.json'
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(data_entries, f, ensure_ascii=False, indent=2)
                print(f"✓ Saved backup as JSON: {backup_file}")
            except Exception as e2:
                print(f"✗ Error saving backup for {law_act_id}: {e2}")

    def closed(self, reason):
        """Called when the spider closes"""
        print(f"\n=== Spider finished with reason: {reason} ===")

        total_entries = sum(len(entries) for entries in self.scraped_data.values())
        total_files = len(self.scraped_data)

        print(f"Total law acts processed: {total_files}")
        print(f"Total entries across all files: {total_entries}")

        # List all created files
        created_files = []
        for law_act_id in self.scraped_data.keys():
            csv_file = f'{law_act_id}.csv'
            if os.path.exists(csv_file):
                created_files.append(csv_file)

        print(f"\nCreated {len(created_files)} CSV files:")
        for file in sorted(created_files):
            file_size = os.path.getsize(file)
            print(f"  {file} ({file_size} bytes)")

        if not created_files:
            print("✗ No CSV files were created - check the HTML structure")
            self.logger.warning("No CSV files were created")
        else:
            print(f"✓ All files saved to current directory: {os.getcwd()}")
            self.logger.info(f"Created {len(created_files)} CSV files with {total_entries} total records")


def run_spider():
    """Function to run the spider"""
    # Print where files will be saved
    print("=== Files will be saved to current directory ===")
    print(f"Current working directory: {os.getcwd()}")
    print("Input file: numbers.csv")
    print("Output files: [law_act_id].csv (e.g., 6445145.csv)")

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