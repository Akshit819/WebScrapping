import io
import fitz  # PyMuPDF
import pandas as pd
import re
from collections import defaultdict
from google.cloud import vision
import os

# ==============================================================================
# PART 1: THE PDF ORCHESTRATOR (NEW LOGIC)
# ==============================================================================

def page_has_table_and_keyword(page, keyword):
    """
    Analyzes a PDF page to check for two conditions:
    1. Does it contain the specified keyword (case-insensitive)?
    2. Does it appear to have a table (heuristic: contains many numbers)?
    """
    text = page.get_text().lower()
    
    # Condition 1: Check for the keyword
    if keyword.lower() not in text:
        return False
        
    # Condition 2: Check for table-like features (e.g., lots of numbers)
    # This simple heuristic avoids running OCR on text-heavy pages.
    numbers_found = re.findall(r'[\d,.]+', text)
    if len(numbers_found) < 20: # You can adjust this threshold
        return False
        
    return True

def process_pdf_for_tables(pdf_path, output_excel_path, keyword="consolidated"):
    """
    Main function to orchestrate the entire PDF processing workflow.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at '{pdf_path}'")
        return

    print(f"Processing PDF: {pdf_path}")
    doc = fitz.open(pdf_path)
    
    # Use pandas ExcelWriter to save multiple sheets to one file
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        extracted_tables_count = 0
        
        # Iterate through each page of the PDF
        for page_num, page in enumerate(doc):
            print(f"\n--- Analyzing Page {page_num + 1}/{len(doc)} ---")
            
            # Check if the page is a candidate for table extraction
            if not page_has_table_and_keyword(page, keyword):
                print(f"Skipping Page {page_num + 1}: Does not contain '{keyword}' or lacks table features.")
                continue

            print(f"Page {page_num + 1} is a candidate. Preparing for extraction...")
            
            # Render the page to a high-resolution image in memory for OCR
            pix = page.get_pixmap(dpi=300)
            img_bytes = pix.tobytes("png")
            
            try:
                # Feed the image bytes to our perfected extraction engine
                df = extract_table_from_image_data(img_bytes)
                
                if not df.empty:
                    # Create a descriptive sheet name
                    sheet_name = f'Page_{page_num + 1}_Table'
                    df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
                    print(f"SUCCESS: Extracted table from Page {page_num + 1} and saved to sheet '{sheet_name}'.")
                    extracted_tables_count += 1
                else:
                    print(f"NOTE: Page {page_num + 1} was processed, but no table data was extracted.")

            except Exception as e:
                print(f"ERROR on Page {page_num + 1}: Could not extract table. Reason: {e}")

    if extracted_tables_count > 0:
        print(f"\nProcessing complete. Found and extracted {extracted_tables_count} table(s).")
        print(f"Output saved to: {output_excel_path}")
    else:
        print("\nProcessing complete. No tables matching the criteria were found or extracted.")


# ==============================================================================
# PART 2: THE IMAGE PROCESSING ENGINE (OUR PERFECTED CODE)
# ==============================================================================

def extract_table_from_image_data(image_bytes):
    """
    A wrapper function that takes image data and runs our full extraction pipeline.
    """
    # 2a. Get OCR data from the image bytes
    api_response = get_full_ocr_response(image_bytes)
    all_words = get_all_words(api_response)
    
    # 2b. Reconstruct the rows and columns
    lines = reconstruct_lines_intelligently(all_words)
    boundaries = detect_boundaries_by_gaps(lines)
    
    # 2c. Build the final table grid
    table_grid = build_table_simply(lines, boundaries)
    
    return pd.DataFrame(table_grid)

# --- All the helper functions from our previous script ---

def get_full_ocr_response(image_bytes):
    print("  Step 2.1: Calling Google Cloud Vision API...")
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=image_bytes)
    response = client.document_text_detection(image=image)
    if response.error.message:
        raise Exception(f'Google Vision API Error: {response.error.message}')
    return response

def get_all_words(response):
    all_words = []
    document = response.full_text_annotation
    for page in document.pages:
        for block in page.blocks:
            for paragraph in block.paragraphs:
                for word in paragraph.words:
                    word_text = ''.join([symbol.text for symbol in word.symbols])
                    vertices = word.bounding_box.vertices
                    all_words.append({'text': word_text, 'vertices': vertices})
    return all_words

def is_numeric_like(text):
    text = text.strip()
    return bool(re.match(r'^-?[\d,.]+$', text)) or text in ['(', ')']

def reconstruct_lines_intelligently(all_words):
    print("  Step 2.2: Intelligently reconstructing rows...")
    if not all_words: return []
    all_words.sort(key=lambda w: (w['vertices'][0].y, w['vertices'][0].x))
    lines = []
    current_line = [all_words[0]]
    for i in range(1, len(all_words)):
        prev_word = current_line[-1]
        current_word = all_words[i]
        vertical_gap = current_word['vertices'][0].y - prev_word['vertices'][0].y
        is_carriage_return = current_word['vertices'][0].x < prev_word['vertices'][0].x
        if vertical_gap > 5 or (vertical_gap > 1 and is_carriage_return):
            lines.append(current_line)
            current_line = [current_word]
        else:
            current_line.append(current_word)
    lines.append(current_line)
    return lines

def detect_boundaries_by_gaps(lines):
    print("  Step 2.3: Detecting column boundaries by analyzing gaps...")
    gaps = []
    for line in lines:
        numeric_words_on_line = [w for w in line if is_numeric_like(w['text'])]
        for i in range(len(numeric_words_on_line) - 1):
            prev_word = numeric_words_on_line[i]
            next_word = numeric_words_on_line[i+1]
            gap = next_word['vertices'][0].x - prev_word['vertices'][1].x
            if gap > 5:
                gaps.append(gap)
    if not gaps: raise ValueError("Could not determine column structure from gaps.")
    avg_gap = sum(gaps) / len(gaps)
    column_gap_threshold = avg_gap * 1.5 
    boundaries = [0]
    for line in lines:
        numeric_words_on_line = [w for w in line if is_numeric_like(w['text'])]
        for i in range(len(numeric_words_on_line) - 1):
            prev_word = numeric_words_on_line[i]
            next_word = numeric_words_on_line[i+1]
            gap = next_word['vertices'][0].x - prev_word['vertices'][1].x
            if gap > column_gap_threshold:
                boundary_x = prev_word['vertices'][1].x + (gap / 2)
                if not any(abs(boundary_x - b) < 20 for b in boundaries):
                    boundaries.append(boundary_x)
    boundaries.sort()
    return boundaries

def build_table_simply(lines, boundaries):
    print("  Step 2.4: Building final table with simple text joining...")
    table = []
    for line in lines:
        row = [''] * len(boundaries)
        for word in line:
            word_mid_x = (word['vertices'][0].x + word['vertices'][1].x) / 2
            col_index = 0
            for i in range(1, len(boundaries)):
                if word_mid_x > boundaries[i]:
                    col_index = i
            if not row[col_index]:
                row[col_index] = word['text']
            else:
                row[col_index] += ' ' + word['text']
        final_row = [cell.strip() for cell in row]
        if any(final_row):
            table.append(final_row)
    return table

# ==============================================================================
# PART 3: SCRIPT EXECUTION
# ==============================================================================

if __name__ == "__main__":
    # --- CONFIGURE YOUR FILES HERE ---
    pdf_to_process = "trial_2.pdf"
    output_excel_file = "extracted_financial_trial2.xlsx"
    
    # Run the main process
    process_pdf_for_tables(pdf_to_process, output_excel_file)