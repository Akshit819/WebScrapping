import io
from google.cloud import vision
import pandas as pd
import re
from collections import defaultdict

def get_full_ocr_response(image_path):
    """Uses Google Cloud Vision API to get a structured response with word coordinates."""
    print("Step 1: Calling Google Cloud Vision API...")
    client = vision.ImageAnnotatorClient()
    with io.open(image_path, 'rb') as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    response = client.document_text_detection(image=image)
    if response.error.message:
        raise Exception(f'{response.error.message}')
    print("API call successful.\n")
    return response

def get_all_words(response):
    """Extracts a simple list of all words with their text and coordinates."""
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
    """A helper function to identify strings that look like numbers or are part of them."""
    text = text.strip()
    # Identifies numbers, or standalone parentheses, which are often OCR'd as separate words.
    return bool(re.match(r'^-?[\d,.]+$', text)) or text in ['(', ')']

def reconstruct_lines_intelligently(all_words):
    """
    THE CORRECT ROW RECONSTRUCTION.
    Detects a new line only on a "carriage return" (down and left).
    """
    print("Step 2: Intelligently reconstructing rows based on reading flow...")
    if not all_words:
        return []
    
    # Sort words primarily by vertical position, then horizontal
    all_words.sort(key=lambda w: (w['vertices'][0].y, w['vertices'][0].x))
    
    lines = []
    current_line = [all_words[0]]
    for i in range(1, len(all_words)):
        prev_word = current_line[-1]
        current_word = all_words[i]
        
        vertical_gap = current_word['vertices'][0].y - prev_word['vertices'][0].y
        is_carriage_return = current_word['vertices'][0].x < prev_word['vertices'][0].x
        
        # A new line is a significant vertical jump OR a smaller jump that also moves left.
        if vertical_gap > 10 or (vertical_gap > 3 and is_carriage_return):
            lines.append(current_line)
            current_line = [current_word]
        else:
            current_line.append(current_word)
    lines.append(current_line) # Add the last line
    
    print(f"Reconstructed {len(lines)} distinct lines.\n")
    return lines

def detect_boundaries_by_gaps(lines):
    """
    THE CORRECT COLUMN DETECTION.
    Finds column boundaries by analyzing the empty space between numbers on each line.
    """
    print("Step 3: Detecting column boundaries by analyzing gaps...")
    gaps = []
    for line in lines:
        numeric_words_on_line = [w for w in line if is_numeric_like(w['text'])]
        for i in range(len(numeric_words_on_line) - 1):
            prev_word = numeric_words_on_line[i]
            next_word = numeric_words_on_line[i+1]
            # Calculate the empty space between two consecutive numeric words
            gap = next_word['vertices'][0].x - prev_word['vertices'][1].x
            if gap > 5: # Ignore tiny gaps
                gaps.append(gap)
    
    if not gaps:
        raise ValueError("Could not determine column structure from gaps.")

    # Find the average "large gap" size, which represents the space between columns
    avg_gap = sum(gaps) / len(gaps)
    column_gap_threshold = avg_gap * 1.5 # A column gap is significantly larger than an average gap

    boundaries = [0]
    for line in lines:
        numeric_words_on_line = [w for w in line if is_numeric_like(w['text'])]
        for i in range(len(numeric_words_on_line) - 1):
            prev_word = numeric_words_on_line[i]
            next_word = numeric_words_on_line[i+1]
            gap = next_word['vertices'][0].x - prev_word['vertices'][1].x
            
            # If we find a large gap, its midpoint is a potential column boundary
            if gap > column_gap_threshold:
                boundary_x = prev_word['vertices'][1].x + (gap / 2)
                # Add it if it's a new boundary
                if not any(abs(boundary_x - b) < 20 for b in boundaries):
                    boundaries.append(boundary_x)

    boundaries.sort()
    print(f"Detected {len(boundaries)} columns. Boundaries at X-coords: {[int(b) for b in boundaries]}\n")
    return boundaries

def build_table_simply(lines, boundaries):
    """Builds the final table using a simple join, with no data manipulation."""
    print("Step 4: Building final table with simple text joining...")
    table = []
    for line in lines:
        row = [''] * len(boundaries)
        for word in line:
            word_mid_x = (word['vertices'][0].x + word['vertices'][1].x) / 2
            
            # Find the correct column for this word
            col_index = 0
            for i in range(1, len(boundaries)):
                if word_mid_x > boundaries[i]:
                    col_index = i
            
            # Simple Join: Add a space if the cell is not empty.
            if not row[col_index]:
                row[col_index] = word['text']
            else:
                row[col_index] += ' ' + word['text']
        
        final_row = [cell.strip() for cell in row]
        if any(final_row):
            table.append(final_row)
            
    print("Table reconstruction complete.\n")
    return table

# --- Main execution block ---
if __name__ == "__main__":
    image_path = 'trial1.png'

    try:
        api_response = get_full_ocr_response(image_path)
        all_words = get_all_words(api_response)
        
        # Use the new, robust functions
        lines = reconstruct_lines_intelligently(all_words)
        boundaries = detect_boundaries_by_gaps(lines)
        table_grid = build_table_simply(lines, boundaries)

        print("Step 5: Finalizing DataFrame...")
        df = pd.DataFrame(table_grid)
        
        print("\n--- FINAL EXTRACTED TABLE ---")
        print(df.to_string())
        print("-----------------------------\n")
        
        try:
            output_csv_path = 'financial_results_last.csv'
            df.to_csv(output_csv_path, index=False, header=False)
            print(f"Table successfully saved to {output_csv_path}")
        except PermissionError:
            print(f"\n--- ERROR: PERMISSION DENIED ---")
            print(f"Could not save the file to '{output_csv_path}'.")
            print("Please CLOSE the CSV file in Excel/Notepad/etc. and run the script again.")
            print("-----------------------------------")

    except FileNotFoundError:
        print(f"Error: The file '{image_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")