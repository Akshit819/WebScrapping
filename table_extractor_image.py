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

def is_numeric(text):
    """Checks if a string is purely numeric, allowing for symbols used in finance."""
    text = text.strip()
    if not text: return False
    if text.startswith('(') and text.endswith(')'):
        text = text[1:-1]
    return bool(re.match(r'^-?[\d,]*\.?\d+$', text))

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

def detect_boundaries_with_projection(all_words, empty_column_threshold=10):
    """
    Finds column boundaries using a horizontal projection profile. This method is robust.
    """
    print("Step 2: Detecting column boundaries using Horizontal Projection Profile...")
    max_x = 0
    for word in all_words:
        for vertex in word['vertices']:
            if vertex.x > max_x:
                max_x = vertex.x
    
    histogram = [0] * (max_x + 1)
    numeric_words = [word for word in all_words if is_numeric(word['text'])]
    for word in numeric_words:
        start_x = word['vertices'][0].x
        end_x = word['vertices'][1].x
        for i in range(start_x, end_x):
            histogram[i] += 1

    boundaries = [0]
    in_valley = False
    for i, count in enumerate(histogram):
        if count <= 1 and not in_valley:
            in_valley = True
        elif count > 1 and in_valley:
            in_valley = False
            boundaries.append(i)

    final_boundaries = []
    if boundaries:
        final_boundaries.append(boundaries[0])
        for i in range(1, len(boundaries)):
            if boundaries[i] - final_boundaries[-1] > empty_column_threshold:
                final_boundaries.append(boundaries[i])

    print(f"Detected {len(final_boundaries)} columns. Boundaries at X-coords: {[int(b) for b in final_boundaries]}\n")
    return final_boundaries


def build_table_with_correct_rows(all_words, boundaries):
    """
    THE DEFINITIVE ROW AND TABLE BUILDER.
    Reconstructs lines based on reading order and vertical gaps, then builds the table.
    """
    print("Step 3: Reconstructing rows based on reading order and building table...")
    
    # Sort all words in reading order: top-to-bottom, then left-to-right
    all_words.sort(key=lambda w: (w['vertices'][0].y, w['vertices'][0].x))
    
    # Reconstruct lines based on vertical jumps
    lines = []
    if all_words:
        current_line = [all_words[0]]
        for i in range(1, len(all_words)):
            prev_word = current_line[-1]
            current_word = all_words[i]
            
            # A new line starts if there's a significant vertical jump
            vertical_gap = current_word['vertices'][0].y - prev_word['vertices'][0].y
            
            if vertical_gap > 8: # Use a small but firm threshold for a new line
                lines.append(current_line)
                current_line = [current_word]
            else:
                current_line.append(current_word)
        lines.append(current_line) # Add the last line

    # Build the final table grid using the correctly identified lines
    table = []
    for line in lines:
        row = [''] * len(boundaries)
        for word in line:
            word_text = word['text']
            word_mid_x = (word['vertices'][0].x + word['vertices'][1].x) / 2
            
            col_index = 0
            for i in range(1, len(boundaries)):
                if word_mid_x > boundaries[i]:
                    col_index = i
            
            # Smart join logic to handle parentheses
            if not row[col_index]:
                row[col_index] = word_text
            elif word_text == ')' or row[col_index].endswith('('):
                row[col_index] += word_text
            else:
                row[col_index] += ' ' + word_text
        
        if any(cell.strip() for cell in row):
            table.append(row)
            
    print("Table reconstruction complete.\n")
    return table

# --- Main execution block ---
if __name__ == "__main__":
    image_path = 'trial2.png' # Make sure this path is correct

    try:
        api_response = get_full_ocr_response(image_path)
        all_words = get_all_words(api_response)

        boundaries = detect_boundaries_with_projection(all_words)
        
        table_grid = build_table_with_correct_rows(all_words, boundaries)

        print("Step 4: Finalizing DataFrame...")
        df = pd.DataFrame(table_grid)
        
        print("\n--- FINAL EXTRACTED TABLE ---")
        print(df.to_string())
        print("-----------------------------\n")
        
        try:
            output_csv_path = 'financial_results_{image_path}.csv'
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